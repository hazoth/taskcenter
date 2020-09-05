import aioredis
from pydantic import BaseModel, BaseSettings, Json
from enum import IntEnum
from typing import Optional, List, Tuple
import weakref
import asyncio
import logging


class QueueStat(BaseModel):
    length: int
    oldest_cursor: Optional[str]
    newest_cursor: Optional[str]


class TaskConfig(BaseModel):
    # retry: Optional[int]
    cache_result_time: Optional[int] = 60
    forward_result_to: Optional[str] = None


class TaskModel(BaseModel):
    id: str
    info: Json
    content: bytes
    config: Json[TaskConfig]
    cursor: str
    task_done: Optional[bool] = None
    result_info: Optional[Json] = None
    result_content: Optional[bytes] = None


class PeekResult(BaseModel):
    id: str
    info: bytes
    cursor: str


class AcquireResult(BaseModel):
    id: str
    info: bytes
    content: bytes


class TaskResult(BaseModel):
    info: bytes
    content: bytes


class TaskDoneEvent:
    event: asyncio.Event
    result: Optional[TaskResult]

    def __init__(self):
        self.event = asyncio.Event()

    def set(self, result):
        self.result = result
        self.event.set()

    def is_set(self):
        return self.event.is_set()

    async def wait(self) -> Optional[TaskResult]:
        await self.event.wait()
        return self.result


class TaskManager:
    def __init__(
        self,
        redis_client: aioredis.Redis,
        redis_prefix: str,
    ):
        self._redis = redis_client
        self._redis_prefix = redis_prefix
        self._task_done_events = weakref.WeakValueDictionary()
        self._running_tasks = [
            # before python 3.7
            # asyncio.ensure_future(self.subscribe_reply())
            # after python 3.7
            asyncio.create_task(self.subscribe_reply())
        ]

    def name_queue(self, queue: str):
        return f'{self._redis_prefix}:queue:{queue}'.encode()

    def name_stream(self, queue: str):
        return f'{self._redis_prefix}:stream:{queue}'.encode()

    def name_task(self, queue: str, id: str):
        return f'{self._redis_prefix}:task:{queue}:{id}'.encode()

    def name_reply_channel(self):
        return f'{self._redis_prefix}:reply'.encode()

    async def subscribe_reply(self):
        redis_client = self._redis
        channels = await redis_client.subscribe(self.name_reply_channel())
        ch:aioredis.Channel = channels[0]
        while (await ch.wait_message()):
            try:
                key = await ch.get()
                event = self.get_task_done_event(key)
                if event is None:
                    continue
                if not event.is_set():
                    result = await redis_client.hmget(
                        key,
                        'id',
                        'task_done',
                        'result_info',
                        'result_content',
                    )
                    if result[1]:
                        event.set(TaskResult(
                            info=result[2],
                            content=result[3],
                        ))
                    else:
                        event.set(None)
                del event
            except Exception as e:
                logging.exception(e)
        return

    async def unsubscribe_reply(self):
        return await self._redis.unsubscribe(self.name_reply_channel())

    async def close(self):
        await self.unsubscribe_reply()
        await asyncio.sleep(0.1)
        self._redis.close()
        await self._redis.wait_closed()

    def get_task_done_event(
        self,
        key: bytes,
        auto_create: bool = False,
    ) -> TaskDoneEvent:
        event = self._task_done_events.get(key)
        if event is None and auto_create:
            event = TaskDoneEvent()
            self._task_done_events[key] = event
        return event

    async def call_task(
        self,
        queue: str,
        id: str,
        info: bytes,
        content: bytes,
        config: TaskConfig,
        timeout: int,
    ) -> Optional[TaskResult]:
        key = self.name_task(queue, id)
        event = self.get_task_done_event(key, auto_create=True)
        if event.is_set():
            return event.result
        result = await self._redis.hmget(
            key,
            'id',
            'task_done',
            'result_info',
            'result_content',
        )
        if result[1]:
            result = TaskResult(
                info=result[2],
                content=result[3],
            )
        elif timeout > 0:
            if not result[0]:
                await self.put_task(
                    queue=queue,
                    id=id,
                    info=info,
                    content=content,
                    config=config,
                )
            result = await asyncio.wait_for(event.wait(), timeout)
        else:
            result = None
        return result

    async def put_task(
        self,
        queue: str,
        id: str,
        info: bytes,
        content: bytes,
        config: TaskConfig,
    ) -> bool:
        script = '''
        if (redis.call('exists', KEYS[1]) == 1) then
            return ''
        else
            local sid = redis.call(
                'xadd',
                KEYS[2],
                '*',
                'id', ARGV[1],
                'info', ARGV[2]
            )
            redis.call(
                'hmset', KEYS[1],
                'id', ARGV[1],
                'info', ARGV[2],
                'content', ARGV[3],
                'config', ARGV[4],
                'cursor', sid
            )
            return sid
        end
        '''
        sid = await self._redis.eval(
            script,
            keys=[
                self.name_task(queue, id),
                self.name_stream(queue),
            ],
            args=[
                id,
                info,
                content,
                config.json(),
            ],
        )
        return bool(sid)

    async def peek_tasks(
        self,
        queue: str,
        cursor: Optional[str],
        size: int,
        timeout: int,
    ) -> List[PeekResult]:
        if not cursor:
            cursor = '0'
        if timeout > 0:
            timeout *= 1000
        else:
            timeout = None
        stream = self.name_stream(queue)
        result = await self._redis.xread(
            streams=[stream],
            timeout=timeout,
            count=size,
            latest_ids=[cursor],
        )
        result = [
            PeekResult(
                id=k[b'id'],
                info=k[b'info'],
                cursor=j,
            ) for i, j, k in result
        ]
        return result

    async def acquire_tasks(
        self,
        queue: str,
        ids: List[str],
        promise_reply_time: int,
    ) -> List[AcquireResult]:
        script = '''
            local sid = redis.call('hget', KEYS[1], 'cursor')
            if (not sid) then
                return nil
            end
            local xdel_succ = redis.call('xdel', KEYS[2], sid)
            if (xdel_succ ~= 1) then
                return nil
            end
            local data = redis.call(
                'hmget',
                KEYS[1],
                'id',
                'info',
                'content'
            )
            if (tonumber(ARGV[1]) > 0) then
                redis.call('expire', KEYS[1], ARGV[1])
            else
                redis.call('del', KEYS[1])
            end
            return data
        '''
        result = []
        for id in ids:
            i = await self._redis.eval(
                script,
                keys=[self.name_task(queue, id), self.name_stream(queue)],
                args=[promise_reply_time],
            )
            if i and i[0]:
                result.append(AcquireResult(
                    id=i[0],
                    info=i[1],
                    content=i[2],
                ))
        return result

    async def reply_task(
        self,
        queue: str,
        id: str,
        info: bytes,
        content: bytes,
    ) -> bool:
        key = self.name_task(queue, id)
        # answer someone's calling
        event = self.get_task_done_event(key)
        if event is not None:
            event.set(TaskResult(
                info=info,
                content=content,
            ))
        # set result
        await self._redis.watch(key)
        try:
            config, sid = await self._redis.hmget(
                key,
                'config',
                'cursor',
            )
            if not config:
                return False
            config = TaskConfig.parse_raw(config)
            p = self._redis.multi_exec()
            if sid:
                r = p.xdel(self.name_stream(queue), sid)
            if config.cache_result_time:
                p.hmset(
                    key,
                    'task_done', 1,
                    'result_info', info,
                    'result_content', content,
                )
                p.expire(key, config.cache_result_time)
            else:
                p.delete(key)
            p.publish(self.name_reply_channel(), key)
            r = await p.execute()
        finally:
            r = await self._redis.unwatch()
        if config.forward_result_to:
            await self.put_task(
                queue=config.forward_result_to,
                id=id,
                info=info,
                content=content,
                config=TaskConfig(),
            )
        return True

    async def remove_task(
        self,
        queue: str,
        id: str,
    ) -> bool:
        script = '''
        local sid = redis.call('hget', KEYS[1], 'cursor')
        if (sid) then
            redis.call('xdel', KEYS[2], sid)
        end
        return redis.call('del', KEYS[1])
        '''
        result = await self._redis.eval(
            script,
            keys=[
                self.name_task(queue, id),
                self.name_stream(queue),
            ],
        )
        return bool(result)

    async def clear_queue(
        self,
        queue: str,
        iter_num: int,
        step_size: int,
    ) -> bool:
        cursor = 0
        for _ in range(iter_num):
            cursor, keys = await self._redis.scan(
                cursor=cursor,
                match=self.name_task(queue, '*'),
                count=step_size,
            )
            if keys:
                await self._redis.delete(*keys)
            if int(cursor) == 0:
                await self._redis.delete(self.name_stream(queue))
                return True
        return False

    async def list_queue(
        self,
        cursor: Optional[str],
        size: int,
    ) -> Tuple[List[str], str]:
        if not cursor:
            cursor = '0'
        cursor, keys = await self._redis.scan(
            cursor=cursor or 0,
            match=self.name_stream('*'),
            count=size,
        )
        cursor = None if int(cursor) == 0 else str(cursor)
        plen = len(self.name_stream(''))
        keys = [i.decode()[plen:] for i in keys]
        return keys, cursor

    async def stat_queue(
        self,
        queue: str,
    ) -> Optional[QueueStat]:
        try:
            result = await self._redis.xinfo_stream(
                self.name_stream(queue),
            )
        except aioredis.errors.ReplyError as e:
            return None
        return QueueStat(
            length=result[b'length'],
            oldest_cursor=(result[b'first-entry'] or (None,))[0],
            newest_cursor=(result[b'last-entry'] or (None,))[0],
        )


class Settings(BaseSettings):
    redis_url: str = 'redis://127.0.0.1'
    redis_prefix: str = 'tc'


class Static:
    task_manager = None


async def get_task_manager() -> TaskManager:
    if Static.task_manager is None:
        settings = Settings()
        redis_client = await aioredis.create_redis_pool(
            settings.redis_url,
        )
        Static.task_manager = TaskManager(
            redis_client,
            settings.redis_prefix,
        )
    return Static.task_manager
