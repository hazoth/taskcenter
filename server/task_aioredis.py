import aioredis
import asyncio


class RedisWrapper:
    def __init__(self, client: aioredis.Redis):
        self._c = client

    async def eval(self, script, keys=[], args=[]):
        return await self._c.eval(script, keys=keys, args=args)

    async def hmget(self, *args):
        return await self._c.hmget(*args)

    async def delete(self, *args):
        return await self._c.delete(*args)

    async def scan(self, cursor, match, count):
        return await self._c.scan(
            cursor=cursor,
            match=match,
            count=count,
        )

    async def xread(self, streams, timeout, count, latest_ids):
        return await self._c.xread(
            streams=streams,
            timeout=timeout,
            count=count,
            latest_ids=latest_ids,
        )

    async def xinfo_stream(self, stream):
        try:
            result = await self._c.xinfo_stream(stream)
        except aioredis.errors.ReplyError as e:
            return None
        return {
            b'length': result[b'length'],
            b'first-entry': result[b'first-entry'],
            b'last-entry': result[b'last-entry'],
        }

    async def close(self):
        self._c.close()
        await self._c.wait_closed()


async def create_client(host, port, db=0):
    result = await aioredis.create_redis_pool(
        f'redis://{host}:{port}/{db}'
    )
    return RedisWrapper(result)


class PubSubWrapper:
    def __init__(
        self,
        client: aioredis.Redis,
        channel: aioredis.Channel,
        channel_name: str,
    ):
        self._c = client
        self._ch = channel
        self.channel_name = channel_name

    async def get_message(self):
        result = await self._ch.wait_message()
        if not result:
            return None
        return await self._ch.get()

    async def close(self):
        await self._c.unsubscribe(self.channel_name)
        await asyncio.sleep(0.1)
        self._c.close()
        await self._c.wait_closed()


async def create_pubsub(client: RedisWrapper, channel_name: str):
    client = client._c
    channels = await client.subscribe(channel_name)
    return PubSubWrapper(client, channels[0], channel_name)
