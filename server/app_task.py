from fastapi import APIRouter, Depends, Body
from pydantic import BaseModel
import json
import base64
from enum import Enum
from typing import Optional, List, Any
import asyncio
from .task import QueueStat, TaskConfig
from .task import TaskManager, get_task_manager


router = APIRouter()


class ErrorCode(Enum):
    NO_ERROR = '0000'
    TIMEOUT = '0001'


class BaseResponse(BaseModel):
    err_code: ErrorCode = ErrorCode.NO_ERROR


# @router.post(
#     '/create_queue',
# )
# async def api_create_queue(
#     queue: str,
#     config: dict,
# ):
#     pass


# @router.post(
#     '/destroy_queue'
# )
# async def api_destroy_queue(
#     queue: str,
# ):
#     pass


class PClearQueue(BaseModel):
    queue: str
    iter_num: int = 1000
    step_size: int = 100


class RClearQueue(BaseResponse):
    data: bool


@router.post(
    '/clear_queue',
    response_model=RClearQueue,
)
async def api_clear_queue(
    param: PClearQueue,
    tm: TaskManager = Depends(get_task_manager),
):
    result = await tm.clear_queue(
        queue=param.queue,
        iter_num=param.iter_num,
        step_size=param.step_size,
    )
    return RClearQueue(data=result)


class PListQueue(BaseModel):
    cursor: Optional[str] = None
    size: int


class RListQueue(BaseResponse):
    data: List[str]
    cursor: Optional[str]


@router.post(
    '/list_queue',
    response_model=RListQueue,
)
async def api_list_queue(
    param: PListQueue,
    tm: TaskManager = Depends(get_task_manager),
):
    keys, cursor = await tm.list_queue(
        param.cursor,
        param.size,
    )
    return RListQueue(
        data=keys,
        cursor=cursor,
    )


class RStatQueue(BaseResponse):
    data: Optional[QueueStat]


@router.post(
    '/stat_queue',
    response_model=RStatQueue,
)
async def api_stat_queue(
    queue: str = Body(..., embed=True),
    tm: TaskManager = Depends(get_task_manager),
):
    result = await tm.stat_queue(queue)
    return RStatQueue(data=result)


# @router.post(
#     '/get'
# )
# async def api_get(
#     queue: str,
#     id: str,
#     fields: List[str],
# ):
#     return {
#         'id',
#         'status', # created/running/done
#         'config',
#         'info',
#         'content',
#         'result',
#     }


class PCallTask(BaseModel):
    queue: str
    id: str
    info: Any = None
    content: str = '' # base64
    config: Optional[TaskConfig] = None
    timeout: int


class RCallTaskData(BaseModel):
    id: str
    result_info: Any
    result_content: str # base64


class RCallTask(BaseResponse):
    data: Optional[RCallTaskData] = None


@router.post(
    '/call',
    response_model=RCallTask,
    # response_model_exclude_none=True,
)
async def api_call(
    param: PCallTask,
    tm: TaskManager = Depends(get_task_manager),
):
    try:
        result = await tm.call_task(
            queue=param.queue,
            id=param.id,
            info=json.dumps(param.info).encode(),
            content=base64.b64decode(param.content),
            config=param.config or TaskConfig(),
            timeout=param.timeout,
        )
    except asyncio.exceptions.TimeoutError as e:
        return RCallTask(err_code=ErrorCode.TIMEOUT)
    if result is None:
        return RCallTask()
    return RCallTask(data=RCallTaskData(
        id=param.id,
        result_info=json.loads(result.info.decode()),
        result_content=base64.b64encode(result.content).decode(),
    ))


class PPutTask(BaseModel):
    queue: str
    id: str
    info: Any = None
    content: str = '' # base64
    config: Optional[TaskConfig] = None


class RPutTask(BaseResponse):
    data: bool


@router.post(
    '/put',
    response_model=RPutTask,
)
async def api_put_task(
    param: PPutTask,
    tm: TaskManager = Depends(get_task_manager),
):
    result = await tm.put_task(
        queue=param.queue,
        id=param.id,
        info=json.dumps(param.info).encode(),
        content=base64.b64decode(param.content),
        config=param.config or TaskConfig(),
    )
    return RPutTask(data=result)


class PPeekTasks(BaseModel):
    queue: str
    cursor: Optional[str] = None
    size: int = 1
    timeout: int = 0


class RPeekTaskData(BaseModel):
    id: str
    info: Any
    cursor: str


class RPeekTasks(BaseResponse):
    data: List[RPeekTaskData]
    cursor: Optional[str]


@router.post(
    '/peek',
    response_model=RPeekTasks,
)
async def api_peek_tasks(
    param: PPeekTasks,
    tm: TaskManager = Depends(get_task_manager),
):
    result = await tm.peek_tasks(
        queue=param.queue,
        cursor=param.cursor,
        size=param.size,
        timeout=param.timeout,
    )
    return RPeekTasks(
        data=[RPeekTaskData(
            id=i.id,
            info=json.loads(i.info.decode()),
            cursor=i.cursor,
        ) for i in result],
        cursor = (result[-1] if result else param).cursor,
    )


class PAcquireTasks(BaseModel):
    queue: str
    ids: List[str]
    promise_reply_time: int


class RAcquireTaskData(BaseModel):
    id: str
    info: Any
    content: str # base64


class RAcquireTasks(BaseResponse):
    data: List[RAcquireTaskData]


@router.post(
    '/acquire',
    response_model=RAcquireTasks,
)
async def api_acquire_tasks(
    param: PAcquireTasks,
    tm: TaskManager = Depends(get_task_manager),
):
    result = await tm.acquire_tasks(
        queue=param.queue,
        ids=param.ids,
        promise_reply_time=param.promise_reply_time,
    )
    return RAcquireTasks(
        data=[RAcquireTaskData(
            id=i.id,
            info=json.loads(i.info.decode()),
            content=base64.b64encode(i.content).decode(),
        ) for i in result]
    )


class PReplyTask(BaseModel):
    queue: str
    id: str
    info: Any = None
    content: str = '' # base64
    # override_config: Optional[TaskConfig] = None


class RReplyTask(BaseResponse):
    data: bool


@router.post(
    '/reply',
    response_model=RReplyTask,
)
async def api_reply_task(
    param: PReplyTask,
    tm: TaskManager = Depends(get_task_manager),
):
    result = await tm.reply_task(
        queue=param.queue,
        id=param.id,
        info=json.dumps(param.info).encode(),
        content=base64.b64decode(param.content),
    )
    return RReplyTask(data=result)


class PRemoveTask(BaseModel):
    queue: str
    id: str


class RRemoveTask(BaseResponse):
    data: bool


@router.post(
    '/remove',
    response_model=RRemoveTask
)
async def api_remove_task(
    param: PRemoveTask,
    tm: TaskManager = Depends(get_task_manager),
):
    result = await tm.remove_task(
        queue=param.queue,
        id=param.id,
    )
    return RRemoveTask(data=result)
