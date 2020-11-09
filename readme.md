# Task Center

基于redis的轻量级分布式任务队列中心。

通过http接口方式进行put(发布)/peek(查看)/acquire(认领)/reply(回复)任务等操作。

## features

- http api, 不依赖特定编程语言。
- 支持分布式异步任务队列消费者。
- 异构的服务器消费者（比如CPU与GPU）之间自动负载均衡。
- 同步调用转异步处理。
- 多次同步调用转一次异步批处理。
- 简单，代码约千行左右。
- 支持部署多个taskcenter实例（由于实例之间通过redis共享数据，因此同一个任务的发步、查看、认领、回复等操作可以在不同实例上执行）。

## 安装

docker打包
```bash
sh build.sh
```
docker启动
```bash
docker-compose up -d
```
需要先在本地启动redis（5.0以上，参考docker-compose.yaml中被注释掉的部分）。

## 工作流程简介

taskcenter只需要消费者实现异步任务消费代码，生产者便可以用同步或异步两种方式工作。

有空再画图：）。

### 异步工作流程

生产者 put task -> taskcenter 队列A。

消费者 peek tasks <- taskcenter 队列A。

消费者 acquire tasks <- taskcenter 队列A（一个task只能被acquire一次）。

消费者 reply -> taskcenter 队列A。

taskcenter 转发reply结果 -> taskcenter 队列A-result。

生产者 peek tasks(长轮询) <- taskcenter 队列A-result。

生产者 更新数据库任务状 <- taskcenter 队列A-result。

生产者 reply -> taskcenter 队列A-result(清除已处理的任务结果)。

### 同步工作流程

生产者 call task -> taskcenter 队列A（同步调用，等待任务处理完成才返回）。

消费者 peek tasks <- taskcenter 队列A。

消费者 acquire tasks <- taskcenter 队列A（一个task只能被acquire一次）。

消费者 reply -> taskcenter 队列A。

taskcenter 返回call结果 -> 生产者。

## Example

开发消费者代码时可以参考client/example.py, 继承TaskConsumer类，实现accept和process方法。

## API 参考

taskcenter使用fastapi框架开发，启动服务后，可以通过/docs路径查看API文档。

### /task/put

参数
```json
{
    "queue": "queue name",
    "id": "task-id",
    "info": {
        "custom-param1-key": "custom-param1-value",
        "custom-param2-key": "custom-param2-value",
        "...": "...",
    },
    "content": "base64 encoded binary task data",
    "config": {
        "forward_result_to": "another queue name to forward reply data to"
    }
}
```

返回
```json
{
    "data": true
}
```
true 表示put成功，false表示任务已存在。

### /task/peek

参数
```json
{
    "queue": "queue name",
    "size": 1,
    "timeout": 10
}
```
返回
```json
{
    "data": [
        {
            "id": "task-id",
            "info": {
                "custom-param1-key": "custom-param1-value",
                "custom-param2-key": "custom-param2-value",
                "...": "...",
            },
        }
    ],
    "cursor": "cursor of the last item of peeked data"
}
```

### /task/acquire

参数
```json
{
    "queue": "queue name",
    "ids": ["task-id"],
    "promise_reply_time": 15,
}
```

返回
```json
{
    "data": [
        {
            "id": "task-id",
            "info": {
                "custom-param1-key": "custom-param1-value",
                "custom-param2-key": "custom-param2-value",
                "...": "...",
            },
            "content": "base64 encoded binary task data",
        }
    ],
}
```

### /task/reply

参数
```json
{
    "queue": "queue name",
    "id": "task-id",
    "info": {
        "result-key1": "result-value1",
        "...": "...",
    },
    "content": "base64 encoded binary result data",
}

返回
```json
{
    "data": true,
}
```
false 表示task-id不存在。

### /task/call

参数
```json
{
    "queue": "queue name",
    "id": "task-id",
    "info": {
        "custom-param1-key": "custom-param1-value",
        "custom-param2-key": "custom-param2-value",
        "...": "...",
    },
    "content": "base64 encoded binary task data",
    "config": {
        "cache_result_time": 3,
    },
    "timeout": 10,
}
```

返回
```json
{
    "err_code": "0000",
    "data": [
        {
            "id": "task-id",
            "result_info": {
                "result-key1": "result-value1",
                "...": "...",
            },
            "result_content": "base64 encoded binary result data"
        }
    ]
}
```
err_code="0001" 表示超时。

### /task/remove

删除任务

### /task/clear_queue

清空指定队列中的所有数据

### /task/list_queue

列举所有存在的队列

### /task/stat_queue

查看队列信息
