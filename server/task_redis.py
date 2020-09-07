import redis
import socket


class RedisWrapper:
    def __init__(self, client: redis.Redis):
        self._c = client

    async def eval(self, script, keys=[], args=[]):
        return self._c.eval(script, len(keys), *(keys + args))

    async def hmget(self, *args):
        return self._c.hmget(*args)

    async def delete(self, *args):
        return self._c.delete(*args)

    async def scan(self, cursor, match, count):
        return self._c.scan(
            cursor=cursor,
            match=match,
            count=count,
        )

    async def xread(self, streams, timeout, count, latest_ids):
        result = self._c.xread(
            streams=dict(zip(streams, latest_ids)),
            count=count,
            block=timeout,
        )
        lst = []
        for i, js in result:
            for j in js:
                lst.append((i, j[0], j[1]))
        return lst

    async def xinfo_stream(self, stream):
        result = self._c.xinfo_stream(stream)
        return {
            b'length': result['length'],
            b'first-entry': result['first-entry'],
            b'last-entry': result['last-entry'],
        }

    async def close(self):
        self._c.close()


async def create_client(host, port, db=0):
    result = redis.Redis(
        host=host,
        port=port,
        db=db,
        socket_keepalive=True, 
        socket_keepalive_options={
            socket.TCP_KEEPIDLE: 120,
            socket.TCP_KEEPINTVL: 5,
            socket.TCP_KEEPCNT: 3,
            },
    )
    return RedisWrapper(result)
