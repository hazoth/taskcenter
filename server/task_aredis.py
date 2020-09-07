import aredis
from aredis.utils import pairs_to_dict


# fix parse_xinfo_stream
def parse_xinfo_stream(response):
    res = pairs_to_dict(response)
    if res[b'first-entry']:
        res[b'first-entry'][1] = pairs_to_dict(res[b'first-entry'][1])
    if res[b'last-entry']:
        res[b'last-entry'][1] = pairs_to_dict(res[b'last-entry'][1])
    return res


aredis.StrictRedis.RESPONSE_CALLBACKS['XINFO STREAM'] = parse_xinfo_stream


class RedisWrapper:
    def __init__(self, client: aredis.StrictRedis):
        self._c = client

    async def eval(self, script, keys=[], args=[]):
        return await self._c.eval(script, len(keys), *(keys + args))

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
        result = await self._c.xread(
            # streams=dict(zip(streams, latest_ids)),
            count=count,
            block=timeout,
            **dict(zip([i.decode() for i in streams], latest_ids)),
        )
        lst = []
        for i, js in result.items():
            for j in js:
                lst.append((i, j[0], j[1]))
        return lst

    async def xinfo_stream(self, stream):
        try:
            result = await self._c.xinfo_stream(stream)
        except aredis.exceptions.ResponseError as e:
            return None
        return result

    async def close(self):
        self._c.close()


async def create_client(host, port, db=0):
    result = aredis.StrictRedis(
        host=host,
        port=port,
        db=db,
    )
    return RedisWrapper(result)
