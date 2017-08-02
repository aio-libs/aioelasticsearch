import asyncio
import sys


PY_352 = sys.version_info >= (3, 5, 2)


def create_future(*, loop=None):
    if loop is None:
        loop = asyncio.get_event_loop()

    if PY_352:
        return loop.create_future()

    return asyncio.Future(loop=loop)
