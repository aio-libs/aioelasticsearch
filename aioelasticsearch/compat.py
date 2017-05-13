import asyncio
import sys

import aiohttp

try:
    from asyncio import ensure_future
except ImportError:
    ensure_future = getattr(asyncio, 'async')


PY_344 = sys.version_info >= (3, 4, 4)
PY_350 = sys.version_info >= (3, 5, 0)
PY_352 = sys.version_info >= (3, 5, 2)

AIOHTTP_2 = aiohttp.__version__ >= '2.0.0'


def create_future(*, loop=None):
    if loop is None:
        loop = asyncio.get_event_loop()

    if PY_352:
        return loop.create_future()

    return asyncio.Future(loop=loop)
