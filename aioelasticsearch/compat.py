import asyncio
import sys

from functools import partial

PY_350 = sys.version_info >= (3, 5, 0)
PY_352 = sys.version_info >= (3, 5, 2)


def create_task(*, loop):
    try:
        return loop.create_task
    except AttributeError:
        try:
            return partial(asyncio.ensure_future, loop=loop)
        except AttributeError:
            return partial(getattr(asyncio, 'async'), loop=loop)
