import asyncio

import pytest

from aioelasticsearch import Elasticsearch


async def test_ping(es):
    ping = await es.ping()

    assert ping


@asyncio.coroutine
def test_ping2(es):
    ping = yield from es.ping()

    assert ping
