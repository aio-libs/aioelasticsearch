import asyncio

import pytest

from aioelasticsearch import Elasticsearch


@pytest.mark.run_loop
async def test_ping(es):
    ping = await es.ping()

    assert ping


def test_elastic_default_loop(auto_close, loop):
    asyncio.set_event_loop(loop)

    es = Elasticsearch()

    auto_close(es)

    assert es.loop is loop
