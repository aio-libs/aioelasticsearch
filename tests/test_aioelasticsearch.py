import asyncio

import pytest

from aioelasticsearch import Elasticsearch


@pytest.mark.run_loop
async def test_ping(es):
    ping = await es.ping()

    assert ping


def test_elastic_default_loop(loop, auto_close, es_server):
    asyncio.set_event_loop(loop)

    es = Elasticsearch(hosts=[{'host': es_server['host'],
                               'port': es_server['port']}],
                       http_auth=es_server['auth'])

    auto_close(es)

    assert es.loop is loop
