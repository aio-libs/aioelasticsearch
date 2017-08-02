import asyncio

import pytest

from aioelasticsearch import Elasticsearch


@pytest.mark.run_loop
async def test_ping(es):
    ping = await es.ping()

    assert ping


@pytest.mark.run_loop
async def test_str_auth(es, es_server, loop):
    async with Elasticsearch(loop=loop, hosts=[{'host': es_server['host'],
                                                'port': es_server['port']}],
                             http_auth=':'.join(es_server['auth'])) as es1:
        await es1.ping()


@pytest.mark.run_loop
async def test_elastic_default_loop(es, loop, es_server):
    asyncio.set_event_loop(loop)

    async with Elasticsearch(hosts=[{'host': es_server['host'],
                                     'port': es_server['port']}],
                             http_auth=':'.join(es_server['auth'])) as es1:
        assert es1._loop is loop
