import pytest

from aioelasticsearch import AIOHttpTransport
from aioelasticsearch.connection import AIOHttpConnection


class DummyConnection(AIOHttpConnection):
    def __init__(self, **kwargs):
        self.exception = kwargs.pop('exception', None)
        self.status = kwargs.pop('status', 200)
        self.data = kwargs.pop('data', '{}')
        self.headers = kwargs.pop('headers', {})
        self.calls = []
        super().__init__(**kwargs)

    async def perform_request(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        if self.exception:
            raise self.exception
        return self.status, self.headers, self.data


@pytest.mark.run_loop
async def test_body_surrogates_replaced_encoded_into_bytes(loop, auto_close):
    t = AIOHttpTransport([{}], connection_class=DummyConnection, loop=loop)
    auto_close(t)

    await t.perform_request('GET', '/', body='你好\uda6a')
    conn = await t.get_connection()

    assert len(conn.calls) == 1
    assert ('GET', '/', None,
            b'\xe4\xbd\xa0\xe5\xa5\xbd\xed\xa9\xaa') == conn.calls[0][0]


@pytest.mark.run_loop
async def test_custom_serializers(auto_close, loop):
    serializer = object()
    t = auto_close(AIOHttpTransport([{}],
                                    serializers={'test': serializer},
                                    loop=loop))
    assert 'test' in t.deserializer.serializers
    assert t.deserializer.serializers['test'] is serializer


@pytest.mark.run_loop
async def test_no_sniff_on_start(auto_close, loop):
    t = auto_close(AIOHttpTransport([{}], sniff_on_start=False, loop=loop))
    assert t.initial_sniff_task is None


@pytest.mark.run_loop
async def test_sniff_on_start(auto_close, loop, es_server):
    t = auto_close(AIOHttpTransport([{'host': 'unknown_host',
                                      'port': 9200},
                                     {'host': es_server['host'],
                                      'port': es_server['port']}],
                                    http_auth=es_server['auth'],
                                    sniff_on_start=True, loop=loop))
    assert t.initial_sniff_task is not None
    await t.initial_sniff_task
    assert t.initial_sniff_task is None
    assert len(t.connection_pool.connections) == 1


@pytest.mark.run_loop
async def test_close_with_sniff_on_start(loop, es_server):
    t = AIOHttpTransport([{'host': es_server['host'],
                           'port': es_server['port']}],
                         http_auth=es_server['auth'],
                         sniff_on_start=True, loop=loop)
    assert t.initial_sniff_task is not None
    await t.close()
    assert t.initial_sniff_task is None
    assert t._closed


@pytest.mark.run_loop
async def test_get_connection_with_sniff_on_start(auto_close, loop, es_server):
    t = auto_close(AIOHttpTransport([{'host': es_server['host'],
                                      'port': es_server['port']}],
                                    http_auth=es_server['auth'],
                                    sniff_on_start=True, loop=loop))
    conn = await t.get_connection()
    assert conn is not None
    assert t.initial_sniff_task is None


@pytest.mark.run_loop
async def test_get_connection_with_sniffer_timeout(auto_close,
                                                   loop, es_server):
    t = auto_close(AIOHttpTransport([{'host': 'unknown_host',
                                      'port': 9200},
                                     {'host': es_server['host'],
                                      'port': es_server['port']}],
                                    http_auth=es_server['auth'],
                                    sniffer_timeout=1e-12, loop=loop))
    conn = await t.get_connection()
    assert conn is not None
    assert t.initial_sniff_task is None
    assert len(t.connection_pool.connections) == 1


@pytest.mark.run_loop
async def test_get_connection_without_sniffer_timeout(auto_close,
                                                   loop, es_server):
    t = auto_close(AIOHttpTransport([{'host': 'unknown_host',
                                      'port': 9200},
                                     {'host': es_server['host'],
                                      'port': es_server['port']}],
                                    http_auth=es_server['auth'],
                                    sniffer_timeout=1e12, loop=loop))
    conn = await t.get_connection()
    assert conn is not None
    assert t.initial_sniff_task is None
    assert len(t.connection_pool.connections) == 2
