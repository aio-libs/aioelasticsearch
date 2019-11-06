import pytest

from aioelasticsearch import (AIOHttpTransport, ConnectionError,
                              ConnectionTimeout, Elasticsearch, TransportError)
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
                                    sniffer_timeout=10, loop=loop))
    assert t.initial_sniff_task is None
    t.last_sniff -= 15
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


@pytest.mark.run_loop
async def test_sniff_hosts_error(auto_close, loop, es_server):
    t = auto_close(AIOHttpTransport([{'host': 'unknown_host',
                                      'port': 9200}],
                                    loop=loop))
    with pytest.raises(TransportError):
        await t.sniff_hosts()


@pytest.mark.run_loop
async def test_sniff_hosts_no_hosts(auto_close, loop, es_server):
    t = auto_close(AIOHttpTransport([{'host': es_server['host'],
                                      'port': es_server['port']}],
                                    http_auth=es_server['auth'],
                                    loop=loop))
    t.host_info_callback = lambda host_info, host: None
    with pytest.raises(TransportError):
        await t.sniff_hosts()


@pytest.mark.run_loop
async def test_mark_dead(auto_close, loop, es_server):
    t = auto_close(AIOHttpTransport([{'host': 'unknown_host',
                                      'port': 9200},
                                     {'host': es_server['host'],
                                      'port': es_server['port']}],
                                    http_auth=es_server['auth'],
                                    randomize_hosts=False,
                                    loop=loop))
    conn = t.connection_pool.connections[0]
    assert conn is not None
    assert conn.host == 'http://unknown_host:9200'
    await t.mark_dead(conn)
    assert len(t.connection_pool.connections) == 1


@pytest.mark.run_loop
async def test_mark_dead_with_sniff(auto_close, loop, es_server):
    t = auto_close(AIOHttpTransport([{'host': 'unknown_host',
                                      'port': 9200},
                                     {'host': 'unknown_host2',
                                      'port': 9200},
                                     {'host': es_server['host'],
                                      'port': es_server['port']}],
                                    http_auth=es_server['auth'],
                                    sniff_on_connection_fail=True,
                                    randomize_hosts=False,
                                    loop=loop))
    conn = t.connection_pool.connections[0]
    assert conn is not None
    assert conn.host == 'http://unknown_host:9200'
    await t.mark_dead(conn)
    assert len(t.connection_pool.connections) == 1


@pytest.mark.run_loop
async def test_send_get_body_as_post(es_server, auto_close, loop):
    cl = auto_close(Elasticsearch([{'host': es_server['host'],
                                   'port': es_server['port']}],
                                  send_get_body_as='POST',
                                  http_auth=es_server['auth'],
                                  loop=loop))
    await cl.create('test', '1', {'val': '1'})
    await cl.create('test', '2', {'val': '2'})
    ret = await cl.mget(
        {"docs": [
                {"_id": "1"},
                {"_id": "2"}
        ]},
        index='test',
    )
    assert ret == {'docs': [{'_id': '1',
                             '_index': 'test',
                             '_source': {'val': '1'},
                             '_type': '_doc',
                             '_version': 1,
                             '_primary_term': 1,
                             '_seq_no': 0,
                             'found': True},
                            {'_id': '2',
                             '_index': 'test',
                             '_source': {'val': '2'},
                             '_type': '_doc',
                             '_version': 1,
                             '_primary_term': 1,
                             '_seq_no': 1,
                             'found': True}]}


@pytest.mark.run_loop
async def test_send_get_body_as_source(es_server, auto_close, loop):
    cl = auto_close(Elasticsearch([{'host': es_server['host'],
                                   'port': es_server['port']}],
                                  send_get_body_as='source',
                                  http_auth=es_server['auth'],
                                  loop=loop))
    await cl.create('test', '1', {'val': '1'})
    await cl.create('test', '2', {'val': '2'})
    ret = await cl.mget(
        {"docs": [
                {"_id": "1"},
                {"_id": "2"}
        ]},
        index='test',
    )
    assert ret == {'docs': [{'_id': '1',
                             '_index': 'test',
                             '_source': {'val': '1'},
                             '_type': '_doc',
                             '_version': 1,
                             '_primary_term': 1,
                             '_seq_no': 0,
                             'found': True},
                            {'_id': '2',
                             '_index': 'test',
                             '_source': {'val': '2'},
                             '_type': '_doc',
                             '_version': 1,
                             '_primary_term': 1,
                             '_seq_no': 1,
                             'found': True}]}


@pytest.mark.run_loop
async def test_send_get_body_as_get(es_server, auto_close, loop):
    cl = auto_close(Elasticsearch([{'host': es_server['host'],
                                   'port': es_server['port']}],
                                  http_auth=es_server['auth'],
                                  loop=loop))
    await cl.create('test', '1', {'val': '1'})
    await cl.create('test', '2', {'val': '2'})
    ret = await cl.mget(
        {"docs": [
                {"_id": "1"},
                {"_id": "2"}
        ]},
        index='test',
    )
    assert ret == {'docs': [{'_id': '1',
                             '_index': 'test',
                             '_source': {'val': '1'},
                             '_type': '_doc',
                             '_version': 1,
                             '_primary_term': 1,
                             '_seq_no': 0,
                             'found': True},
                            {'_id': '2',
                             '_index': 'test',
                             '_source': {'val': '2'},
                             '_type': '_doc',
                             '_version': 1,
                             '_primary_term': 1,
                             '_seq_no': 1,
                             'found': True}]}


@pytest.mark.run_loop
async def test_send_get_body_as_source_none_params(es_server,
                                                   auto_close, loop):
    cl = auto_close(Elasticsearch([{'host': es_server['host'],
                                   'port': es_server['port']}],
                                  send_get_body_as='source',
                                  http_auth=es_server['auth'],
                                  loop=loop))
    await cl.create('test', '1', {'val': '1'})
    await cl.create('test', '2', {'val': '2'})
    ret = await cl.transport.perform_request(
        'GET', 'test/_mget',
        body={"docs": [
            {"_id": "1"},
            {"_id": "2"}
        ]})
    assert ret == {'docs': [{'_id': '1',
                             '_index': 'test',
                             '_source': {'val': '1'},
                             '_type': '_doc',
                             '_version': 1,
                             '_primary_term': 1,
                             '_seq_no': 0,
                             'found': True},
                            {'_id': '2',
                             '_index': 'test',
                             '_source': {'val': '2'},
                             '_type': '_doc',
                             '_version': 1,
                             '_primary_term': 1,
                             '_seq_no': 1,
                             'found': True}]}


@pytest.mark.run_loop
async def test_set_connections_closed(es):
    await es.close()
    with pytest.raises(RuntimeError):
        es.transport.set_connections(['host1', 'host2'])


@pytest.mark.run_loop
async def test_sniff_hosts_closed(es):
    await es.close()
    with pytest.raises(RuntimeError):
        await es.transport.sniff_hosts()


@pytest.mark.run_loop
async def test_close_closed(es):
    await es.close()
    await es.close()


@pytest.mark.run_loop
async def test_get_connection_closed(es):
    await es.close()
    with pytest.raises(RuntimeError):
        await es.transport.get_connection()


@pytest.mark.run_loop
async def test_mark_dead_closed(es):
    await es.close()
    conn = object()
    with pytest.raises(RuntimeError):
        await es.transport.mark_dead(conn)


@pytest.mark.run_loop
async def test_perform_request_closed(es):
    await es.close()
    with pytest.raises(RuntimeError):
        await es.transport.perform_request('GET', '/')


@pytest.mark.run_loop
async def test_request_error_404_on_head(loop, auto_close):
    exc = TransportError(404)
    t = AIOHttpTransport([{}], connection_class=DummyConnection, loop=loop,
                         exception=exc)
    auto_close(t)

    ret = await t.perform_request('HEAD', '/')
    assert not ret


@pytest.mark.run_loop
async def test_request_connection_error(loop, auto_close):
    exc = ConnectionError()
    t = AIOHttpTransport([{}], connection_class=DummyConnection, loop=loop,
                         exception=exc)
    auto_close(t)

    with pytest.raises(ConnectionError):
        await t.perform_request('GET', '/')

    conn = await t.get_connection()
    assert len(conn.calls) == 3


@pytest.mark.run_loop
async def test_request_connection_timeout(loop, auto_close):
    exc = ConnectionTimeout()
    t = AIOHttpTransport([{}], connection_class=DummyConnection, loop=loop,
                         exception=exc)
    auto_close(t)

    with pytest.raises(ConnectionTimeout):
        await t.perform_request('GET', '/')

    conn = await t.get_connection()
    assert len(conn.calls) == 1


@pytest.mark.run_loop
async def test_request_connection_timeout_with_retry(loop, auto_close):
    exc = ConnectionTimeout()
    t = AIOHttpTransport([{}], connection_class=DummyConnection, loop=loop,
                         exception=exc, retry_on_timeout=True)
    auto_close(t)

    with pytest.raises(ConnectionTimeout):
        await t.perform_request('GET', '/')

    conn = await t.get_connection()
    assert len(conn.calls) == 3


@pytest.mark.run_loop
async def test_request_retry_on_status(loop, auto_close):
    exc = TransportError(500)
    t = AIOHttpTransport([{}], connection_class=DummyConnection, loop=loop,
                         exception=exc, retry_on_status=(500,))
    auto_close(t)

    with pytest.raises(TransportError):
        await t.perform_request('GET', '/')

    conn = await t.get_connection()
    assert len(conn.calls) == 3


@pytest.mark.run_loop
async def test_request_without_data(loop, auto_close):
    t = AIOHttpTransport([{}], connection_class=DummyConnection, loop=loop,
                         data='')
    auto_close(t)

    ret = await t.perform_request('GET', '/')
    assert ret == ''


@pytest.mark.run_loop
async def test_request_headers(loop, auto_close, es_server, mocker):
    t = auto_close(AIOHttpTransport(
        [{'host': es_server['host'],
          'port': es_server['port']}],
        http_auth=es_server['auth'],
        loop=loop,
        headers={'H1': 'V1', 'H2': 'V2'},
    ))

    for conn in t.connection_pool.connections:
        mocker.spy(conn.session, 'request')

    await t.perform_request('GET', '/', headers={'H1': 'VV1', 'H3': 'V3'})

    session = (await t.get_connection()).session
    _, kwargs = session.request.call_args
    assert kwargs['headers'] == {
        'H1': 'VV1',
        'H2': 'V2',
        'H3': 'V3',
        'Content-Type': 'application/json',
    }
