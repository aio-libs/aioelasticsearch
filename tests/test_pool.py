import pytest


from aioelasticsearch import Elasticsearch, AIOHttpConnectionPool


@pytest.mark.run_loop
async def test_mark_dead_removed_connection(auto_close, es_server, loop):
    es = auto_close(Elasticsearch(hosts=[{'host': es_server['host'],
                                          'port': es_server['port']},
                                         {'host': 'unknown_host',
                                          'port': 9200}],
                                  http_auth=es_server['auth'],
                                  loop=loop))
    conn = await es.transport.get_connection()
    pool = es.transport.connection_pool
    pool.mark_dead(conn)
    assert conn in pool.dead_count
    # second call should succeed
    pool.mark_dead(conn)
    assert conn in pool.dead_count


@pytest.mark.run_loop
async def test_mark_live(auto_close, es_server, loop):
    es = auto_close(Elasticsearch(hosts=[{'host': es_server['host'],
                                          'port': es_server['port']},
                                         {'host': 'unknown_host',
                                          'port': 9200}],
                                  http_auth=es_server['auth'],
                                  loop=loop))
    conn = await es.transport.get_connection()
    pool = es.transport.connection_pool
    pool.mark_dead(conn)
    assert conn in pool.dead_count

    pool.mark_live(conn)
    assert conn not in pool.dead_count


@pytest.mark.run_loop
async def test_mark_live_not_dead(auto_close, es_server, loop):
    es = auto_close(Elasticsearch(hosts=[{'host': es_server['host'],
                                          'port': es_server['port']},
                                         {'host': 'unknown_host',
                                          'port': 9200}],
                                  http_auth=es_server['auth'],
                                  loop=loop))
    conn = await es.transport.get_connection()
    pool = es.transport.connection_pool
    pool.mark_live(conn)
    assert conn not in pool.dead_count


@pytest.mark.run_loop
async def test_resurrect_empty(loop):
    conn1 = object()
    conn2 = object()
    conns = [(conn1, object()), (conn2, object())]
    pool = AIOHttpConnectionPool(connections=conns,
                                 randomize_hosts=False, loop=loop)
    pool.resurrect()
    assert pool.connections == [conn1, conn2]


@pytest.mark.run_loop
async def test_resurrect_empty_force(loop):
    conn1 = object()
    conn2 = object()
    conns = [(conn1, object()), (conn2, object())]
    pool = AIOHttpConnectionPool(connections=conns,
                                 randomize_hosts=False, loop=loop)
    assert pool.resurrect(force=True) in (conn1, conn2)
