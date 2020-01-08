import pytest

from aioelasticsearch import (AIOHttpConnectionPool, Elasticsearch,
                              ImproperlyConfigured)
from aioelasticsearch.pool import DummyConnectionPool


async def test_mark_dead_removed_connection(auto_close, es_server):
    es = auto_close(Elasticsearch(hosts=[{'host': es_server['host'],
                                          'port': es_server['port']},
                                         {'host': 'unknown_host',
                                          'port': 9200}],
                                  http_auth=es_server['auth']))
    conn = await es.transport.get_connection()
    pool = es.transport.connection_pool
    pool.mark_dead(conn)
    assert conn in pool.dead_count
    # second call should succeed
    pool.mark_dead(conn)
    assert conn in pool.dead_count


async def test_mark_live(auto_close, es_server):
    es = auto_close(Elasticsearch(hosts=[{'host': es_server['host'],
                                          'port': es_server['port']},
                                         {'host': 'unknown_host',
                                          'port': 9200}],
                                  http_auth=es_server['auth']))
    conn = await es.transport.get_connection()
    pool = es.transport.connection_pool
    pool.mark_dead(conn)
    assert conn in pool.dead_count

    pool.mark_live(conn)
    assert conn not in pool.dead_count


async def test_mark_live_not_dead(auto_close, es_server):
    es = auto_close(Elasticsearch(hosts=[{'host': es_server['host'],
                                          'port': es_server['port']},
                                         {'host': 'unknown_host',
                                          'port': 9200}],
                                  http_auth=es_server['auth']))
    conn = await es.transport.get_connection()
    pool = es.transport.connection_pool
    pool.mark_live(conn)
    assert conn not in pool.dead_count


async def test_resurrect_empty():
    conn1 = object()
    conn2 = object()
    conns = [(conn1, object()), (conn2, object())]
    pool = AIOHttpConnectionPool(connections=conns, randomize_hosts=False)
    pool.resurrect()
    assert pool.connections == [conn1, conn2]


async def test_resurrect_empty_force():
    conn1 = object()
    conn2 = object()
    conns = [(conn1, object()), (conn2, object())]
    pool = AIOHttpConnectionPool(connections=conns, randomize_hosts=False)
    assert pool.resurrect(force=True) in (conn1, conn2)


async def test_resurrect_from_dead_not_ready_connection():
    conn1 = object()
    conn2 = object()
    conns = [(conn1, object()), (conn2, object())]
    pool = AIOHttpConnectionPool(connections=conns, randomize_hosts=False)
    pool.mark_dead(conn1)
    pool.resurrect()
    assert pool.connections == [conn2]


async def test_resurrect_from_dead_ready_connection():
    conn1 = object()
    conn2 = object()
    conns = [(conn1, object()), (conn2, object())]
    pool = AIOHttpConnectionPool(connections=conns, randomize_hosts=False)
    pool.dead_timeout = lambda t: 0
    pool.mark_dead(conn1)
    pool.resurrect()
    assert pool.connections == [conn2, conn1]


async def test_get_connections_only_one_conn():
    conn1 = object()
    conn2 = object()
    conns = [(conn1, object()), (conn2, object())]
    pool = AIOHttpConnectionPool(connections=conns, randomize_hosts=False)
    pool.mark_dead(conn1)
    conn = pool.get_connection()
    assert conn is conn2


async def test_get_connections_no_conns():
    conn1 = object()
    conn2 = object()
    conns = [(conn1, object()), (conn2, object())]
    pool = AIOHttpConnectionPool(connections=conns, randomize_hosts=False)
    pool.mark_dead(conn1)
    pool.mark_dead(conn2)
    conn = pool.get_connection()
    assert conn in (conn1, conn2)


async def test_dummy_improperly_configured():
    conn1 = object()
    conn2 = object()
    conns = [(conn1, object()), (conn2, object())]
    with pytest.raises(ImproperlyConfigured):
        DummyConnectionPool(connections=conns)


async def test_dummy_mark_dead_and_live():
    conn1 = object()
    conns = [(conn1, object())]

    pool = DummyConnectionPool(connections=conns)
    pool.mark_dead(conn1)
    assert pool.connections == [conn1]

    pool.mark_live(conn1)
    assert pool.connections == [conn1]

    pool.resurrect()
    assert pool.connections == [conn1]
