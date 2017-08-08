import pytest


from aioelasticsearch import Elasticsearch


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
