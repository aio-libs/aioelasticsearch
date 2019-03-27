# -*- coding: utf-8 -*-
import asyncio
import logging

import pytest

from aioelasticsearch.helpers import bulk, concurrency_bulk, _retry_handler
from aioelasticsearch import Elasticsearch, TransportError

logger = logging.getLogger('elasticsearch')


def gen_data1():
    for i in range(10):
        yield {"_index": "test_aioes",
               "_type": "type_3",
               "_id": str(i),
               "foo": "1"}


def gen_data2():
    for i in range(10, 20):
        yield {"_index": "test_aioes",
               "_type": "type_3",
               "_id": str(i),
               "_source": {"foo": "1"}
               }


@pytest.mark.run_loop
async def test_bulk_simple(es):
    success, fails = await bulk(es, gen_data1(),
                                stats_only=True)
    assert success == 10
    assert fails == 0

    success, fails = await bulk(es, gen_data2(),
                                stats_only=True)
    assert success == 10
    assert fails == 0

    success, fails = await bulk(es, gen_data1(),
                                stats_only=False)
    assert success == 10
    assert fails == []


@pytest.mark.run_loop
async def test_bulk_fails(es):
    datas = [{'_op_type': 'delete',
              '_index': 'test_aioes',
              '_type': 'type_3', '_id': "999"}
             ]

    success, fails = await bulk(es, datas, stats_only=True, max_retries=1)
    assert success == 0
    assert fails == 1


@pytest.mark.run_loop
async def test_concurrency_bulk(es):
    success, fails = await concurrency_bulk(es, gen_data1())
    assert success == 10
    assert fails == 0

    success, fails = await concurrency_bulk(es, gen_data2())
    assert success == 10
    assert fails == 0


@pytest.mark.run_loop
async def test_bulk_raise_exception(loop):

    asyncio.set_event_loop(loop)
    es = Elasticsearch()
    datas = [{'_op_type': 'delete',
              '_index': 'test_aioes',
              '_type': 'type_3', '_id': "999"}
             ]
    with pytest.raises(TransportError):
        success, fails = await bulk(es, datas, stats_only=True)


@pytest.mark.run_loop
async def test_retry_handler(es):
    async def mock_data():
        # finish_count, [( es_action, source_data ), ... ]
        return 0, [(
            {'index': {'_index': 'test_aioes', '_type': 'test_aioes', '_id': 100}},
            {'name': 'Karl 1', 'email': 'karl@example.com'}),
            ({'index': {'_index': 'test_aioes', '_type': 'test_aioes','_id': 101}},
             {'name': 'Karl 2', 'email': 'karl@example.com'})]

    done, fail = await _retry_handler(es,
                                      mock_data(),
                                      max_retries=1,
                                      initial_backoff=2,
                                      max_backoff=600)
    assert done == 2
    assert fail == []
