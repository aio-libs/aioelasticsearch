# -*- coding: utf-8 -*-
import logging

import pytest

from aioelasticsearch.helpers import bulk

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
                                concurrency_limit=2,
                                stats_only=True)
    assert success == 10
    assert fails == 0

    success, fails = await bulk(es, gen_data2(),
                                concurrency_limit=2,
                                stats_only=True)
    assert success == 10
    assert fails == 0


@pytest.mark.run_loop
async def test_bulk_fails(es):
    datas = [{'op_type': 'delete',
              '_index': 'test_aioes',
              '_type': 'type_3', '_id': "999"}
             ]
    success, fails = await bulk(es, datas, stats_only=True)
    assert success == 0
    assert success == 1
