import asyncio
import logging
from copy import deepcopy
from unittest import mock

import pytest

from aioelasticsearch import ElasticsearchException
from aioelasticsearch.helpers import CompositeAggregationScan

logger = logging.getLogger('elasticsearch')


ES_DATA = [
    {
        'score': '1',
    },
    {
        'score': '2',
    },
    {
        'score': '2',
    },
    {
        'score': '3',
    },
    {
        'score': '3',
    },
    {
        'score': '3',
    },
]


QUERY = {
    'aggs': {
        'buckets': {
            'composite': {
                'sources': [
                    {'score': {'terms': {'field': 'score.keyword'}}},
                ],
            },
        },
    },
}

INDEX = 'test_aioes'


@pytest.fixture
def populate_aggs_data(loop, es):

    async def do(index, docs):
        coros = []

        await es.indices.create(index)

        for i, doc in enumerate(docs):
            coros.append(
                es.index(
                    index=index,
                    id=str(i),
                    body=doc,
                ),
            )

        await asyncio.gather(*coros, loop=loop)
        await es.indices.refresh()

    return do


@pytest.mark.run_loop
async def test_async_for_without_context_manager(es):
    scan = CompositeAggregationScan(es, QUERY)

    with pytest.raises(RuntimeError):
        async for doc in scan:
            doc


@pytest.mark.run_loop
async def test_non_aggregation_query(es):
    with pytest.raises(RuntimeError):
        CompositeAggregationScan(
            es,
            {
                'query': {
                    'bool': {
                        'match_all': {},
                    },
                },
            },
        )


@pytest.mark.run_loop
async def test_non_composite_aggregation(es):
    with pytest.raises(RuntimeError):
        CompositeAggregationScan(
            es,
            {
                'query': {
                    'aggs': {
                        'counts': {
                            'value_count': {'field': 'domains'},
                        },
                    },
                },
            },
        )


@pytest.mark.run_loop
async def test_scan(loop, es, populate_aggs_data):
    await populate_aggs_data(INDEX, ES_DATA)

    async with CompositeAggregationScan(
        es,
        QUERY,
        loop=loop,
        index=INDEX,
    ) as scan:
        i = 1
        async for doc in scan:
            assert doc == {
                'key': {'score': str(i)},
                'doc_count': i,
            }

            i += 1

        assert i == 4


@pytest.mark.run_loop
async def test_scan_no_index(loop, es, populate_aggs_data):
    await populate_aggs_data(INDEX, ES_DATA)

    async with CompositeAggregationScan(
        es,
        QUERY,
        loop=loop,
    ) as scan:
        i = 1
        async for doc in scan:
            assert doc == {
                'key': {'score': str(i)},
                'doc_count': i,
            }

            i += 1

        assert i == 4


@pytest.mark.run_loop
async def test_scan_multiple_fetch(loop, es, populate_aggs_data):
    await populate_aggs_data(INDEX, ES_DATA)

    q = deepcopy(QUERY)
    q['aggs']['buckets']['composite']['size'] = 1

    async with CompositeAggregationScan(
        es,
        q,
        loop=loop,
        index=INDEX,
    ) as scan:
        i = 1
        original_update = scan._update_state
        mock_update = mock.MagicMock(side_effect=original_update)
        with mock.patch.object(scan, '_update_state', mock_update):

            async for doc in scan:
                assert doc == {
                    'key': {'score': str(i)},
                    'doc_count': i,
                }

                i += 1

            assert mock_update.call_count == 3


@pytest.mark.run_loop
async def test_scan_with_prefetch_next(loop, es, populate_aggs_data):
    await populate_aggs_data(INDEX, ES_DATA)

    q = deepcopy(QUERY)
    q['aggs']['buckets']['composite']['size'] = 1

    async with CompositeAggregationScan(
        es,
        q,
        loop=loop,
        prefetch_next_chunk=True,
        index=INDEX,
    ) as scan:
        original_reset_prefetch = scan._reset_prefetched
        mock_reset_prefetch = mock.MagicMock(
            side_effect=original_reset_prefetch,
        )
        with mock.patch.object(
            scan,
            '_reset_prefetched',
            mock_reset_prefetch,
        ):
            async for _ in scan:  # noqa
                assert scan._prefetched is not None

            assert mock_reset_prefetch.call_count == 3

            last_prefetch_task = scan._prefetched

    await asyncio.sleep(0)

    assert last_prefetch_task.cancelled()
    assert scan._prefetched is None


@pytest.mark.run_loop
async def test_scan_warning_on_failed_shards(
    loop,
    es,
    populate_aggs_data,
    mocker,
):
    mocker.spy(logger, 'warning')

    await populate_aggs_data(INDEX, ES_DATA)

    async with CompositeAggregationScan(
        es,
        QUERY,
        loop=loop,
        raise_on_error=False,
        index=INDEX,
    ) as scan:
        i = 0
        async for doc in scan:  # noqa
            if i == 1:
                scan._successful_shards = 4
                scan._total_shards = 5
            i += 1

    logger.warning.assert_called_once_with(
        'Aggregation request has only succeeded on %d shards out of %d.',
        4,
        5,
    )


@pytest.mark.run_loop
async def test_scan_exception_on_failed_shards(
    loop,
    es,
    populate_aggs_data,
    mocker,
):
    mocker.spy(logger, 'warning')

    await populate_aggs_data(INDEX, ES_DATA)

    async with CompositeAggregationScan(
        es,
        QUERY,
        loop=loop,
        index=INDEX,
    ) as scan:
        i = 0
        with pytest.raises(ElasticsearchException):
            async for doc in scan:  # noqa
                if i == 1:
                    scan._successful_shards = 4
                    scan._total_shards = 5
                i += 1

    assert i == 3
    logger.warning.assert_called_once_with(
        'Aggregation request has only succeeded on %d shards out of %d.', 4, 5,
    )
