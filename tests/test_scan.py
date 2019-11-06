import logging
from unittest import mock

import pytest

from aioelasticsearch import NotFoundError
from aioelasticsearch.helpers import Scan, ScanError

logger = logging.getLogger('elasticsearch')


def test_scan_total_without_context_manager(es):
    scan = Scan(es)

    with pytest.raises(RuntimeError):
        scan.total


@pytest.mark.run_loop
async def test_scan_async_for_without_context_manager(es):
    scan = Scan(es)

    with pytest.raises(RuntimeError):
        async for doc in scan:
            doc


def test_scan_scroll_id_without_context_manager(es):
    scan = Scan(es)

    with pytest.raises(RuntimeError):
        scan.scroll_id


@pytest.mark.run_loop
async def test_scan_simple(es, populate):
    index = 'test_aioes'
    scroll_size = 3
    n = 10

    body = {'foo': 1}
    await populate(index, n, body)
    ids = set()

    async with Scan(
        es,
        index=index,
        size=scroll_size,
    ) as scan:
        assert isinstance(scan.scroll_id, str)
        assert scan.total['value'] == 10
        async for doc in scan:
            ids.add(doc['_id'])
            assert doc == {'_id': mock.ANY,
                           '_index': 'test_aioes',
                           '_score': None,
                           '_source': {'foo': 1},
                           '_type': '_doc',
                           'sort': mock.ANY}

    assert ids == {str(i) for i in range(10)}


@pytest.mark.run_loop
async def test_scan_equal_chunks_for_loop(es, es_clean, populate):
    for n, scroll_size in [
        (0, 1),  # no results
        (6, 6),  # 1 scroll
        (6, 8),  # 1 scroll
        (6, 3),  # 2 scrolls
        (6, 4),  # 2 scrolls
        (6, 2),  # 3 scrolls
        (6, 1),  # 6 scrolls
    ]:
        es_clean()

        index = 'test_aioes'
        body = {'foo': 1}

        await populate(index, n, body)

        ids = set()

        async with Scan(
            es,
            index=index,
            size=scroll_size,
        ) as scan:

            async for doc in scan:
                ids.add(doc['_id'])

            # check number of unique doc ids
            assert len(ids) == n == scan.total['value']


@pytest.mark.run_loop
async def test_scan_no_mask_index(es):
    index = 'undefined-*'
    scroll_size = 3

    async with Scan(
        es,
        index=index,
        size=scroll_size,
    ) as scan:
        assert scan.scroll_id is None
        assert scan.total['value'] == 0
        cnt = 0
        async for doc in scan:  # noqa
            cnt += 1
        assert cnt == 0


@pytest.mark.run_loop
async def test_scan_no_scroll(es, loop, populate):
    index = 'test_aioes'
    n = 10
    scroll_size = 1
    body = {'foo': 1}

    await populate(index, n, body)

    async with Scan(
        es,
        size=scroll_size,
    ) as scan:
        # same comes after search context expiration
        await scan._do_clear_scroll()

        with pytest.raises(NotFoundError):
            async for doc in scan:
                doc


@pytest.mark.run_loop
async def test_scan_no_index(es):
    index = 'undefined'
    scroll_size = 3

    async with Scan(
        es,
        index=index,
        size=scroll_size,
    ) as scan:
        assert scan.scroll_id is None
        assert scan.total == 0
        cnt = 0
        async for doc in scan:  # noqa
            cnt += 1
        assert cnt == 0


@pytest.mark.run_loop
async def test_scan_warning_on_failed_shards(es, populate, mocker):
    index = 'test_aioes'
    scroll_size = 3
    n = 10

    body = {'foo': 1}
    await populate(index, n, body)

    mocker.spy(logger, 'warning')

    async with Scan(
        es,
        index=index,
        size=scroll_size,
        raise_on_error=False,
    ) as scan:
        i = 0
        async for doc in scan:  # noqa
            if i == 3:
                # once after first scroll
                scan._successful_shards = 4
                scan._total_shards = 5
            i += 1

    logger.warning.assert_called_once_with(
        'Scroll request has only succeeded on %d shards out of %d.', 4, 5)


@pytest.mark.run_loop
async def test_scan_exception_on_failed_shards(es, populate, mocker):
    index = 'test_aioes'
    scroll_size = 3
    n = 10

    body = {'foo': 1}
    await populate(index, n, body)

    mocker.spy(logger, 'warning')

    i = 0
    async with Scan(
        es,
        index=index,
        size=scroll_size,
    ) as scan:
        with pytest.raises(ScanError) as cm:
            async for doc in scan:  # noqa
                if i == 3:
                    # once after first scroll
                    scan._successful_shards = 4
                    scan._total_shards = 5
                i += 1

        assert (str(cm.value) ==
                'Scroll request has only succeeded on 4 shards out of 5.')

    assert i == 6
    logger.warning.assert_called_once_with(
        'Scroll request has only succeeded on %d shards out of %d.', 4, 5)
