import logging

from unittest import mock

import pytest

from aioelasticsearch.helpers import Scan, ScanError


logger = logging.getLogger('elasticsearch')


@pytest.mark.run_loop
async def test_scan_initial_raises(es):
    scan = Scan(es)

    with pytest.raises(RuntimeError):
        async for scroll in scan:  # noqa
            pass

    with pytest.raises(RuntimeError):
        scan.scroll_id

    with pytest.raises(RuntimeError):
        scan.total


@pytest.mark.run_loop
async def test_scan_simple(es, populate):
    index = 'test_aioes'
    doc_type = 'type_2'
    scroll_size = 3
    n = 10

    body = {'foo': 1}
    await populate(es, index, doc_type, n, body)
    ids = set()

    async with Scan(
        es,
        index=index,
        doc_type=doc_type,
        size=scroll_size,
    ) as scan:
        assert scan.scroll_id is not None
        assert scan.total == 10
        async for doc in scan:
            ids.add(doc['_id'])
            assert doc == {'_id': mock.ANY,
                           '_index': 'test_aioes',
                           '_score': None,
                           '_source': {'foo': 1},
                           '_type': 'type_2',
                           'sort': mock.ANY}

    assert ids == set(str(i) for i in range(10))


@pytest.mark.parametrize('n,scroll_size', [
    (0, 1),  # no results
    (6, 6),  # 1 scroll
    (6, 8),  # 1 scroll
    (6, 3),  # 2 scrolls
    (6, 4),  # 2 scrolls
    (6, 2),  # 3 scrolls
    (6, 1),  # 6 scrolls
])
@pytest.mark.run_loop
async def test_scan_equal_chunks_for_loop(es, n, scroll_size, populate):
    index = 'test_aioes'
    doc_type = 'type_1'
    body = {'foo': 1}

    await populate(es, index, doc_type, n, body)

    ids = set()

    async with Scan(
        es,
        index=index,
        doc_type=doc_type,
        size=scroll_size,
    ) as scan:

        async for doc in scan:
                ids.add(doc['_id'])

        # check number of unique doc ids
        assert len(ids) == n == scan.total


@pytest.mark.run_loop
async def test_scan_no_mask_index(es):
    index = 'undefined-*'
    doc_type = 'any'
    scroll_size = 3

    async with Scan(
        es,
        index=index,
        doc_type=doc_type,
        size=scroll_size,
    ) as scan:
        assert scan.scroll_id is not None
        assert scan.total == 0
        cnt = 0
        async for doc in scan:  # noqa
            cnt += 1
        assert cnt == 0


@pytest.mark.run_loop
async def test_scan_no_index(es):
    index = 'undefined'
    doc_type = 'any'
    scroll_size = 3

    async with Scan(
        es,
        index=index,
        doc_type=doc_type,
        size=scroll_size,
    ) as scan:
        assert scan.scroll_id is not None
        assert scan.total == 0
        cnt = 0
        async for doc in scan:  # noqa
            cnt += 1
        assert cnt == 0


@pytest.mark.run_loop
async def test_scan_iter_without_context_manager(es):
    index = 'undefined'
    doc_type = 'any'
    scroll_size = 3

    scan = Scan(
        es,
        index=index,
        doc_type=doc_type,
        size=scroll_size,
    )
    with pytest.raises(RuntimeError):
        async for doc in scan:
            doc


@pytest.mark.run_loop
async def test_scan_warning_on_failed_shards(es, populate, mocker):
    index = 'test_aioes'
    doc_type = 'type_2'
    scroll_size = 3
    n = 10

    body = {'foo': 1}
    await populate(es, index, doc_type, n, body)

    mocker.spy(logger, 'warning')

    async with Scan(
        es,
        index=index,
        doc_type=doc_type,
        size=scroll_size,
        raise_on_error=False,
    ) as scan:
        i = 0
        async for doc in scan:  # noqa
            if i == 3:
                # once after first scroll
                scan._failed_shards = 1
                scan._totl_shards = 2
            i += 1

    logger.warning.assert_called_once_with(
        'Scroll request has failed on %d shards out of %d.', 1, 5)


@pytest.mark.run_loop
async def test_scan_exception_on_failed_shards(es, populate, mocker):
    index = 'test_aioes'
    doc_type = 'type_2'
    scroll_size = 3
    n = 10

    body = {'foo': 1}
    await populate(es, index, doc_type, n, body)

    mocker.spy(logger, 'warning')

    i = 0
    async with Scan(
        es,
        index=index,
        doc_type=doc_type,
        size=scroll_size,
    ) as scan:
        with pytest.raises(ScanError) as cm:
            async for doc in scan:  # noqa
                if i == 3:
                    # once after first scroll
                    scan._failed_shards = 1
                    scan._totl_shards = 2
                i += 1

        assert (str(cm.value) ==
                'Scroll request has failed on 1 shards out of 5.')

    assert i == 6
    logger.warning.assert_called_once_with(
        'Scroll request has failed on %d shards out of %d.', 1, 5)
