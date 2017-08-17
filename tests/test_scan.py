from unittest import mock


import pytest

from aioelasticsearch.helpers import Scan


@pytest.mark.run_loop
async def test_scan_initial_raises(es):
    scan = Scan(es)

    with pytest.raises(AssertionError):
        async for scroll in scan:  # noqa
            pass

    with pytest.raises(AssertionError):
        scan.scroll_id

    with pytest.raises(AssertionError):
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
    # data = []

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

    # check number of docs in a scroll
    expected_scroll_sizes = [scroll_size] * (n // scroll_size)
    if n % scroll_size != 0:
        expected_scroll_sizes.append(n % scroll_size)

    # scroll_sizes = [len(scroll) for scroll in data]
    # assert scroll_sizes == expected_scroll_sizes


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
