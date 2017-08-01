import pytest
from aioelasticsearch.exceptions import NotFoundError
from aioelasticsearch.helpers import Scan


@pytest.mark.run_loop
async def test_scan_initial_raises(loop, es):
    scan = Scan(es, loop=loop)

    with pytest.raises(AssertionError):
        scan.scroll_id

    with pytest.raises(AssertionError):
        scan.total

    with pytest.raises(AssertionError):
        scan.has_more

    with pytest.raises(AssertionError):
        next(scan)

    with pytest.raises(AssertionError):
        await scan.search()


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
async def test_scan_equal_chunks_for_loop(loop, es, n, scroll_size, populate):
    index = 'test_aioes'
    doc_type = 'type_1'
    body = {'foo': 1}

    await populate(es, index, doc_type, n, body)

    ids = set()
    data = []

    with Scan(
        es,
        index=index,
        doc_type=doc_type,
        size=scroll_size,
        loop=loop,
    ) as scan:
        try:
            await scan.scroll()

            for scroll in scan:
                scroll = await scroll

                data.append(scroll)

                for doc in scroll:
                    ids.add(doc['_id'])

            # check number of unique doc ids
            assert len(ids) == n == scan.total
        finally:
            await scan.clear_scroll()

    # check number of docs in a scroll
    expected_scroll_sizes = [scroll_size] * (n // scroll_size)
    if n % scroll_size != 0:
        expected_scroll_sizes.append(n % scroll_size)

    scroll_sizes = [len(scroll) for scroll in data]
    assert scroll_sizes == expected_scroll_sizes


@pytest.mark.run_loop
async def test_scan_has_more(loop, es, populate):
    index = 'test_aioes'
    doc_type = 'type_1'
    n = 10
    scroll_size = 3
    body = {'foo': 1}

    await populate(es, index, doc_type, n, body)

    with Scan(
        es,
        index=index,
        doc_type=doc_type,
        size=scroll_size,
        loop=loop,
    ) as scan:
        try:
            await scan.scroll()

            assert scan.has_more

            for scroll in scan:
                await scroll

            with pytest.raises(StopIteration):
                next(scan)

            assert not scan.has_more

        finally:
            await scan.clear_scroll()


@pytest.mark.run_loop
async def test_scan_no_mask_index(loop, es):
    index = 'undefined-*'
    doc_type = 'any'
    scroll_size = 3

    with Scan(
        es,
        index=index,
        doc_type=doc_type,
        size=scroll_size,
        loop=loop,
    ) as scan:
        try:
            await scan.scroll()

            assert scan.scroll_id is None
            assert not scan.has_more
            assert scan.total == 0
        finally:
            await scan.clear_scroll()


@pytest.mark.run_loop
async def test_scan_no_index(loop, es):
    index = 'undefined'
    doc_type = 'any'
    scroll_size = 3

    with pytest.raises(NotFoundError):
        with Scan(
            es,
            index=index,
            doc_type=doc_type,
            size=scroll_size,
            loop=loop,
        ) as scan:
            try:
                await scan.scroll()
            finally:
                await scan.clear_scroll()


@pytest.mark.run_loop
async def test_scan_clear_scroll(loop, es, populate):
    index = 'test_aioes'
    doc_type = 'type_1'
    n = 10
    scroll_size = 3
    body = {'foo': 1}

    await populate(es, index, doc_type, n, body)

    with Scan(
        es,
        index=index,
        doc_type=doc_type,
        size=scroll_size,
        loop=loop,
    ) as scan:
        await scan.scroll()

        await scan.clear_scroll()

        with pytest.raises(NotFoundError):
            for scroll in scan:
                await scroll
