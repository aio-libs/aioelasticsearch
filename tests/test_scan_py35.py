import pytest

from aioelasticsearch.helpers import Scan
from tests.utils import populate


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
async def test_scan_equal_chunks_for_loop(loop, es, n, scroll_size):  # noqa
    index = 'test_aioes'
    doc_type = 'type_1'
    await populate(es, index, doc_type, n, loop=loop)

    ids = set()
    data = []
    async with Scan(
        es,
        index=index,
        doc_type=doc_type,
        size=scroll_size,
        loop=loop,
    ) as scan:

        async for scroll in scan:
            if scroll:
                data.append(scroll)

                for doc in scroll:
                    ids.add(doc['_id'])

        # check number of unique doc ids
        assert len(ids) == n == scan.total

    # check number of docs in a scroll
    expected_scroll_sizes = [scroll_size] * (n // scroll_size)
    if n % scroll_size != 0:
        expected_scroll_sizes.append(n % scroll_size)

    scroll_sizes = [len(scroll) for scroll in data]
    assert scroll_sizes == expected_scroll_sizes
