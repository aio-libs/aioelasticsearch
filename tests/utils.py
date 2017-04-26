import asyncio


@asyncio.coroutine
def populate(es, index, doc_type, n, *, loop):
    coros = []

    for i in range(n):
        body = {
            'foo': i,
            'bar': i,
        }
        coros.append(
            es.index(
                index=index,
                doc_type=doc_type,
                id=i,
                body=body,
                refresh=True,
            ),
        )

    yield from asyncio.gather(*coros, loop=loop)
