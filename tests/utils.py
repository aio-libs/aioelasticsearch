import asyncio


@asyncio.coroutine
def populate(es, index, doc_type, n, body, *, loop):
    coros = []

    yield from es.indices.create(index)

    for i in range(n):
        coros.append(
            es.index(
                index=index,
                doc_type=doc_type,
                id=str(i),
                body=body,
                refresh=True,
            ),
        )

    yield from asyncio.gather(*coros, loop=loop)
