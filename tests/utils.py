import asyncio


@asyncio.coroutine
def populate(es, index, doc_type, n, *, loop):  # TODO: check for * python version
    url_pattern = '/{index}/{doc_type}/{_id}'

    coros = []

    for i in range(n):
        url = url_pattern.format(
            index=index,
            doc_type=doc_type,
            _id=i,
        )
        body = {
            'foo': i,
            'bar': i,
        }
        coros.append(
            es.transport.perform_request('PUT', url=url, body=body),
        )

    yield from asyncio.gather(*coros, loop=loop)

    yield from es.transport.perform_request(
        'POST',
        '/test_aioes/_refresh'
    )
