import asyncio

import pytest
from aioelasticsearch import AIOHttpTransport
from aioelasticsearch.connection import AIOHttpConnection


class DummyConnection(AIOHttpConnection):
    def __init__(self, **kwargs):
        self.exception = kwargs.pop('exception', None)
        self.status = kwargs.pop('status', 200)
        self.data = kwargs.pop('data', '{}')
        self.headers = kwargs.pop('headers', {})
        self.calls = []
        super().__init__(**kwargs)

    @asyncio.coroutine
    def perform_request(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        if self.exception:
            raise self.exception
        return self.status, self.headers, self.data


@pytest.mark.run_loop
@asyncio.coroutine
def test_body_surrogates_replaced_encoded_into_bytes(loop):
    t = AIOHttpTransport([], connection_class=DummyConnection, loop=loop)

    yield from t.perform_request('GET', '/', body='你好\uda6a')
    conn = yield from t.get_connection()

    assert len(conn.calls) == 1
    assert ('GET', '/', None, b'\xe4\xbd\xa0\xe5\xa5\xbd\xed\xa9\xaa') == conn.calls[0][0]  # noqa

    yield from t.close()
