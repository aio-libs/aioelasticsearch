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

    async def perform_request(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        if self.exception:
            raise self.exception
        return self.status, self.headers, self.data


@pytest.mark.run_loop
async def test_body_surrogates_replaced_encoded_into_bytes(loop):
    t = AIOHttpTransport([{}], connection_class=DummyConnection, loop=loop)

    await t.perform_request('GET', '/', body='你好\uda6a')
    conn = await t.get_connection()

    assert len(conn.calls) == 1
    assert ('GET', '/', None,
            b'\xe4\xbd\xa0\xe5\xa5\xbd\xed\xa9\xaa') == conn.calls[0][0]

    await t.close()


@pytest.mark.run_loop
async def test_custom_serializers(loop):
    serializer = object()
    t = AIOHttpTransport([{}], serializers={'test': serializer}, loop=loop)
    try:
        assert 'test' in t.deserializer.serializers
        assert t.deserializer.serializers['test'] is serializer
    finally:
        await t.close()


@pytest.mark.run_loop
async def test_sniff_on_startup(loop):
    t = AIOHttpTransport([{}], loop=loop)
    try:
        assert 'test' in t.deserializer.serializers
        assert t.deserializer.serializers['test'] is serializer
    finally:
        await t.close()
