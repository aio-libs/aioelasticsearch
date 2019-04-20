import asyncio
from unittest import mock

import aiohttp
import pytest
from aiohttp.test_utils import make_mocked_coro
from elasticsearch import ConnectionTimeout

from aioelasticsearch.connection import (
    AIOHttpConnection, ConnectionError, SSLError,
)


@pytest.mark.run_loop
async def test_default_headers(auto_close, loop):
    conn = auto_close(AIOHttpConnection(loop=loop))
    assert conn.headers == {'Content-Type': 'application/json'}


@pytest.mark.run_loop
async def test_custom_headers(auto_close, loop):
    conn = auto_close(AIOHttpConnection(headers={'X-Custom': 'value'},
                                        loop=loop))
    assert conn.headers == {'Content-Type': 'application/json',
                            'X-Custom': 'value'}


@pytest.mark.run_loop
async def test_auth_no_auth(auto_close, loop):
    conn = auto_close(AIOHttpConnection(loop=loop))
    assert conn.http_auth is None


@pytest.mark.run_loop
async def test_auth_str(auto_close, loop):
    auth = aiohttp.BasicAuth('user', 'pass')
    conn = auto_close(AIOHttpConnection(http_auth='user:pass', loop=loop))
    assert conn.http_auth == auth


@pytest.mark.run_loop
async def test_auth_tuple(auto_close, loop):
    auth = aiohttp.BasicAuth('user', 'pass')
    conn = auto_close(AIOHttpConnection(http_auth=('user', 'pass'), loop=loop))
    assert conn.http_auth == auth


@pytest.mark.run_loop
async def test_auth_basicauth(auto_close, loop):
    auth = aiohttp.BasicAuth('user', 'pass')
    conn = auto_close(AIOHttpConnection(http_auth=auth, loop=loop))
    assert conn.http_auth == auth


@pytest.mark.run_loop
async def test_auth_invalid(loop):
    with pytest.raises(TypeError):
        AIOHttpConnection(http_auth=object(), loop=loop)


@pytest.mark.run_loop
async def test_explicit_session(auto_close, loop):
    session = aiohttp.ClientSession(loop=loop)
    conn = auto_close(AIOHttpConnection(session=session, loop=loop))
    assert conn.session is session


@pytest.mark.run_loop
async def test_perform_request_ssl_error(auto_close, loop):
    for exc, expected in [
        (aiohttp.ClientConnectorCertificateError(mock.Mock(), mock.Mock()), SSLError),  # noqa
        (aiohttp.ClientConnectorSSLError(mock.Mock(), mock.Mock()), SSLError),
        (aiohttp.ClientSSLError(mock.Mock(), mock.Mock()), SSLError),
        (aiohttp.ClientError('Other'), ConnectionError),
        (asyncio.TimeoutError, ConnectionTimeout),
    ]:
        session = aiohttp.ClientSession(loop=loop)

        async def coro(*args, **Kwargs):
            raise exc

        session._request = coro

        conn = auto_close(AIOHttpConnection(session=session, loop=loop,
                                            use_ssl=True))
        with pytest.raises(expected):
            await conn.perform_request('HEAD', '/')


@pytest.mark.run_loop
async def test_priority_queue_same_timestamp(auto_close, loop):
    conn = auto_close(AIOHttpConnection(http_auth='user:pass', loop=loop))
    conn2 = auto_close(AIOHttpConnection(http_auth='user:pass', loop=loop))
    queue = asyncio.PriorityQueue(10, loop=loop)
    time = loop.time()

    # not raising error if both AIOHttpConnectionPool
    queue.put_nowait(
        (time, conn)
    )
    queue.put_nowait(
        (time, conn2)
    )

    with pytest.raises(TypeError):
        queue.put_nowait((time, object()))
