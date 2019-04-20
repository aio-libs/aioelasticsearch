import asyncio
import ssl
from unittest import mock

import aiohttp
import pytest
from elasticsearch import ConnectionTimeout

from aioelasticsearch.connection import (AIOHttpConnection, ConnectionError,
                                         SSLError)


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
async def test_ssl_context(auto_close, loop):
    context = ssl.create_default_context()
    conn = auto_close(
        AIOHttpConnection(loop=loop, verify_certs=True, ssl_context=context)
    )
    assert conn.session.connector._ssl is context


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
async def test_http_compression(auto_close, loop):
    session = aiohttp.ClientSession(loop=loop, auto_decompress=False)
    conn = AIOHttpConnection(session=session, loop=loop)
    with pytest.raises(UnicodeDecodeError) as excinfo:
        await conn.perform_request("GET", "index")
    assert str(excinfo.value).startswith("'utf-8' codec can't decode byte")
    await session.close()


@pytest.mark.run_loop
async def test_http_compression_headers(auto_close, loop):
        conn = auto_close(AIOHttpConnection(loop=loop, http_compress=True))
        assert conn.headers['Content-Encoding'] == 'gzip'
