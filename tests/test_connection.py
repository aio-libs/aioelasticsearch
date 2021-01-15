import asyncio
import ssl
from unittest import mock
from urllib.parse import unquote

import aiohttp
import pytest
from elasticsearch import ConnectionTimeout
from elasticsearch.client.utils import _make_path

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
async def test_explicit_session_not_closed(loop):
    session = aiohttp.ClientSession(loop=loop)
    conn = AIOHttpConnection(session=session, loop=loop)
    await conn.close()
    assert not conn.session.closed and not session.closed


@pytest.mark.run_loop
async def test_default_session(auto_close, loop):
    conn = auto_close(AIOHttpConnection(loop=loop))
    assert isinstance(conn.session, aiohttp.ClientSession)


@pytest.mark.run_loop
async def test_session_closed(loop):
    conn = AIOHttpConnection(loop=loop)
    await conn.close()
    assert conn.session.closed


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
async def test_path_encoding(loop):
    class StopProcessing(Exception):
        pass

    class DummyClientSession(object):
        def request(self, method, url, **kwargs):
            raise StopProcessing(unquote(url.path))

    conn = AIOHttpConnection(session=DummyClientSession(), loop=loop)

    for id in ("123abc", "123+abc", "123%4"):
        path = _make_path("index", "_doc", id)

        with pytest.raises(StopProcessing) as sp:
            await conn.perform_request("GET", path)

        assert id == str(sp.value).split("/")[-1]
