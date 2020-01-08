import asyncio
import ssl
from unittest import mock

import aiohttp
import pytest
from elasticsearch import ConnectionTimeout

from aioelasticsearch.connection import (AIOHttpConnection, ConnectionError,
                                         SSLError)


async def test_default_headers(auto_close):
    conn = auto_close(AIOHttpConnection())
    assert conn.headers == {'Content-Type': 'application/json'}


async def test_custom_headers(auto_close):
    conn = auto_close(AIOHttpConnection(headers={'X-Custom': 'value'}))
    assert conn.headers == {'Content-Type': 'application/json',
                            'X-Custom': 'value'}


async def test_auth_no_auth(auto_close):
    conn = auto_close(AIOHttpConnection())
    assert conn.http_auth is None


async def test_ssl_context(auto_close):
    context = ssl.create_default_context()
    conn = auto_close(
        AIOHttpConnection(verify_certs=True, ssl_context=context)
    )
    assert conn.session.connector._ssl is context


async def test_auth_str(auto_close):
    auth = aiohttp.BasicAuth('user', 'pass')
    conn = auto_close(AIOHttpConnection(http_auth='user:pass'))
    assert conn.http_auth == auth


async def test_auth_tuple(auto_close):
    auth = aiohttp.BasicAuth('user', 'pass')
    conn = auto_close(AIOHttpConnection(http_auth=('user', 'pass')))
    assert conn.http_auth == auth


async def test_auth_basicauth(auto_close):
    auth = aiohttp.BasicAuth('user', 'pass')
    conn = auto_close(AIOHttpConnection(http_auth=auth))
    assert conn.http_auth == auth


async def test_auth_invalid():
    with pytest.raises(TypeError):
        AIOHttpConnection(http_auth=object())


async def test_explicit_session(auto_close):
    async with aiohttp.ClientSession() as session:
        conn = auto_close(AIOHttpConnection(session=session))
        assert conn.session is session


async def test_explicit_session_not_closed():
    async with aiohttp.ClientSession() as session:
        conn = AIOHttpConnection(session=session)
        await conn.close()
        assert not conn.session.closed and not session.closed


async def test_session_closed():
    conn = AIOHttpConnection()
    await conn.close()
    assert conn.session.closed


async def test_perform_request_ssl_error(auto_close):
    for exc, expected in [
        (aiohttp.ClientConnectorCertificateError(mock.Mock(), mock.Mock()), SSLError),  # noqa
        (aiohttp.ClientConnectorSSLError(mock.Mock(), mock.Mock()), SSLError),
        (aiohttp.ClientSSLError(mock.Mock(), mock.Mock()), SSLError),
        (aiohttp.ClientError('Other'), ConnectionError),
        (asyncio.TimeoutError, ConnectionTimeout),
    ]:
        async with aiohttp.ClientSession() as session:

            async def coro(*args, **Kwargs):
                raise exc

            session._request = coro

            conn = auto_close(AIOHttpConnection(session=session, use_ssl=True))
            with pytest.raises(expected):
                await conn.perform_request('HEAD', '/')
