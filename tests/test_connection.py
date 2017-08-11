import asyncio
import ssl


import aiohttp
import pytest


from aioelasticsearch.connection import (AIOHttpConnection,
                                         ConnectionError, SSLError)


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
async def test_perform_request_bad_cert(auto_close, loop):
    session = aiohttp.ClientSession(loop=loop)

    @asyncio.coroutine
    def request(*args, **kwargs):
        raise ssl.CertificateError()
    session._request = request

    conn = auto_close(AIOHttpConnection(session=session, loop=loop,
                                        use_ssl=True))
    with pytest.raises(SSLError):
        await conn.perform_request('HEAD', '/')


@pytest.mark.run_loop
async def test_perform_request_bad_cert2(auto_close, loop):
    session = aiohttp.ClientSession(loop=loop)

    @asyncio.coroutine
    def request(*args, **kwargs):
        raise aiohttp.ClientError('SSL: CERTIFICATE_VERIFY_FAILED')
    session._request = request

    conn = auto_close(AIOHttpConnection(session=session, loop=loop,
                                        use_ssl=True))
    with pytest.raises(SSLError):
        await conn.perform_request('HEAD', '/')


@pytest.mark.run_loop
async def test_perform_connection_error(auto_close, loop):
    session = aiohttp.ClientSession(loop=loop)

    @asyncio.coroutine
    def request(*args, **kwargs):
        raise aiohttp.ClientError('Other')
    session._request = request

    conn = auto_close(AIOHttpConnection(session=session, loop=loop,
                                        use_ssl=True))
    with pytest.raises(ConnectionError):
        await conn.perform_request('HEAD', '/')
