import asyncio
import ssl

import aiohttp

from .compat import AIOHTTP_2  # isort:skip

if AIOHTTP_2:
    from aiohttp import ClientError
else:
    from aiohttp.errors import ClientError

from elasticsearch.connection import Connection  # noqa # isort:skip
from elasticsearch.exceptions import (ConnectionError, ConnectionTimeout,  # noqa # isort:skip
                                      SSLError)
from yarl import URL  # noqa # isort:skip


class AIOHttpConnection(Connection):

    def __init__(
        self,
        host='localhost',
        port=9200,
        http_auth=None,
        use_ssl=False,
        ssl_context=None,
        verify_certs=False,
        maxsize=10,
        headers=None,
        *,
        loop,
        **kwargs
    ):
        super().__init__(host=host, port=port, **kwargs)

        if headers is None:
            headers = {}
        self.headers = headers
        self.headers.setdefault('Content-Type', 'application/json')

        self.loop = loop

        if http_auth is not None:
            if isinstance(http_auth, str):
                http_auth = tuple(http_auth.split(':', 1))

            if isinstance(http_auth, (tuple, list)):
                http_auth = aiohttp.BasicAuth(*http_auth)

        self.http_auth = http_auth

        self.verify_certs = verify_certs

        self.base_url = URL('http%s://%s:%d%s/' % (
            's' if use_ssl else '', host, port, self.url_prefix,
        ))

        self.session = kwargs.get('session')
        if self.session is None:
            connector_kwargs = {
                'limit': maxsize,
                'use_dns_cache': kwargs.get('use_dns_cache', False),
                'ssl_context': ssl_context,
                'verify_ssl': self.verify_certs,
                'loop': self.loop,
            }
            session_kwargs = {'auth': self.http_auth}

            if AIOHTTP_2:
                session_kwargs['conn_timeout'] = None
            else:
                connector_kwargs['conn_timeout'] = None

            self.session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(**connector_kwargs),
                **session_kwargs
            )

    def close(self):
        return self.session.close()

    @asyncio.coroutine
    def perform_request(self, method, url, params=None, body=None, timeout=None, ignore=()):  # noqa
        url_path = url

        url = (self.base_url / url.lstrip('/')).with_query(params)

        start = self.loop.time()
        response = None
        try:
            with aiohttp.Timeout(timeout or self.timeout, loop=self.loop):  # noqa
                response = yield from self.session.request(method, url, data=body, headers=self.headers, timeout=None)  # noqa
                raw_data = yield from response.text()

            duration = self.loop.time() - start

        except ssl.CertificateError as exc:
            self.log_request_fail(method, url, url_path, body, self.loop.time() - start, exception=exc)  # noqa
            raise SSLError('N/A', str(exc), exc)

        except asyncio.TimeoutError as exc:
            self.log_request_fail(method, url, url_path, body, self.loop.time() - start, exception=exc)  # noqa
            raise ConnectionTimeout('TIMEOUT', str(exc), exc)

        except ClientError as exc:
            self.log_request_fail(method, url, url_path, body, self.loop.time() - start, exception=exc)  # noqa
            _exc = str(exc)
            # aiohttp wraps ssl error
            if 'SSL: CERTIFICATE_VERIFY_FAILED' in _exc:
                raise SSLError('N/A', _exc, exc)

            raise ConnectionError('N/A', _exc, exc)

        finally:
            if response is not None:
                yield from response.release()

        # raise errors based on http status codes, let the client handle those if needed  # noqa
        if not (200 <= response.status < 300) and response.status not in ignore:  # noqa
            self.log_request_fail(method, url, url_path, body, duration, response.status, raw_data)  # noqa
            self._raise_error(response.status, raw_data)

        self.log_request_success(method, url, url_path, body, response.status, raw_data, duration)  # noqa

        return response.status, response.headers, raw_data
