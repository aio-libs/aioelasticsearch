import asyncio

import aiohttp
from aiohttp.errors import ClientError, FingerprintMismatch
from elasticsearch.connection import Connection
from elasticsearch.exceptions import (ConnectionError, ConnectionTimeout,
                                      SSLError)
from yarl import URL


class AIOHttpConnection(Connection):

    def __init__(
        self,
        host='localhost',
        port=9200,
        http_auth=None,
        use_ssl=False,
        verify_certs=False,
        maxsize=10,
        headers=None,
        *, loop,
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
            's' if use_ssl else '',
            host, port, self.url_prefix,
        ))

        self.session = kwargs.get('session')
        if self.session is None:
            self.session = aiohttp.ClientSession(
                auth=self.http_auth,
                connector=aiohttp.TCPConnector(
                    limit=maxsize,
                    use_dns_cache=kwargs.get('use_dns_cache', False),
                    verify_ssl=self.verify_certs,
                    conn_timeout=None,
                    loop=self.loop,
                ),
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

        except asyncio.TimeoutError as exc:
            self.log_request_fail(method, url, url_path, body, self.loop.time() - start, exception=exc)  # noqa
            raise ConnectionTimeout('TIMEOUT', str(exc), exc)

        except FingerprintMismatch as exc:
            self.log_request_fail(method, url, url_path, body, self.loop.time() - start, exception=exc)  # noqa
            raise SSLError('N/A', str(exc), exc)

        except ClientError as exc:
            self.log_request_fail(method, url, url_path, body, self.loop.time() - start, exception=exc)  # noqa
            raise ConnectionError('N/A', str(exc), exc)

        finally:
            if response is not None:
                yield from response.release()

        # raise errors based on http status codes, let the client handle those if needed  # noqa
        if not (200 <= response.status < 300) and response.status not in ignore:  # noqa
            self.log_request_fail(method, url, url_path, body, duration, response.status, raw_data)  # noqa
            self._raise_error(response.status, raw_data)

        self.log_request_success(method, url, url_path, body, response.status, raw_data, duration)  # noqa

        return response.status, response.headers, raw_data
