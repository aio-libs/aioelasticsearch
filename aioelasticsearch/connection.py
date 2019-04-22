import asyncio

import aiohttp

from .exceptions import ConnectionError, ConnectionTimeout, SSLError  # noqa # isort:skip

from elasticsearch.connection import Connection  # noqa # isort:skip
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
        super().__init__(host=host, port=port, use_ssl=use_ssl, **kwargs)

        if headers is None:
            headers = {}
        self.headers = headers
        self.headers.setdefault('Content-Type', 'application/json')

        self.loop = loop

        if http_auth is not None:
            if isinstance(http_auth, aiohttp.BasicAuth):
                pass
            elif isinstance(http_auth, str):
                http_auth = aiohttp.BasicAuth(*http_auth.split(':', 1))
            elif isinstance(http_auth, (tuple, list)):
                http_auth = aiohttp.BasicAuth(*http_auth)
            else:
                raise TypeError("Expected str, list, tuple or "
                                "aiohttp.BasicAuth as http_auth parameter,"
                                "got {!r}".format(http_auth))

        self.http_auth = http_auth

        self.verify_certs = verify_certs

        self.base_url = URL.build(scheme='https' if self.use_ssl else 'http',
                                  host=host,
                                  port=port,
                                  path=self.url_prefix)

        self.session = kwargs.get('session')
        self.close_session = False
        if self.session is None:
            kwargs = {}
            if not self.verify_certs:
                kwargs['ssl'] = False
            else:
                kwargs['ssl'] = ssl_context
            self.session = aiohttp.ClientSession(
                auth=self.http_auth,
                connector=aiohttp.TCPConnector(
                    limit=maxsize,
                    use_dns_cache=kwargs.get('use_dns_cache', False),
                    loop=self.loop,
                    **kwargs,
                ),
            )
            self.close_session = True

    async def close(self):
        if self.close_session:
            await self.session.close()

    async def perform_request(
        self,
        method,
        url,
        params=None,
        body=None,
        headers=None,
        timeout=None,
        ignore=()
    ):
        url_path = url

        url = (self.base_url / url.lstrip('/')).with_query(params)

        start = self.loop.time()
        try:
            async with self.session.request(
                    method,
                    url,
                    data=body,
                    headers=self._build_headers(headers),
                    timeout=timeout or self.timeout) as response:
                raw_data = await response.text()

                duration = self.loop.time() - start

        except aiohttp.ClientSSLError as exc:
            self.log_request_fail(
                method,
                url,
                url_path,
                body,
                self.loop.time() - start,
                exception=exc,
            )
            raise SSLError('N/A', str(exc), exc)

        except asyncio.TimeoutError as exc:
            self.log_request_fail(
                method,
                url,
                url_path,
                body,
                self.loop.time() - start,
                exception=exc,
            )
            raise ConnectionTimeout('TIMEOUT', str(exc), exc)

        except aiohttp.ClientError as exc:
            self.log_request_fail(
                method,
                url,
                url_path,
                body,
                self.loop.time() - start,
                exception=exc,
            )

            raise ConnectionError('N/A', str(exc), exc)

        # raise errors based on http status codes
        # let the client handle those if needed
        if (
            not (200 <= response.status < 300) and
            response.status not in ignore
        ):
            self.log_request_fail(
                method,
                url,
                url_path,
                body,
                duration,
                response.status,
                raw_data,
            )
            self._raise_error(response.status, raw_data)

        self.log_request_success(
            method,
            url,
            url_path,
            body,
            response.status,
            raw_data,
            duration,
        )

        return response.status, response.headers, raw_data

    def _build_headers(self, headers):
        if headers:
            final_headers = self.headers.copy()
            final_headers.update(headers)
        else:
            final_headers = self.headers
        return final_headers
