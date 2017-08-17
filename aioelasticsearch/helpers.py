import asyncio

from aioelasticsearch import NotFoundError
from elasticsearch.helpers import ScanError

from .compat import PY_352


class Scan:

    def __init__(
        self,
        es,
        query=None,
        scroll='5m',
        raise_on_error=True,
        preserve_order=False,
        size=1000,
        clear_scroll=True,
        **kwargs
    ):
        self._es = es

        if not preserve_order:
            query = query.copy() if query else {}
            query['sort'] = '_doc'
        self._query = query
        self._scroll = scroll
        self._raise_on_error = raise_on_error
        self._size = size
        self._clear_scroll = clear_scroll
        self._kwargs = kwargs

        self._scroll_id = None

        self._total = 0

        self._initial = True
        self._done = False
        self._hits = []
        self._hits_idx = 0

    async def __aenter__(self):  # noqa
        await self._do_search()
        return self

    async def __aexit__(self, *exc_info):  # noqa
        await self._do_clear_scroll()

    def __aiter__(self):
        return self

    if not PY_352:
        __aiter__ = asyncio.coroutine(__aiter__)

    async def __anext__(self):  # noqa
        assert not self._initial

        if self._done:
            raise StopAsyncIteration

        if self._hits_idx >= len(self._hits):
            await self._do_scroll()
        ret = self._hits[self._hits_idx]
        self._hits_idx += 1
        return ret

    @property
    def scroll_id(self):
        assert not self._initial
        return self._total

    @property
    def total(self):
        assert not self._initial
        return self._total

    async def _do_search(self):
        assert self._initial
        self._initial = False

        try:
            resp = await self._es.search(
                body=self._query,
                scroll=self._scroll,
                size=self._size,
                **self._kwargs
            )
        except NotFoundError:
            self._done = True
            return
        else:
            self._hits = resp['hits']['hits']
            self._hits_idx = 0
            self._scroll_id = resp.get('_scroll_id')
            self._total = resp['hits']['total']
            self._done = not self._hits or self._scroll_id is None

    async def _do_scroll(self):
        assert not self._initial

        try:
            resp = await self._es.scroll(
                self._scroll_id,
                scroll=self._scroll,
            )
        except NotFoundError:  # pragma: no cover
            # Don't know how to make test case for it
            # but if search could return 404 on not exsiting index
            # scroll maybe can do it too
            self._done = True
            raise StopAsyncIteration
        else:
            self._hits = resp['hits']['hits']
            self._hits_idx = 0
            self._scroll_id = resp.get('_scroll_id')
            self._done = not self._hits or self._scroll_id is None
            if self._done:
                raise StopAsyncIteration

    async def _do_clear_scroll(self):
        if self._scroll_id is not None and self._clear_scroll:
            await self._es.clear_scroll(
                body={'scroll_id': [self._scroll_id]},
                ignore=404,
            )
