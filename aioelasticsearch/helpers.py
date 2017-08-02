import asyncio

from .compat import PY_352


class Scan:

    def __init__(
        self,
        es,
        query=None,
        scroll='5m',
        size=1000,
        preserve_order=False,
        clear_scroll=True,
        **kwargs
    ):
        self._es = es

        if not preserve_order:
            query = query.copy() if query else {}
            query['sort'] = '_doc'
        self._query = query
        self._scroll = scroll
        self._size = size
        self._clear_scroll = clear_scroll
        self._kwargs = kwargs

        self._scroll_id = None

        self._total = 0

        self.__initial = True
        self.__has_more = None
        self.__found = 0
        self.__scroll_hits = None
        self.__scroll_hits_found = False

    async def __aenter__(self):  # noqa
        await self.scroll()

        return self

    async def __aexit__(self, *exc_info):  # noqa
        await self.clear_scroll()

    def __aiter__(self):
        return self

    if not PY_352:
        __aiter__ = asyncio.coroutine(__aiter__)

    async def __anext__(self):  # noqa
        assert not self.__initial

        if self.__scroll_hits is not None:
            hits = self.__scroll_hits

            self.__scroll_hits = None
        else:
            hits = await self.search()

        if not hits:
            raise StopAsyncIteration

        return hits

    @property
    def scroll_id(self):
        assert not self.__initial

        return self._scroll_id

    @property
    def total(self):
        assert not self.__initial

        return self._total

    @property
    def has_more(self):
        assert not self.__initial

        if self._scroll_id is None:
            return False

        if self.__has_more is False:
            return False

        return True

    async def scroll(self):
        assert self.__initial

        resp = await self._es.search(
            body=self._query,
            scroll=self._scroll,
            size=self._size,
            **self._kwargs
        )

        self.__initial = False

        hits = resp['hits']['hits']

        self._scroll_id = resp.get('_scroll_id')

        self._total = resp['hits']['total']

        self.__found += len(hits)

        self.__has_more = self.__found < self._total

        self.__scroll_hits = hits

        self.__scroll_hits_found = bool(self.__scroll_hits)

        return hits

    async def search(self):
        assert not self.__initial

        resp = await self._es.scroll(
            self._scroll_id,
            scroll=self._scroll,
        )

        hits = resp['hits']['hits']

        self._scroll_id = resp.get('_scroll_id')

        self.__found += len(hits)

        self.__has_more = self.__found < self._total

        return hits

    async def clear_scroll(self):
        if self._scroll_id is not None and self._clear_scroll:
            await self._es.clear_scroll(
                body={'scroll_id': [self._scroll_id]},
            )
