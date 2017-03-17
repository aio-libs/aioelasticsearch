import asyncio

from .compat import PY_350, PY_352


if not PY_350:
    StopAsyncIteration = None  # noqa


def create_future(*, loop=None):
    if loop is None:
        loop = asyncio.get_event_loop()

    try:
        return loop.create_future()
    except AttributeError:
        return asyncio.Future(loop=loop)


class Scan:

    def __init__(
        self,
        es,
        query=None,
        scroll='5m',
        size=1000,
        preserve_order=False,
        clear_scroll=True,
        *, loop=None,
        **kwargs
    ):
        self._loop = loop

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

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        pass

    def __iter__(self):  # noqa
        return self

    def __next__(self):  # noqa
        assert not self.__initial

        if self.__scroll_hits is not None:
            fut = create_future(loop=self._loop)
            fut.set_result(self.__scroll_hits)

            self.__scroll_hits = None

            return fut

        if not self.has_more:
            raise StopIteration

        return self.search()

    if PY_350:
        @asyncio.coroutine
        def __aenter__(self):  # noqa
            yield from self.scroll()

            return self

        @asyncio.coroutine
        def __aexit__(self, *exc_info):  # noqa
            yield from self.clear_scroll()

        __aiter__ = __iter__

        if not PY_352:
            __aiter__ = asyncio.coroutine(__aiter__)

        @asyncio.coroutine
        def __anext__(self):  # noqa
            assert not self.__initial

            if self.__scroll_hits is not None:
                hits = self.__scroll_hits

                self.__scroll_hits = None
            else:
                hits = yield from self.search()

            if hits:
                return hits
            else:
                raise StopAsyncIteration

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

    @asyncio.coroutine
    def scroll(self):
        assert self.__initial

        resp = yield from self._es.search(
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

        return hits

    @asyncio.coroutine
    def search(self):
        assert not self.__initial

        resp = yield from self._es.scroll(
            self._scroll_id, scroll=self._scroll,
        )

        hits = resp['hits']['hits']

        self.__found += len(hits)

        self.__has_more = self.__found < self._total

        return hits

    @asyncio.coroutine
    def clear_scroll(self):
        if self._scroll_id is not None and self._clear_scroll:
            yield from self._es.clear_scroll(
                body={
                    'scroll_id': [self._scroll_id],
                },
            )
