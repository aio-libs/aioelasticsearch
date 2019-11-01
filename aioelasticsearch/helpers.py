import logging

from elasticsearch.helpers import ScanError

from aioelasticsearch import NotFoundError

__all__ = ('Scan', 'ScanError')


logger = logging.getLogger('elasticsearch')


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
        scroll_kwargs=None,
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
        self._scroll_kwargs = scroll_kwargs or {}

        self._scroll_id = None

        self._total = 0

        self._initial = True
        self._done = False
        self._hits = []
        self._hits_idx = 0
        self._successful_shards = 0
        self._total_shards = 0

    async def __aenter__(self):  # noqa
        await self._do_search()
        return self

    async def __aexit__(self, *exc_info):  # noqa
        await self._do_clear_scroll()

    def __aiter__(self):
        if self._initial:
            raise RuntimeError("Scan operations should be done "
                               "inside async context manager")
        return self

    async def __anext__(self):  # noqa
        if self._done:
            raise StopAsyncIteration

        if self._hits_idx >= len(self._hits):
            if self._successful_shards < self._total_shards:
                logger.warning(
                    'Scroll request has only succeeded on %d shards out of %d.',  # noqa
                    self._successful_shards, self._total_shards
                )
                if self._raise_on_error:
                    raise ScanError(
                        self._scroll_id,
                        'Scroll request has only succeeded on {} shards out of {}.'  # noqa
                        .format(self._successful_shards, self._total_shards)
                    )

            await self._do_scroll()
        ret = self._hits[self._hits_idx]
        self._hits_idx += 1
        return ret

    @property
    def scroll_id(self):
        if self._initial:
            raise RuntimeError("Scan operations should be done "
                               "inside async context manager")

        return self._scroll_id

    @property
    def total(self):
        if self._initial:
            raise RuntimeError("Scan operations should be done "
                               "inside async context manager")
        return self._total

    async def _do_search(self):
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
            self._total = resp['hits']['total']
            self._update_state(resp)

    async def _do_scroll(self):
        resp = await self._es.scroll(
            scroll_id=self._scroll_id,
            scroll=self._scroll,
            **self._scroll_kwargs,
        )
        self._update_state(resp)

        if self._done:
            raise StopAsyncIteration

    async def _do_clear_scroll(self):
        if self._scroll_id is not None and self._clear_scroll:
            await self._es.clear_scroll(
                body={'scroll_id': [self._scroll_id]},
                ignore=404,
            )

    def _update_state(self, resp):
        self._hits = resp['hits']['hits']
        self._hits_idx = 0
        self._scroll_id = resp.get('_scroll_id')
        self._successful_shards = resp['_shards']['successful']
        self._total_shards = resp['_shards']['total']
        self._done = not self._hits or self._scroll_id is None
