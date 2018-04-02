import asyncio

import logging
from operator import methodcaller

from aioelasticsearch import NotFoundError
from elasticsearch.helpers import ScanError, _chunk_actions, expand_action
from elasticsearch.exceptions import TransportError

from .compat import PY_352


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

    if not PY_352:
        __aiter__ = asyncio.coroutine(__aiter__)

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
            self._scroll_id,
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


async def worker_bulk(client, datas , actions,  **kwargs):
    try:
        resp = await client.bulk("\n".join(actions) + '\n', **kwargs)
    except TransportError as e:
        return e, datas
    fail_actions = []
    finish_count = 0
    for data, (op_type, item) in zip(datas, map(methodcaller('popitem'),
                                                resp['items'])):
        ok = 200 <= item.get('status', 500) < 300
        if not ok:
            fail_actions.append(data)
        else:
            finish_count += 1
    return finish_count, fail_actions


def _get_fail_data(results, serializer):
    finish_count = 0
    bulk_action = []
    bulk_data = []
    lazy_exception = None
    for result in results:
        if isinstance(result[0], int):
            finish_count += result[0]
        else:
            if lazy_exception is None:
                lazy_exception = result[0]

        for fail_data in result[1]:
            for _ in fail_data:
                bulk_data.append(_)
        if result[1]:
            bulk_action.extend(map(serializer.dumps,result[1]))
    return finish_count, bulk_data, bulk_action, lazy_exception


async def _retry_handler(client, futures, max_retries, initial_backoff,
                         max_backoff, **kwargs):
    finish = 0
    for attempt in range(max_retries + 1):
        if attempt:
            sleep = min(max_backoff, initial_backoff * 2 ** (attempt - 1))
            await asyncio.sleep(sleep)

        results = await asyncio.gather(*futures,
                                       return_exceptions=True)
        futures = []

        count, fail_data, fail_action, lazy_exception = \
            _get_fail_data(results, client.transport.serializer)

        finish += count

        if not fail_action or attempt == max_retries:
            break

        coroutine = worker_bulk(client, fail_data, fail_action, **kwargs)
        futures.append(asyncio.ensure_future(coroutine))

    if lazy_exception:
        raise lazy_exception

    return finish, fail_data


async def bulk(client, actions, concurrency_limit=2, chunk_size=500,
               max_chunk_bytes=100 * 1024 * 1024,
               expand_action_callback=expand_action, max_retries=0,
               initial_backoff=2, max_backoff=600, stats_only=False, **kwargs):

    async def concurrency_wrapper(chunk_iter):

        partial_count = 0
        if stats_only:
            partial_fail = 0
        else:
            partial_fail = []
        for bulk_data, bulk_action in chunk_iter:
            futures = [worker_bulk(client, bulk_data, bulk_action, **kwargs)]
            count, fails = await _retry_handler(client,
                                                futures,
                                                max_retries,
                                                initial_backoff,
                                                max_backoff, **kwargs)
            partial_count += count
            if stats_only:
                partial_fail += len(fails)
            else:
                partial_fail.extend(fails)
        return partial_count, partial_fail

    actions = map(expand_action_callback, actions)
    finish_count = 0
    if stats_only:
        fail_datas = 0
    else:
        fail_datas = []

    chunk_action_iter = _chunk_actions(actions, chunk_size, max_chunk_bytes,
                                       client.transport.serializer)

    tasks = []
    concurrency_limit = concurrency_limit if concurrency_limit > 0 else 2
    for i in range(concurrency_limit):
        tasks.append(concurrency_wrapper(chunk_action_iter))

    results = await asyncio.gather(*tasks)
    for p_count, p_fails in results:
        finish_count += p_count
        if stats_only:
            fail_datas += p_fails
        else:
            fail_datas.extend(p_fails)

    return finish_count, fail_datas
