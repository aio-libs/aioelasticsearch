import asyncio
import collections
import logging
import random

from elasticsearch import RoundRobinSelector
from elasticsearch.exceptions import ImproperlyConfigured

logger = logging.getLogger('elasticsearch')


class AIOHttpConnectionPool:

    def __init__(
        self,
        connections,
        dead_timeout=60,
        timeout_cutoff=5,
        selector_class=RoundRobinSelector,
        randomize_hosts=True,
        *, loop,
        **kwargs
    ):
        self._dead_timeout = dead_timeout
        self.timeout_cutoff = timeout_cutoff
        self.connection_opts = connections
        self.connections = [c for (c, _) in connections]
        self.orig_connections = tuple(self.connections)
        self.dead = asyncio.PriorityQueue(len(self.connections), loop=loop)
        self.dead_count = collections.Counter()

        self.loop = loop

        if randomize_hosts:
            random.shuffle(self.connections)

        self.selector = selector_class(dict(connections))

    def dead_timeout(self, dead_count):
        exponent = min(dead_count - 1, self.timeout_cutoff)
        return self._dead_timeout * 2 ** exponent

    @asyncio.coroutine
    def mark_dead(self, connection):
        now = self.loop.time()

        try:
            self.connections.remove(connection)
        except ValueError:
            # connection not alive or another thread marked it already, ignore
            return
        else:
            self.dead_count[connection] += 1
            dead_count = self.dead_count[connection]

            timeout = self.dead_timeout(dead_count)

            yield from self.dead.put((now + timeout, connection))
            logger.warning(
                'Connection %r has failed for %i times in a row, '
                'putting on %i second timeout.',
                connection, dead_count, timeout,
            )

    def mark_live(self, connection):
        try:
            del self.dead_count[connection]
        except KeyError:
            # possible due to race condition
            pass

    @asyncio.coroutine
    def resurrect(self, force=False):
        if self.dead.empty():
            # we are forced to return a connection, take one from the original
            # list. This is to avoid a race condition where get_connection can
            # see no live connections but when it calls resurrect self.dead is
            # also empty. We assume that other threat has resurrected all
            # available connections so we can safely return one at random.
            if force:
                return random.choice(self.orig_connections)
            return

        try:
            timeout, connection = self.dead.get_nowait()
        except asyncio.QueueEmpty:
            # other thread has been faster and the queue is now empty. If we
            # are forced, return a connection at random again.
            if force:
                return random.choice(self.orig_connections)
            return

        if not force and timeout > self.loop.time():
            # return it back if not eligible and not forced
            yield from self.dead.put((timeout, connection))
            return

        # either we were forced or the connection is elligible to be retried
        self.connections.append(connection)

        logger.info(
            'Resurrecting connection %r (force=%s).',
            connection, force,
        )

        return connection

    @asyncio.coroutine
    def get_connection(self):
        yield from self.resurrect()

        if not self.connections:
            yield from self.resurrect(force=True)

        if len(self.connections) > 1:
            return self.selector.select(self.connections)

        return self.connections[0]

    def close(self):
        coros = [connection.close() for connection in self.orig_connections]

        while not self.dead.empty():
            _, connection = self.dead.get_nowait()
            coros.append(connection.close())

        return asyncio.gather(*coros, return_exceptions=True, loop=self.loop)


class DummyConnectionPool(AIOHttpConnectionPool):

    def __init__(self, connections, **kwargs):
        if len(connections) != 1:
            raise ImproperlyConfigured(
                'DummyConnectionPool needs exactly one connection defined.',
            )

        self.connection_opts = connections
        self.connection = connections[0][0]
        self.connections = (self.connection, )

    @asyncio.coroutine
    def get_connection(self):
        return self.connection

    def close(self):
        return self.connection.close()

    def mark_live(self, connection):
        pass

    @asyncio.coroutine
    def mark_dead(self, connection):
        pass

    @asyncio.coroutine
    def resurrect(self, force=False):
        pass
