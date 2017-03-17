import asyncio

from elasticsearch import Elasticsearch as _Elasticsearch  # isort:skip
from elasticsearch.exceptions import *  # noqa # isort:skip

from elasticsearch.connection_pool import (  # noqa # isort:skip
    ConnectionSelector, RoundRobinSelector,
)
from elasticsearch.serializer import JSONSerializer  # noqa # isort:skip

from .compat import PY_350  # isort:skip
from .pool import AIOHttpConnectionPool  # noqa # isort:skip
from .transport import AIOHttpTransport  # isort:skip


__version__ = '0.1.1'


class Elasticsearch(_Elasticsearch):

    def __init__(
        self, hosts=None, transport_class=AIOHttpTransport,
        *, loop=None, **kwargs
    ):
        if loop is None:
            loop = asyncio.get_event_loop()

        self.loop = loop

        kwargs['loop'] = self.loop

        super().__init__(hosts, transport_class=transport_class, **kwargs)

    def close(self):
        return self.transport.close()

    if PY_350:
        @asyncio.coroutine
        def __aenter__(self):  # noqa
            return self

        @asyncio.coroutine
        def __aexit__(self, *exc_info):  # noqa
            yield from self.close()
