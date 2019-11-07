import asyncio

from elasticsearch import Elasticsearch as _Elasticsearch  # noqa # isort:skip
from elasticsearch.connection_pool import (ConnectionSelector, # noqa # isort:skip
                                           RoundRobinSelector)
from elasticsearch.serializer import JSONSerializer  # noqa # isort:skip

from .exceptions import *  # noqa # isort:skip
from .pool import AIOHttpConnectionPool  # noqa # isort:skip
from .transport import AIOHttpTransport  # noqa # isort:skip


__version__ = '0.6.0'


class Elasticsearch(_Elasticsearch):

    def __init__(
        self,
        hosts=None,
        transport_class=AIOHttpTransport,
        *,
        loop=None,
        **kwargs
    ):
        if loop is None:
            loop = asyncio.get_event_loop()

        self.loop = loop

        kwargs['loop'] = self.loop

        super().__init__(hosts, transport_class=transport_class, **kwargs)

    async def close(self):
        await self.transport.close()

    async def __aenter__(self):  # noqa
        return self

    async def __aexit__(self, *exc_info):  # noqa
        await self.close()
