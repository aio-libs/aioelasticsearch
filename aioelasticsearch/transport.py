import asyncio
import logging
from itertools import chain, count

from elasticsearch.serializer import (DEFAULT_SERIALIZERS, Deserializer,
                                      JSONSerializer)
from elasticsearch.transport import Transport, get_host_info

from .connection import AIOHttpConnection
from .exceptions import (ConnectionError, ConnectionTimeout,
                         SerializationError, TransportError)
from .pool import AIOHttpConnectionPool, DummyConnectionPool

logger = logging.getLogger('elasticsearch')


class AIOHttpTransport(Transport):

    def __init__(
        self,
        hosts,
        connection_class=AIOHttpConnection,
        connection_pool_class=AIOHttpConnectionPool,
        host_info_callback=get_host_info,
        serializer=JSONSerializer(),
        serializers=None,
        sniff_on_start=False,
        sniffer_timeout=None,
        sniff_timeout=.1,
        sniff_on_connection_fail=False,
        default_mimetype='application/json',
        max_retries=3,
        retry_on_status=(502, 503, 504, ),
        retry_on_timeout=False,
        send_get_body_as='GET',
        *,
        loop,
        **kwargs
    ):
        self.loop = loop
        self._closed = False

        _serializers = DEFAULT_SERIALIZERS.copy()
        # if a serializer has been specified,
        # use it for deserialization as well
        _serializers[serializer.mimetype] = serializer
        # if custom serializers map has been supplied,
        # override the defaults with it
        if serializers:
            _serializers.update(serializers)
        # create a deserializer with our config
        self.deserializer = Deserializer(_serializers, default_mimetype)

        self.max_retries = max_retries
        self.retry_on_timeout = retry_on_timeout
        self.retry_on_status = retry_on_status
        self.send_get_body_as = send_get_body_as

        # data serializer
        self.serializer = serializer

        # sniffing data
        self.sniffer_timeout = sniffer_timeout
        self.sniff_on_connection_fail = sniff_on_connection_fail
        self.last_sniff = self.loop.time()
        self.sniff_timeout = sniff_timeout

        # callback to construct host dict from data in /_cluster/nodes
        self.host_info_callback = host_info_callback

        # store all strategies...
        self.connection_pool_class = connection_pool_class
        self.connection_class = connection_class
        self._connection_pool_lock = asyncio.Lock(loop=self.loop)

        # ...save kwargs to be passed to the connections
        self.kwargs = kwargs
        self.hosts = hosts

        # ...and instantiate them
        self.set_connections(hosts)
        # retain the original connection instances for sniffing
        self.seed_connections = set(self.connection_pool.connections)

        self.seed_connection_opts = self.connection_pool.connection_opts

        self.initial_sniff_task = None

        if sniff_on_start:
            def _initial_sniff_reset(fut):
                self.initial_sniff_task = None

            task = self.sniff_hosts(initial=True)

            self.initial_sniff_task = asyncio.ensure_future(task,
                                                            loop=self.loop)
            self.initial_sniff_task.add_done_callback(_initial_sniff_reset)

    def set_connections(self, hosts):
        if self._closed:
            raise RuntimeError("Transport is closed")

        def _create_connection(host):
            # if this is not the initial setup look at the existing connection
            # options and identify connections that haven't changed and can be
            # kept around.
            if hasattr(self, 'connection_pool'):
                existing_connections = (self.connection_pool.connection_opts +
                                        self.seed_connection_opts)

                for (connection, old_host) in existing_connections:
                    if old_host == host:
                        return connection

            kwargs = self.kwargs.copy()
            kwargs.update(host)
            kwargs['loop'] = self.loop

            return self.connection_class(**kwargs)

        connections = map(_create_connection, hosts)

        connections = list(zip(connections, hosts))

        if len(connections) == 1:
            self.connection_pool = DummyConnectionPool(
                connections,
                loop=self.loop,
                **self.kwargs
            )
        else:
            self.connection_pool = self.connection_pool_class(
                connections,
                loop=self.loop,
                **self.kwargs
            )

    async def _get_sniff_data(self, initial=False):
        previous_sniff = self.last_sniff

        tried = set()

        try:
            # reset last_sniff timestamp
            self.last_sniff = self.loop.time()
            for connection in chain(
                self.connection_pool.connections,
                self.seed_connections,
            ):
                if connection in tried:
                    continue

                tried.add(connection)

                try:
                    # use small timeout for the sniffing request,
                    # should be a fast api call
                    _, headers, node_info = await connection.perform_request(
                        'GET',
                        '/_nodes/_all/http',
                        timeout=self.sniff_timeout if not initial else None,
                    )

                    node_info = self.deserializer.loads(
                        node_info, headers.get('content-type'),
                    )
                    break
                except (ConnectionError, SerializationError):
                    pass
            else:
                raise TransportError('N/A', 'Unable to sniff hosts.')
        except:  # noqa
            # keep the previous value on error
            self.last_sniff = previous_sniff
            raise

        return list(node_info['nodes'].values())

    async def sniff_hosts(self, initial=False):
        if self._closed:
            raise RuntimeError("Transport is closed")
        async with self._connection_pool_lock:
            node_info = await self._get_sniff_data(initial)
            hosts = (self._get_host_info(n) for n in node_info)
            hosts = [host for host in hosts if host is not None]
            # we weren't able to get any nodes, maybe using an incompatible
            # transport_schema or host_info_callback blocked all - raise error.
            if not hosts:
                raise TransportError(
                    'N/A', 'Unable to sniff hosts - no viable hosts found.',
                )

            old_connection_pool = self.connection_pool

            self.set_connections(hosts)

            skip = (self.seed_connections |
                    self.connection_pool.orig_connections)

            await old_connection_pool.close(skip=skip)

    async def close(self):
        if self._closed:
            return
        seeds = self.seed_connections - self.connection_pool.orig_connections

        coros = [connection.close() for connection in seeds]

        if self.initial_sniff_task is not None:
            self.initial_sniff_task.cancel()

            async def _initial_sniff_wrapper():
                try:
                    await self.initial_sniff_task
                except asyncio.CancelledError:
                    return

            coros.append(_initial_sniff_wrapper())

        coros.append(self.connection_pool.close())

        await asyncio.gather(*coros, loop=self.loop)
        self._closed = True

    async def get_connection(self):
        if self._closed:
            raise RuntimeError("Transport is closed")
        if self.initial_sniff_task is not None:
            await self.initial_sniff_task

        if self.sniffer_timeout:
            if self.loop.time() >= self.last_sniff + self.sniffer_timeout:
                await self.sniff_hosts()

        async with self._connection_pool_lock:
            return self.connection_pool.get_connection()

    async def mark_dead(self, connection):
        if self._closed:
            raise RuntimeError("Transport is closed")
        self.connection_pool.mark_dead(connection)

        if self.sniff_on_connection_fail:
            await self.sniff_hosts()

    async def _perform_request(
        self,
        method, url, params, body,
        ignore=(), timeout=None, headers=None,
    ):
        for attempt in count(1):  # pragma: no branch
            connection = await self.get_connection()

            try:

                status, headers, data = await connection.perform_request(
                    method, url, params, body,
                    ignore=ignore, timeout=timeout, headers=headers,
                )
            except TransportError as e:
                if method == 'HEAD' and e.status_code == 404:
                    return False

                retry = False
                if isinstance(e, ConnectionTimeout):
                    retry = self.retry_on_timeout
                elif isinstance(e, ConnectionError):
                    retry = True
                elif e.status_code in self.retry_on_status:
                    retry = True

                if retry:
                    await self.mark_dead(connection)

                    if attempt == self.max_retries:
                        raise
                else:
                    raise

            else:
                self.connection_pool.mark_live(connection)

                if method == 'HEAD':
                    return 200 <= status < 300

                if data:
                    data = self.deserializer.loads(
                        data, headers.get('content-type'),
                    )

                return data

    async def perform_request(self, method, url, headers=None, params=None, body=None):  # noqa
        if self._closed:
            raise RuntimeError("Transport is closed")
        # yarl fix for https://github.com/elastic/elasticsearch-py/blob/d4efb81b0695f3d9f64784a35891b732823a9c32/elasticsearch/client/utils.py#L29  # noqa
        if params is not None:
            to_replace = {}
            for k, v in params.items():
                if isinstance(v, bytes):
                    to_replace[k] = v.decode('utf-8')
            for k, v in to_replace.items():
                params[k] = v

        if body is not None:
            body = self.serializer.dumps(body)

            # some clients or environments don't support sending GET with body
            if method in ('HEAD', 'GET') and self.send_get_body_as != 'GET':
                # send it as post instead
                if self.send_get_body_as == 'POST':
                    method = 'POST'

                # or as source parameter
                elif self.send_get_body_as == 'source':
                    if params is None:
                        params = {}
                    params['source'] = body
                    params['source_content_type'] = self.serializer.mimetype
                    body = None

        if body is not None:
            try:
                body = body.encode('utf-8', 'surrogatepass')
            except (UnicodeDecodeError, AttributeError):
                # bytes/str - no need to re-encode
                pass

        ignore = ()
        timeout = None
        if params:
            timeout = params.pop('request_timeout', None)
            ignore = params.pop('ignore', ())
            if isinstance(ignore, int):
                ignore = (ignore, )

        return await self._perform_request(
            method, url, params, body,
            ignore=ignore, timeout=timeout, headers=headers,
        )
