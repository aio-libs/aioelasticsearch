import asyncio
import gc
import time
import uuid

import elasticsearch
import pytest
from aiohttp.test_utils import unused_port
from docker import from_env as docker_from_env

import aioelasticsearch


@pytest.fixture
def loop(request):
    asyncio.set_event_loop(None)

    loop = asyncio.new_event_loop()

    yield loop

    if not loop._closed:
        loop.call_soon(loop.stop)
        loop.run_forever()
        loop.close()

    gc.collect()
    asyncio.set_event_loop(None)


@pytest.fixture(scope='session')
def session_id():
    '''Unique session identifier, random string.'''
    return str(uuid.uuid4())


@pytest.fixture(scope='session')
def docker():
    client = docker_from_env(version='auto')
    return client


def pytest_addoption(parser):
    parser.addoption("--es_tag", action="append", default=[],
                     help=("Elasticsearch server versions. "
                           "May be used several times. "
                           "6.0.0 by default"))
    parser.addoption("--no-pull", action="store_true", default=False,
                     help="Don't perform docker images pulling")
    parser.addoption("--local-docker", action="store_true", default=False,
                     help="Use 0.0.0.0 as docker host, useful for MacOs X")


def pytest_generate_tests(metafunc):
    if 'es_tag' in metafunc.fixturenames:
        tags = set(metafunc.config.option.es_tag)
        if not tags:
            tags = ['6.0.0']
        else:
            tags = list(tags)
        metafunc.parametrize("es_tag", tags, scope='session')


@pytest.fixture(scope='session')
def es_container(docker, session_id, es_tag, request):
    image = 'docker.elastic.co/elasticsearch/elasticsearch:{}'.format(es_tag)

    if not request.config.option.no_pull:
        docker.images.pull(image)

    es_auth = ('elastic', 'changeme')

    if request.config.option.local_docker:
        es_port_9200 = es_access_port = unused_port()
        es_port_9300 = unused_port()
    else:
        es_port_9200 = es_port_9300 = None
        es_access_port = 9200

    container = docker.containers.run(
        image=image,
        detach=True,
        name='aioelasticsearch-' + session_id,
        ports={
            '9200/tcp': es_port_9200,
            '9300/tcp': es_port_9300,
        },
        environment={
            'http.host': '0.0.0.0',
            'transport.host': '127.0.0.1',
        },
    )

    if request.config.option.local_docker:
        docker_host = '0.0.0.0'
    else:
        inspection = docker.api.inspect_container(container.id)
        docker_host = inspection['NetworkSettings']['IPAddress']

    delay = 0.1
    for i in range(20):
        es = elasticsearch.Elasticsearch(
            [{
                'host': docker_host,
                'port': es_access_port,
            }],
            http_auth=es_auth,
        )

        try:
            es.transport.perform_request('GET', '/_nodes/_all/http')
        except elasticsearch.TransportError:
            time.sleep(delay)
            delay *= 2
        else:
            break
        finally:
            es.transport.close()
    else:
        pytest.fail("Cannot start elastic server")

    yield {
        'host': docker_host,
        'port': es_access_port,
        'auth': es_auth,
    }

    container.kill(signal=9)
    container.remove(force=True)


@pytest.fixture
def es_clean(es_container):
    def do():
        es = elasticsearch.Elasticsearch(
            hosts=[{
                'host': es_container['host'],
                'port': es_container['port'],
            }],
            http_auth=es_container['auth'],
        )

        try:
            es.transport.perform_request('DELETE', '/_template/*')
            es.transport.perform_request('DELETE', '/_all')
        finally:
            es.transport.close()

    return do


@pytest.fixture
def es_server(es_clean, es_container):
    es_clean()

    return es_container


@pytest.fixture
def es(es_server, auto_close, loop):
    es = aioelasticsearch.Elasticsearch(
        hosts=[{
            'host': es_server['host'],
            'port': es_server['port'],
        }],
        http_auth=es_server['auth'],
        loop=loop,
    )

    return auto_close(es)


@pytest.fixture
def auto_close(loop):
    close_list = []

    def f(arg):
        close_list.append(arg)
        return arg

    yield f

    for arg in close_list:
        loop.run_until_complete(arg.close())


@pytest.mark.tryfirst
def pytest_pycollect_makeitem(collector, name, obj):
    if collector.funcnamefilter(name):
        item = pytest.Function(name, parent=collector)

        if 'run_loop' in item.keywords:
            return list(collector._genfunctions(name, obj))


@pytest.mark.tryfirst
def pytest_pyfunc_call(pyfuncitem):
    if 'run_loop' in pyfuncitem.keywords:
        funcargs = pyfuncitem.funcargs

        loop = funcargs['loop']

        testargs = {
            arg: funcargs[arg]
            for arg in pyfuncitem._fixtureinfo.argnames
        }

        assert asyncio.iscoroutinefunction(pyfuncitem.obj)

        loop.run_until_complete(pyfuncitem.obj(**testargs))

        return True


@pytest.fixture
def populate(es, loop):
    async def do(index, n, body):
        coros = []

        await es.indices.create(index)

        for i in range(n):
            coros.append(
                es.index(
                    index=index,
                    id=str(i),
                    body=body,
                ),
            )

        await asyncio.gather(*coros, loop=loop)
        await es.indices.refresh()
    return do
