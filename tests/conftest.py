import asyncio
import gc
import socket
import time
import uuid

import elasticsearch
import pytest
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
    return str(uuid.uuid4())


@pytest.fixture(scope='session')
def docker():
    client = docker_from_env(version='auto')
    return client


def pytest_addoption(parser):
    parser.addoption('--es_tag', action='append', default=[],
                     help=('Elasticsearch server versions. '
                           'May be used several times. '
                           '5.5.1 by default'))
    parser.addoption('--no-pull', action='store_true', default=False,
                     help='Don\'t perform docker images pulling')
    parser.addoption('--local-docker', action='store_true', default=False,
                     help='Use 0.0.0.0 as docker host, useful for MacOs X')


def pytest_generate_tests(metafunc):
    if 'es_tag' in metafunc.fixturenames:
        tags = set(metafunc.config.option.es_tag)
        if not tags:
            tags = ['5.5.1']
        else:
            tags = list(tags)
        metafunc.parametrize('es_tag', tags, scope='session')


@pytest.fixture(scope='session')
def unused_port():
    def factory():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('127.0.0.1', 0))
            return s.getsockname()[1]
    return factory


@pytest.fixture(scope='session')
def es_container(docker, session_id, es_tag, unused_port, request):
    image = 'docker.elastic.co/elasticsearch/elasticsearch:{es_tag}'.format(
        es_tag=es_tag,
    )

    if not request.config.option.no_pull:
        docker.images.pull(image)

    es_port_9200 = str(unused_port())
    es_port_9300 = str(unused_port())

    auth = ('elastic', 'changeme')

    container = None

    try:
        container = docker.containers.run(
            image=image,
            detach=True,
            name='aioelasticsearch-' + session_id,
            ports={
                '9200/tcp': es_port_9200 + '/tcp',
                '9300/tcp': es_port_9300 + '/tcp',
            },
            environment={
                'http.host': '0.0.0.0',
                'transport.host': '127.0.0.1',
            },
        )

        if request.config.option.local_docker:
            docker_ip_address = '0.0.0.0'
        else:
            inspection = docker.api.inspect_container(container.id)
            docker_ip_address = inspection['NetworkSettings']['IPAddress']

        delay = 0.1
        for i in range(10):
            es = elasticsearch.Elasticsearch(
                [{
                    'host': docker_ip_address,
                    'port': es_port_9200,
                }],
                http_auth=auth,
            )

            try:
                es.transport.perform_request('GET', '/_nodes/_all/http')
                break
            except elasticsearch.TransportError as ex:
                time.sleep(delay)
                delay *= 2
            finally:
                es.transport.close()
        else:
            pytest.fail('Cannot start elastic server')

        ret = {
            'container': container,
            'host': docker_ip_address,
            'port': es_port_9200,
            'auth': auth,
        }

        yield ret
    finally:
        if container is not None:
            container.kill()
            container.remove()


@pytest.fixture
def es_server(es_container):
    es = elasticsearch.Elasticsearch(
        hosts=[{
            'host': es_container['host'],
            'port': es_container['port'],
        }],
        http_auth=es_container['auth'],
    )

    es.transport.perform_request('DELETE', '/_template/*')
    es.transport.perform_request('DELETE', '/_all')

    yield es_container

    es.transport.close()


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
def populate(loop):
    async def do(es, index, doc_type, n, body):
        coros = []

        await es.indices.create(index)

        for i in range(n):
            coros.append(
                es.index(
                    index=index,
                    doc_type=doc_type,
                    id=str(i),
                    body=body,
                ),
            )

        await asyncio.gather(*coros, loop=loop)
        await es.indices.refresh()
    return do
