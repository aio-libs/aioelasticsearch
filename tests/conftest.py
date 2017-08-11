import asyncio
import time
import uuid

import elasticsearch
import pytest
from docker import from_env as docker_from_env

from aioelasticsearch import Elasticsearch


@pytest.fixture
def loop(request):
    asyncio.set_event_loop(None)

    loop = asyncio.new_event_loop()

    yield loop

    loop.call_soon(loop.stop)
    loop.run_forever()
    loop.close()


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
                           "5.5.1 by default"))
    parser.addoption("--no-pull", action="store_true", default=False,
                     help="Don't perform docker images pulling")


def pytest_generate_tests(metafunc):
    if 'es_tag' in metafunc.fixturenames:
        tags = set(metafunc.config.option.es_tag)
        if not tags:
            tags = ['5.5.1']
        else:
            tags = list(tags)
        metafunc.parametrize("es_tag", tags, scope='session')


@pytest.fixture(scope='session')
def es_container(docker, session_id, es_tag, request):
    image = 'docker.elastic.co/elasticsearch/elasticsearch:{}'.format(es_tag)
    if not request.config.option.no_pull:
        docker.images.pull(image)
    container = docker.containers.run(
        image, detach=True,
        name='aioelasticsearch-'+session_id,
        ports={'9200/tcp': None, '9300/tcp': None},
        environment={'http.host': '0.0.0.0',
                     'transport.host': '127.0.0.1'})

    inspection = docker.api.inspect_container(container.id)
    host = inspection['NetworkSettings']['IPAddress']
    delay = 0.001
    for i in range(100):
        es = elasticsearch.Elasticsearch([host],
                                         http_auth=('elastic', 'changeme'))
        try:
            es.exists(index='index', doc_type='doc-type', id=1)
            break
        except elasticsearch.TransportError as ex:
            time.sleep(delay)
            delay *= 2
        finally:
            es.transport.close()
    else:
        pytest.fail("Cannot start elastic server")
    ret = {'container': container,
           'host': host,
           'port': 9200,
           'auth': ('elastic', 'changeme')}
    yield ret

    container.kill()
    container.remove()


@pytest.fixture
def es_server(es_container):
    host = es_container['host']
    es = elasticsearch.Elasticsearch([host],
                                     http_auth=('elastic', 'changeme'))

    es.transport.perform_request('DELETE', '/_template/*')
    es.transport.perform_request('DELETE', '/_all')

    return es_container


@pytest.fixture
def es(loop, es_server):
    es = Elasticsearch(loop=loop, hosts=[{'host': es_server['host'],
                                          'port': es_server['port']}],
                       http_auth=es_server['auth'])
    yield es
    loop.run_until_complete(es.close())


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
                    refresh=True,
                ),
            )

        await asyncio.gather(*coros, loop=loop)
    return do
