import asyncio

import pytest
from aioelasticsearch import Elasticsearch


@pytest.fixture
def loop(request):
    with pytest.raises(RuntimeError):
        asyncio.get_event_loop()

    loop = asyncio.new_event_loop()

    loop.set_debug(True)

    request.addfinalizer(lambda: asyncio.set_event_loop(None))

    yield loop

    loop.call_soon(loop.stop)
    loop.run_forever()
    loop.close()


@pytest.fixture
def es(loop):
    es = Elasticsearch(loop=loop)

    def _flush_es():
        delete_template = es.transport.perform_request(
            'DELETE',
            '/_template/*',
        )
        delete_all = es.transport.perform_request(
            'DELETE',
            '/_all',
        )

        return asyncio.gather(*[delete_template, delete_all], loop=loop)

    loop.run_until_complete(_flush_es())

    yield es

    loop.run_until_complete(_flush_es())
    loop.run_until_complete(es.close())


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
