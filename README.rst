aioelasticsearch
================

:info: elasticsearch-py wrapper for asyncio

.. image:: https://img.shields.io/travis/aio-libs/aioelasticsearch.svg
    :target: https://travis-ci.org/aio-libs/aioelasticsearch

.. image:: https://img.shields.io/pypi/v/aioelasticsearch.svg
    :target: https://pypi.python.org/pypi/aioelasticsearch

.. image:: https://codecov.io/gh/aio-libs/aioelasticsearch/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/aio-libs/aioelasticsearch

Getting Started
---------------

Aioelasticsearch - is an asynchronous elasticsearch python client wrapper.

Prerequisites
-------------

Running Elasticsearch server is required.

Run it with docker (Linux/MAC):

.. code-block:: shell

    docker run -p 9200:9200 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:6.0.0
    
read more in `CONTRIBUTE.rst <./CONTRIBUTE.rst>`_

Installation
------------

.. code-block:: shell

    pip install aioelasticsearch

Usage
-----

.. code-block:: python

    import asyncio

    from aioelasticsearch import Elasticsearch

    async def go():
        es = Elasticsearch()

        print(await es.search())

        await es.close()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(go())
    loop.close()

Features
--------

Asynchronous `scroll <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html>`_

.. code-block:: python

    import asyncio

    from aioelasticsearch import Elasticsearch
    from aioelasticsearch.helpers import Scan

    async def go():
        async with Elasticsearch() as es:
            async with Scan(
                es,
                index='index',
                doc_type='doc_type',
                query={},
            ) as scan:
                print(scan.total)

                async for doc in scan:
                    print(doc['_source'])

    loop = asyncio.get_event_loop()
    loop.run_until_complete(go())
    loop.close()

Running the tests
-----------------

Enter *pytest* and *tox* in project directory for run tests.

Thanks
------

The library was donated by `Ocean S.A. <https://ocean.io/>`_

Thanks to the company for contribution.
