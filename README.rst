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
------------

Aioelasticsearch - the asynchronous python client wrap for elasticsearch, the client needs to be connected to the elasticsearch server.

Prerequisites
------------

You can run Elasticsearch locally or in a docker.

Run it with docker:

.. code-block:: shell

    docker run -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.0.0

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

Pytest

.. code-block:: shell
  python -m pytest 

For TOX tests with different python install different python form *tox.ini* and enter *tox* in project directory.

Thanks
------

The library was donated by `Ocean S.A. <https://ocean.io/>`_

Thanks to the company for contribution.
