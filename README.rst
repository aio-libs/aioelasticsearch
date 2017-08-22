aioelasticsearch
================

:info: elasticsearch-py wrapper for asyncio

.. image:: https://img.shields.io/travis/wikibusiness/aioelasticsearch.svg
    :target: https://travis-ci.org/wikibusiness/aioelasticsearch

.. image:: https://img.shields.io/pypi/v/aioelasticsearch.svg
    :target: https://pypi.python.org/pypi/aioelasticsearch

.. image:: https://codecov.io/gh/wikibusiness/aioelasticsearch/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/wikibusiness/aioelasticsearch

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
