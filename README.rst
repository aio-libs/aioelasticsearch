aioelasticsearch
================

:info: aioelasticsearch-py wrapper asyncio

.. image:: https://img.shields.io/travis/wikibusiness/aioelasticsearch.svg
    :target: https://travis-ci.org/wikibusiness/aioelasticsearch

.. image:: https://img.shields.io/pypi/v/aioelasticsearch.svg
    :target: https://pypi.python.org/pypi/aioelasticsearch

Installation
------------

.. code-block:: shell

    pip install aioelasticsearch

Usage
-----

.. code-block:: python

    from aioelasticsearch import Elasticsearch

    es = Elasticsearch()

    # es...

    await es.close()  # is a MUST!

Additions
---------

.. code-block:: python

    from aioelasticsearch import Elasticsearch
    from aioelasticsearch.helpers import Scan

    es = Elasticsearch()

    async with Scan(
        es
        index='<index>',
        doc_type='<doc type>',
        query={'<query>'},
    ) as scan:
        async for scroll in scan:
            for doc in scroll:
                print(doc['_source'])
