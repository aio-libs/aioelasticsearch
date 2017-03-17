import asyncio

import pytest


@pytest.mark.run_loop
@asyncio.coroutine
def test_ping(es):
    ping = yield from es.ping()

    assert ping
