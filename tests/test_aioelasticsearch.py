import pytest


@pytest.mark.run_loop
async def test_ping(es):
    ping = await es.ping()

    assert ping
