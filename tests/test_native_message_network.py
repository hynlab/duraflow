"""Real database partitions isolated to an engine, including latency and pool recovery."""

import asyncio
import os

import pytest

from tests.message_app import APPROVAL
from tests.message_cluster import Processes
from tests.tcp_fault_proxy import TCPFaultProxy

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not (os.getenv("DURAFLOW_TEST_POSTGRES") and os.getenv("DURAFLOW_TEST_PULSAR")),
        reason="Native endpoints required",
    ),
]


@pytest.mark.parametrize("restart_engine", [False, True])
async def test_engine_database_partition_reconnects_without_losing_signal(tmp_path, restart_engine):
    from sqlalchemy.engine import make_url

    url = make_url(os.environ["DURAFLOW_TEST_POSTGRES"])
    async with TCPFaultProxy(url.host, url.port or 5432) as proxy:
        proxied = url.set(host="127.0.0.1", port=proxy.local_port).render_as_string(hide_password=False)
        async with Processes(tmp_path, engines=0) as cluster:
            await cluster.spawn("engine", 0, database_url=proxied)
            await cluster.spawn("task", 0)
            handle = await cluster.client.start(cluster.ref, 6, request_id="partition")
            await cluster.waiting(handle)
            assert proxy.forwarded_bytes > 0, "The engine must actually use the fault-injected wire"
            proxy.disconnect()
            sending = asyncio.create_task(handle.signal(APPROVAL, True, signal_id="during-partition"))
            try:
                await asyncio.sleep(1)
                assert not sending.done()
                if restart_engine:
                    await cluster.kill("engine", 0)
                proxy.blocked = False
                proxy.delay = 0.005
                if restart_engine:
                    await cluster.spawn("engine", 0, database_url=proxied)
                await asyncio.wait_for(sending, 40)
                assert await handle.result(timeout=40) == 24
                assert len(cluster.ledger()[0]) == 2
            finally:
                proxy.blocked = False
                if not sending.done():
                    sending.cancel()
                await asyncio.gather(sending, return_exceptions=True)
