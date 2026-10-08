from pathlib import Path
from helpers import replace, write

replace('src/duraflow/observability.py', 'elif type(value) in (int, float) and math.isfinite(value):', 'elif isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value):')
replace('src/duraflow/quarantine.py', 'if not isinstance(envelope, dict) or envelope.get("version") != 1:', 'if not isinstance(envelope, dict) or type(envelope.get("version")) is not int or envelope.get("version") != 1:')
replace('src/duraflow/quarantine.py', '        if current["lease_until"] > now:', '        if current["not_before"] > now:\n            raise Conflict("The current retry is not due")\n        if current["lease_until"] > now:')
write('tests/supervision_process.py', '''
"""A deliberately stuck synchronous task, isolated in a disposable test process."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
import os
import signal
import time

from duraflow import Registry
from duraflow.config import RuntimeSettings
from duraflow.storage import MemoryStore
from duraflow.supervision import supervise
from duraflow.transport import MemoryTransport
from tests.test_engine import double


class Runtime:
    def __init__(self):
        self.metrics = {}
        self.pool = ThreadPoolExecutor(max_workers=1)
    async def run(self, stop, **kwargs):
        future = asyncio.get_running_loop().run_in_executor(self.pool, time.sleep, 60)
        print("READY", flush=True)
        await asyncio.shield(future)
    async def close(self):
        self.pool.shutdown(wait=False, cancel_futures=True)


async def main():
    stop = asyncio.Event()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, stop.set)
    settings = RuntimeSettings(shutdown_timeout=.05, probe_timeout=.05)
    await supervise(Runtime(), MemoryTransport(), MemoryStore(), Registry(double), settings,
                    stop, role="worker", on_hard_timeout=lambda: os._exit(75))


if __name__ == "__main__":
    asyncio.run(main())
''')
write('tests/test_supervision_process.py', '''
import os
import select
import signal
import subprocess
import sys


def test_stuck_synchronous_worker_process_obeys_hard_shutdown_boundary():
    process = subprocess.Popen([sys.executable, '-m', 'tests.supervision_process'],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                               env={**os.environ})
    try:
        assert select.select([process.stdout], [], [], 10)[0]
        assert process.stdout.readline().strip() == 'READY'
        process.send_signal(signal.SIGTERM)
        process.communicate(timeout=5)
        assert process.returncode == 75
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=5)
''')
with Path('tests/test_phase3.py').open('a') as stream:
    stream.write('''\n\n@pytest.mark.integration
@pytest.mark.skipif(not os.getenv("DURAFLOW_TEST_POSTGRES"), reason="Native PostgreSQL not configured")
async def test_native_telemetry_is_bounded_and_contains_no_payload():
    from uuid import uuid4
    from duraflow import Client
    from duraflow.postgres import PostgresStore
    store = PostgresStore(os.environ["DURAFLOW_TEST_POSTGRES"])
    try:
        await store.initialize()
        namespace = "metrics-" + uuid4().hex[:12]
        client = Client(store, Registry(sequence, double), namespace=namespace)
        await client.start(sequence, 99, request_id="metrics-input")
        stats = await store.telemetry(namespace)
        assert stats["statuses"]["PENDING"] == 1
        assert stats["outbox_age"] >= 0
        assert "input" not in json.dumps(stats)
    finally:
        await store.close()
''')
