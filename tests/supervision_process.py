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
    settings = RuntimeSettings(shutdown_timeout=0.05, probe_timeout=0.05)
    await supervise(
        Runtime(),
        MemoryTransport(),
        MemoryStore(),
        Registry(double),
        settings,
        stop,
        role="worker",
        on_hard_timeout=lambda: os._exit(75),
    )


if __name__ == "__main__":
    asyncio.run(main())
