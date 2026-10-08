"""Run a connected role: python -m examples.runtime workflow-engine --initialize."""

import argparse
import asyncio
import signal

from duraflow import Registry, Runtime, RuntimeSettings
from examples.quickstart import double, example

registry = Registry(example, double)


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "role", choices=("workflow-engine", "workflow-worker", "task-worker", "tag-engine", "task-tag-engine", "client")
    )
    parser.add_argument("--from-environment", action="store_true")
    parser.add_argument("--initialize", action="store_true")
    args = parser.parse_args()
    settings = (
        RuntimeSettings.from_environment(role=args.role)
        if args.from_environment
        else RuntimeSettings(
            database_url="sqlite:///duraflow.db",
            task_journal_url="sqlite:///duraflow-tasks.db",
            broker_url="pulsar://localhost:6650",
            namespace="runtime-demo",
        )
    )
    runtime = Runtime(role=args.role, settings=settings, app="examples.runtime")
    if args.initialize:
        await runtime.initialize()
    async with runtime:
        if args.role == "client":
            handle = await runtime.client.start(example, 5, request_id="runtime-demo-1")
            print(await handle.result(timeout=30))
        else:
            stop = asyncio.Event()
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, stop.set)
            try:
                await runtime.run(stop=stop)
            finally:
                for sig in (signal.SIGTERM, signal.SIGINT):
                    loop.remove_signal_handler(sig)


if __name__ == "__main__":
    asyncio.run(main())
