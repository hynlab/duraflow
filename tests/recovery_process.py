"""Hard-exit fixture: the resume phase cannot see any prior Python objects."""
import asyncio
import json
import os
import sys
from pathlib import Path

from duraflow import Registry, SQLiteStore
from duraflow.state import subscription
from duraflow.testing import TestEnvironment
from tests.test_engine import A, B, a, b, c, broadcast, bindings


async def main() -> None:
    phase, path, output = sys.argv[1:]
    reg = Registry(broadcast, a, b, c)
    async with TestEnvironment(reg, store=SQLiteStore(path), broadcasts=bindings()) as env:
        if phase == "crash":
            h = await env.client.start(broadcast, 2, request_id="recovery")
            await env.engine.tick()
            for ref, sub in ((A, "price"), (B, "shipping")):
                delivery = await env.transport.receive("values", subscription(env.namespace, sub))
                assert delivery is not None
                await env.worker.process(delivery, ref)
            await env.engine.advance(h.run_id)
            Path(output).write_text(json.dumps({"run_id": h.run_id}))
            os._exit(23)
        else:
            h = env.client.get_handle(json.loads(Path(output).read_text())["run_id"])
            env.clock.advance(31)
            await env.drain()
            state = await h.describe()
            Path(output).write_text(json.dumps({"status": state["status"],
                "resumed_task_executions": env.worker.metrics["executed"],
                "final_publications": sum(m[0] == "analyzed" for m in env.transport.publications)}))


if __name__ == "__main__":
    asyncio.run(main())
