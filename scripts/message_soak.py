"""Bounded real-service soak with independently restarted protocol-2 processes.

Run from a source checkout: PYTHONPATH=src:. python -m scripts.message_soak
Requires the same DURAFLOW_TEST_* endpoints as native-check.
"""

import argparse
import asyncio
import json
import signal
import statistics
import subprocess
import tempfile
import time
from pathlib import Path

from tests.message_app import APPROVAL
from tests.message_cluster import Processes, eventually


def process_memory(cluster):
    pids = [str(p.pid) for p in cluster.processes.values() if p.poll() is None]
    result = subprocess.run(["ps", "-o", "rss=", "-p", ",".join(pids)], check=True, capture_output=True, text=True)
    return sum(int(value) for value in result.stdout.split()) / 1024


async def run(seconds, batch_size, directory):
    from sqlalchemy import func, select

    started = time.monotonic()
    latencies, samples, faults = [], [], []
    completed = 0
    async with Processes(directory, workflows=2) as cluster:
        for role in ("task", "tags", "task-tags"):
            await cluster.spawn(role, 0)
        await cluster.spawn("task", 1)
        workload_started = time.monotonic()
        batch = 0
        while time.monotonic() - workload_started < seconds:
            began = time.monotonic()
            handles = await asyncio.gather(
                *[
                    cluster.client.start(cluster.ref, i, request_id=f"soak-{batch}-{i}", tags=("soak",))
                    for i in range(batch_size)
                ]
            )
            for handle in handles:
                await cluster.waiting(handle)
            if batch % 5 == 0:
                role = ("engine", "workflow", "task", "tags", "task-tags")[(batch // 5) % 5]
                sig = signal.SIGKILL if (batch // 5) % 2 == 0 else signal.SIGTERM
                await cluster.kill(role, 0, sig=sig)
                await cluster.spawn(role, 0)
                faults.append({"batch": batch, "role": role, "signal": sig.name})
            await cluster.client.signal_tagged(cluster.ref, "soak", APPROVAL, True, signal_id=f"approve-{batch}")
            assert await asyncio.gather(*[h.result(timeout=90) for h in handles]) == [4 * i for i in range(batch_size)]
            elapsed = time.monotonic() - began
            latencies.append(elapsed)
            completed += len(handles)
            effects, calls = cluster.ledger()
            assert len(effects) == completed * 2
            assert set(calls.values()) == {1}, "A settled-work restart unexpectedly repeated an external call"
            samples.append(
                {
                    "batch": batch,
                    "elapsed_seconds": round(time.monotonic() - workload_started, 3),
                    "rss_mib": round(await asyncio.to_thread(process_memory, cluster), 2),
                }
            )
            batch += 1
            print(f"batch={batch} completed={completed} elapsed={samples[-1]['elapsed_seconds']}s", flush=True)

        async def drained():
            for store in cluster.stores:
                async with store.database._transaction() as conn:
                    if (await conn.execute(select(func.count()).select_from(store.outbox))).scalar_one():
                        return False
            return True

        await eventually(drained)
        for process in cluster.processes.values():
            assert process.poll() is None
    quantiles = statistics.quantiles(latencies, n=100, method="inclusive") if len(latencies) > 1 else latencies * 99
    return {
        "status": "passed",
        "protocol": 2,
        "requested_seconds": seconds,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "completed_workflows": completed,
        "accepted_effects": completed * 2,
        "batch_size": batch_size,
        "batch_latency_seconds": {"p50": statistics.median(latencies), "p95": quantiles[94], "p99": quantiles[98]},
        "faults": faults,
        "samples": samples,
        "outbox_drained": True,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=int, default=1800)
    parser.add_argument("--batch-size", type=int, default=4)
    parser.add_argument("--output", type=Path, default=Path("soak-results.json"))
    parser.add_argument("--logs-root", type=Path, default=Path("qualification-artifacts"))
    args = parser.parse_args()
    if not 1 <= args.seconds <= 86400 or not 1 <= args.batch_size <= 32:
        parser.error("seconds must be 1..86400 and batch-size 1..32")
    args.logs_root.mkdir(parents=True, exist_ok=True)
    directory = Path(tempfile.mkdtemp(prefix="soak-", dir=args.logs_root)).resolve()
    try:
        report = asyncio.run(run(args.seconds, args.batch_size, directory))
    except BaseException as exc:
        args.output.write_text(
            json.dumps({"status": "failed", "error_type": type(exc).__name__, "logs": str(directory)}, indent=2) + "\n"
        )
        raise
    report["logs"] = str(directory)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: value for key, value in report.items() if key not in {"samples", "faults"}}, indent=2))


if __name__ == "__main__":
    main()
