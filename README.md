# Duraflow

**Durable workflows for Python, backed by PostgreSQL and Apache Pulsar.**

[![CI](https://github.com/hynlab/duraflow/actions/workflows/ci.yml/badge.svg)](https://github.com/hynlab/duraflow/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.12%2B-blue)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

Write workflows as Python `async` functions. Duraflow records their progress,
dispatches tasks to workers, and replays committed results so execution can
continue after a process restart.

Use it to coordinate background jobs, join results from multiple services, or
build long-running processes that wait for timers and external signals.

## Features

- **Python-native orchestration** — typed tasks, sequential steps, parallel joins,
  races, and child workflows.
- **Durable progress** — recorded results, persistent timers, buffered signals,
  and execution rollover.
- **Distributed workers** — PostgreSQL stores workflow state; Apache Pulsar
  delivers tasks and business events.
- **Broadcast and join** — publish once and wait for a declared set of handlers.
- **Failure handling** — retry policies, deadlines, execution leases, and stable
  task idempotency keys.
- **Local development** — an in-memory test environment, SQLite storage, and
  runnable examples without external services.
- **Operational tools** — CLI inspection and controls, readiness probes,
  Prometheus metrics, and dead-letter inspection.

Duraflow is currently **alpha**. APIs and storage formats may evolve; see the
[operations guide](guide/operations.md) for execution and recovery considerations.

## Installation

Requires **Python 3.12+**. To use the version documented in this repository:

```bash
git clone https://github.com/hynlab/duraflow.git
cd duraflow
python -m venv .venv
source .venv/bin/activate
python -m pip install -e .
```

For PostgreSQL and Pulsar support, install the optional adapters:

```bash
python -m pip install -e '.[postgres,pulsar]'
```

See [Getting started](guide/getting-started.md) for environment setup and examples.

## Quick start

This complete example runs locally without a database or broker:

```python
import asyncio

from duraflow import Registry, TaskRef, WorkflowContext, task, workflow
from duraflow.testing import TestEnvironment

DOUBLE = TaskRef("double", int, int)


@task(ref=DOUBLE)
def double(value: int) -> int:
    return value * 2


@workflow(name="example", version=1, build_id="example-v1")
async def example(ctx: WorkflowContext, value: int) -> int:
    first, second = await ctx.gather(
        ctx.call(DOUBLE, value),
        ctx.call(DOUBLE, value + 1),
    )
    return first + second


async def main() -> None:
    async with TestEnvironment(Registry(example, double)) as env:
        handle = await env.client.start(example, 5, request_id="demo-1")
        print(await env.run(handle))  # 22


if __name__ == "__main__":
    asyncio.run(main())
```

Workflows describe **what should happen**; tasks perform the actual work, such as
API calls or database writes. `ctx.call()` schedules a durable task, while calling
a decorated task function directly remains an ordinary Python call.

Run the included examples:

```bash
python examples/quickstart.py
python examples/broadcast_join.py
```

## How it works

```text
Client ──► PostgreSQL ◄── Engine ──► Pulsar ──► Workers
               ▲                                 │
               └────── recorded task results ─────┘
```

The engine reconstructs a workflow from its recorded history and schedules its
next steps. Workers execute tasks and persist results. Completed task results
are reused during replay. Tasks that lose their result before it is committed
may execute again, so external effects should use the task's idempotency key.

## Guides

| Guide | What you'll learn |
| --- | --- |
| [Getting started](guide/getting-started.md) | Install Duraflow and run your first workflow |
| [Writing workflows](guide/workflows.md) | Tasks, retries, timers, signals, children, and replay |
| [Broadcast and join](guide/broadcast-and-join.md) | Coordinate multiple handlers from one event |
| [Running distributed services](guide/distributed.md) | Start PostgreSQL, Pulsar, engines, and workers |
| [Operations](guide/operations.md) | Configure services, inspect runs, and manage recovery |
| [Testing and contributing](guide/testing.md) | Test workflows, run repository checks, and contribute |

Browse the [guide index](guide/README.md) for a suggested reading order.

## Contributing

Bug reports, documentation improvements, and pull requests are welcome.
Please include a reproducible example when [reporting an issue](https://github.com/hynlab/duraflow/issues).

```bash
python -m pip install -e '.[dev,postgres]'
make check
```

See [Testing and contributing](guide/testing.md) for integration tests and the
development workflow.

## License

Duraflow is licensed under the [Apache License 2.0](LICENSE).
See [PROVENANCE.md](PROVENANCE.md) for implementation provenance.
