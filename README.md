# Duraflow

**Message-driven durable workflows for Python, SQLite/PostgreSQL, and Apache Pulsar.**

[![CI](https://github.com/hynlab/duraflow/actions/workflows/ci.yml/badge.svg)](https://github.com/hynlab/duraflow/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.12%2B-blue)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

Write orchestration as Python `async` functions. Duraflow records progress,
executes work through message-driven workers, and resumes from committed history
after restarts. Use it for multi-service jobs, human approvals, event-driven joins,
and long-running business processes.

## Features

- **Durable Python workflows:** typed task calls, parallel joins, races, children,
  recorded time/UUID values, and execution rollover.
- **A complete message loop:** workflow starts, replay requests, task results,
  signals, and completion responses travel through Pulsar.
- **Typed signal channels:** non-blocking receive registration, repeated receives,
  bounded streams, payload filters, and tag-targeted delivery.
- **Separate scalable roles:** workflow engines, workflow workers, task workers,
  workflow tag engines, and task tag engines.
- **Failure recovery:** atomic inbox/state/outbox commits, fenced task execution,
  stable idempotency keys, delayed retries, and broker-driven timers.
- **Python-friendly development:** infrastructure-free tests and runnable examples
  that use the same message handlers as the distributed runtime.
- **Operational tools:** CLI inspection and controls, readiness probes, structured
  logs, Prometheus metrics, and explicit history retention.

Duraflow **1.0** uses execution protocol 2. See the [upgrade section](guide/6_operations.md#upgrading-from-protocol-1)
when working with earlier alpha executions.

## Installation

Requires **Python 3.12+**.

```bash
pip install duraflow
```

Includes the PostgreSQL driver, SQLAlchemy, and Pulsar client. SQLite uses Python's
built-in `sqlite3`. No adapter extras are needed; the old `[postgres,pulsar]` extras
remain compatible aliases. PostgreSQL and Pulsar servers are configured separately.

## Quick start

This example runs a complete message-driven workflow locally:

```python
import asyncio

from duraflow import Registry, TaskRef, WorkflowContext, task, workflow
from duraflow.testing import TestEnvironment

DOUBLE = TaskRef("double", int, int)


@task(ref=DOUBLE)
def double(value: int) -> int:
    return value * 2


@workflow(name="example", build_id="example-v1")
async def example(ctx: WorkflowContext, value: int) -> int:
    first, second = await ctx.gather(
        ctx.call(DOUBLE, value),
        ctx.call(DOUBLE, value + 1),
    )
    return first + second


registry = Registry(example, double)


async def main() -> None:
    async with TestEnvironment(registry) as env:
        handle = await env.client.start(example, 5, request_id="demo-1")
        print(await env.run(handle))  # 22


if __name__ == "__main__":
    asyncio.run(main())
```

Workflow functions describe orchestration. Tasks perform API calls, database
writes, and other business effects. `ctx.call()` schedules a durable task;
calling the decorated function directly remains an ordinary Python call.

Run the included examples:

```bash
python examples/quickstart.py       # 22
python examples/broadcast_join.py  # publication receipt and payload 27
python -m examples.signals         # approval through a channel; result 14
```

The included examples require a [source checkout](guide/1_getting_started.md).

## Complete Docker Compose example

The [`docker-compose/`](docker-compose/README.md) folder includes PostgreSQL,
Apache Pulsar, and the three Duraflow execution services. DB/Pulsar environment
variables and container networking are preconfigured; no manual setup is needed.
From a source checkout:

```bash
docker compose -f docker-compose/compose.yaml up --build -d --wait --wait-timeout 300
docker compose -f docker-compose/compose.yaml run --rm client  # prints 22
docker compose -f docker-compose/compose.yaml down            # keeps journal/broker volumes
```

The image installs the package with its base dependencies. The on-demand client
executes a real message-driven workflow through Pulsar and PostgreSQL. See the
[Compose guide](docker-compose/README.md) for settings, restart tests, and cleanup.

## Connect DB and Pulsar

Use Python settings or environment variables. Both use the same runtime and
connection factories. Save the quick-start application above as `my_app.py` so
workers can import its `registry`.

### Configure in Python

Save this as `run_engine.py`:

```python
import asyncio
from duraflow import Runtime, RuntimeSettings

settings = RuntimeSettings(
    database_url="sqlite:///duraflow.db",
    task_journal_url="sqlite:///duraflow-tasks.db",
    broker_url="pulsar://localhost:6650",
    namespace="demo",
)
# PostgreSQL instead: database_url="postgresql+psycopg://user:password@localhost:5432/duraflow"
# Set task_journal_url to the task journal's PostgreSQL URL too.


async def main() -> None:
    runtime = Runtime(role="workflow-engine", settings=settings, app="my_app")
    await runtime.initialize()  # Prepare/check this role's journal; no broker connection.
    async with runtime:
        await runtime.run()  # Or run(stop=your_asyncio_event).


if __name__ == "__main__":
    asyncio.run(main())
```

Run `python run_engine.py`. Run separate processes with the same settings and
`role="workflow-worker"` and `role="task-worker"` to execute workflows and tasks.
Workflow workers use the importable `app` module in replay subprocesses. Tag
features additionally use `tag-engine` and `task-tag-engine` roles.

For a broker-only client, inside an async function:

```python
from my_app import example, registry

async with Runtime(role="client", settings=settings, registry=registry) as runtime:
    handle = await runtime.client.start(example, 5, request_id="demo-1")
    print(await handle.result(timeout=30))  # 22, with the three services running
```

`Runtime` owns and closes its connections. Construction does not connect; entering
the context prepares subscriptions. Initialize journals before starting services.
Client and workflow-worker roles never open the workflow DB. Python callers own
their signal handling; cancelling `run()` or setting its stop event shuts it down.

### Configure with environment variables

In each service terminal, set these variables (bash/zsh):

```bash
# SQLite: files are relative to each process's working directory.
export DURAFLOW_DATABASE_URL='sqlite:///duraflow.db'
export DURAFLOW_TASK_JOURNAL_URL='sqlite:///duraflow-tasks.db'
export DURAFLOW_PULSAR_URL='pulsar://localhost:6650'
export DURAFLOW_NAMESPACE='demo'

# Optional routing/schema settings; these are the defaults.
export DURAFLOW_PULSAR_TENANT='public'
export DURAFLOW_PULSAR_NAMESPACE='default'
export DURAFLOW_MESSAGE_SCHEMA='duraflow_messages'
```

For PostgreSQL, replace the two DB variables:

```bash
export DURAFLOW_DATABASE_URL='postgresql+psycopg://duraflow:development-only@localhost:5432/duraflow'
export DURAFLOW_TASK_JOURNAL_URL="$DURAFLOW_DATABASE_URL"
```

The same PostgreSQL server may host both journals: task workers use the separate
`duraflow_messages_tasks` schema by default. SQLite ignores schemas; use separate
files. For an absolute SQLite path use `sqlite:////absolute/path/duraflow.db`.

Initialize once, then run each service in its own terminal:

```bash
python -m duraflow init
python -m duraflow --database "$DURAFLOW_TASK_JOURNAL_URL" --schema duraflow_messages_tasks init

python -m duraflow --app my_app workflow-engine
python -m duraflow --app my_app workflow-worker
python -m duraflow --app my_app task-worker
```

With the services running, submit the example from another configured terminal:

```bash
python -m duraflow --app my_app start example:v1 --input 5 --request-id demo-1
python -m duraflow --app my_app result demo-1 --timeout 30
```

Python can read the same variables explicitly:

```python
settings = RuntimeSettings.from_environment(role="workflow-engine")
# Explicit arguments override environment values:
settings = RuntimeSettings.from_environment(role="workflow-engine", namespace="another-app")
```

Direct `RuntimeSettings(...)` construction does **not** read environment variables.
Environment loading uses **explicit overrides > environment > defaults**; CLI
options override environment values. The optional `role` argument skips unrelated
DB secrets. `DURAFLOW_DATABASE_URL_FILE`, `DURAFLOW_TASK_JOURNAL_URL_FILE`, and
`DURAFLOW_PULSAR_TOKEN_FILE` support secret files. TLS settings and the full
configuration table are in [operations](guide/6_operations.md).

### Why do the distributed guides use PostgreSQL?

**SQLite is supported**, including durable state, task journals, and restart
recovery. PostgreSQL is the recommended distributed backend:

| Backend | Intended use and behavior |
| --- | --- |
| SQLite | Local/single-host execution; WAL and `BEGIN IMMEDIATE`; one writer at a time |
| PostgreSQL | Distributed services and concurrent writers; row locks, `SKIP LOCKED`, and DB-server time |

The existing PostgreSQL adapter uses **SQLAlchemy Core**, PostgreSQL `JSONB`,
schemas, and advisory locks. SQLAlchemy supports many databases, but cannot make
their locking, clock, and transaction semantics identical. Other SQLAlchemy
dialects require an adapter and recovery tests; changing the URL alone is not
enough. The SQLite adapter uses `sqlite3` directly behind the same `MessageStore`
interface.

The explicit `production=True` / `DURAFLOW_PRODUCTION=true` policy currently
requires verified PostgreSQL TLS for DB-backed roles and authenticated Pulsar TLS.
SQLite works with the default `production=False`; that policy restriction is
separate from SQLite storage support. See [distributed services](guide/5_distributed.md)
for infrastructure setup and independent role deployment.

## Signal channels

Register reception before sending a request that may generate an immediate reply:

```python
from duraflow import ChannelRef

APPROVAL = ChannelRef("approval", bool)

# Inside a workflow:
approvals = ctx.channel(APPROVAL).receive(max_signals=1)
await ctx.call(SEND_APPROVAL_REQUEST, order)
approved = await approvals.next(timeout=3600)

# From a client holding this workflow's handle:
await handle.signal(APPROVAL, True, signal_id="approval-42")
```

`receive()` registers a durable stream without suspending the workflow. `next()`
waits for a value. Matching signals received after registration are buffered even
before `next()`; signals before registration are discarded. See the
[signal guide](guide/3_signals.md) for filters, tags, and replay semantics.

## Internal architecture

Duraflow follows Infinitic's message-driven component boundaries with an
independently authored Python API and JSON protocol.

```mermaid
flowchart LR
    C[Client] -->|Start / signal / query| A[Workflow command topic]
    A --> E
    I[Workflow event inbox] --> E[WorkflowEngine]
    E <--> S[(Workflow state + inbox + outbox)]
    E -->|Activation via outbox| Q[Workflow execution topic]
    Q --> W[WorkflowWorker]
    W -->|Replay decision| I
    E -->|Task via outbox| T[Task execution topic]
    T --> R[TaskWorker]
    R <--> J[(Independent task journal)]
    R -->|Task result via outbox| I
    E -->|Delayed timer| D[Timer topic]
    D --> I
    E -->|Response via outbox| P[Client response topic]
    P --> C
```

**The loop:** start → replay → task → result → replay → next step → completion.
Signals and timers enter the same workflow state transition path. Normal
execution advances by consuming messages; workflow records are not scanned for
progress.

| Component | Responsibility |
| --- | --- |
| `Client` | Publish commands and consume responses; no workflow DB connection |
| `WorkflowEngine` | Own workflow state, accept events, resolve waits, and commit outgoing intent |
| `WorkflowWorker` | Replay immutable snapshots using the pinned implementation |
| `TaskWorker` | Execute business functions and publish results using its own execution journal |
| `TagEngine` / `TaskTagEngine` | Maintain separate indexes and perform resumable tag fan-out |
| `OutboxRelay` | Publish committed messages and recover interrupted sends |

Completed task results are injected during replay. External effects can run again
after an effect-before-result crash; use `TaskContext.idempotency_key` to make
those effects idempotent. The [architecture guide](guide/8_architecture.md)
details topic routing, transactional boundaries, ordering, and recovery.

## Guides

| # | Guide | Contents |
| --- | --- | --- |
| 0 | [Guide index](guide/0_index.md) | Reading order and terminology |
| 1 | [Getting started](guide/1_getting_started.md) | Installation and local examples |
| 2 | [Writing workflows](guide/2_workflows.md) | Calls, futures, retries, children, and replay |
| 3 | [Signals and channels](guide/3_signals.md) | Registration, filtering, repetition, and tag delivery |
| 4 | [Broadcast and join](guide/4_broadcast_and_join.md) | Multi-handler event processing |
| 5 | [Distributed services](guide/5_distributed.md) | Independent runtime processes and broker-only clients |
| 6 | [Operations](guide/6_operations.md) | Configuration, monitoring, controls, and protocol cutover |
| 7 | [Testing and contributing](guide/7_testing.md) | Unit, native, crash-recovery, and package checks |
| 8 | [Internal architecture](guide/8_architecture.md) | Components, topics, execution flow, and failure semantics |

## Contributing

Bug reports, documentation improvements, and pull requests are welcome.
Include a reproducible case when [reporting an issue](https://github.com/hynlab/duraflow/issues).

```bash
python -m pip install -e '.[dev]'
make check
```

See the [testing guide](guide/7_testing.md) for native integration and recovery checks.

## License

Duraflow is licensed under the [Apache License 2.0](LICENSE).
See [PROVENANCE.md](PROVENANCE.md) for implementation provenance.
