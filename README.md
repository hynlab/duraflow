# Duraflow

An independent **Apache-2.0** Python durable workflow engine. Write orchestration
as ordinary `async def` functions; execute business functions through typed task
references. Completed results are recorded and replayed after a restart.

**Status: 0.1.0a1, alpha.** Core behavior and file-backed process recovery are
tested. Native PostgreSQL/Pulsar qualification is a separate integration gate;
this is not a claim of production readiness or exactly-once external effects.
See [implementation status](docs/implementation_status.md).

## Install from this repository

Python 3.12 or newer is required. Native broker wheels have their own platform
support; the integration baseline is Linux x86_64 / Python 3.12.

```bash
python -m pip install -e '.[dev,postgres,pulsar]'
python examples/quickstart.py
python examples/broadcast_join.py
make test
```

No PyPI publication or package-name ownership is implied by the project name.

## A workflow

```python
from duraflow import Registry, TaskRef, WorkflowContext, task, workflow

DOUBLE = TaskRef("double", int, int)

@task(ref=DOUBLE)
def double(value: int) -> int:
    return value * 2

@workflow(name="example", version=1, build_id="example-release-1")
async def example(ctx: WorkflowContext, value: int) -> int:
    first, second = await ctx.gather(
        ctx.call(DOUBLE, value),
        ctx.call(DOUBLE, value + 1),
    )
    return first + second

registry = Registry(example, double)
```

Calling `double(2)` remains an ordinary local function call. Only
`ctx.call(DOUBLE, 2)` denotes remote work. A workflow imports contracts, not worker
implementations. `build_id` must identify the complete immutable implementation,
including helper code; a source-derived default cannot detect changed imports.

## One message, three subscriptions, one join

```python
results = await ctx.broadcast(
    PRODUCT_READY,
    product,
    handlers=(PRICE_HANDLER, SHIPPING_HANDLER, DEMAND_HANDLER),
)
await ctx.publish(READY_FOR_ANALYSIS, product)
```

The declared participant set is saved before publishing. Three duplicate
completions from the price handler still count as **one** participant. The worker
runner handles completion reporting automatically. Only the failed handler is
retried; successful participants are not broadcast again. Handler subscriptions
are namespace-scoped, while replicas of one handler share its subscription.

## Included capabilities

- Deterministic coroutine replay, version/codec pinning, typed boundaries,
  sequential calls, parallel joins, race, and recorded time/UUID values.
- Transactional starts, run revisions, inbox/outbox, fenced execution leases,
  automatic results, retry/deadline policies and stable business idempotency keys.
- Persistent timers, buffered typed signals, audited cancel/terminate/resume,
  child workflows, execution rollover, tags and delegated external completion.
- Memory, SQLite development/recovery, PostgreSQL adapters; memory and official
  Pulsar transports; SDK, CLI, examples, CI and fault/recovery tests.

## Run distributed processes

```bash
docker compose up -d
export DURAFLOW_DATABASE_URL='postgresql+psycopg://duraflow:development-only@localhost:5432/duraflow'
export DURAFLOW_PULSAR_URL='pulsar://localhost:6650'
python -m duraflow --app examples.application init
# Separate terminals/processes:
python -m duraflow --app examples.application engine
python -m duraflow --app examples.application worker
python -m duraflow --app examples.application start product:v1 --input '7' --request-id example-1
```

The compose environment is **local development only**, with loopback ports and
an explicit development password. Production needs TLS/authentication, backups,
supervision and infrastructure availability engineering.

## Important boundaries

Workflow code is trusted, deterministic orchestration: no HTTP, database access,
live environment decisions, ordinary asyncio concurrency, or external effects.
Put those in tasks. Durable operations spanning a suspended `finally` or async
context-manager exit are unsupported and rejected when encountered. Python code
is not sandboxed; a pure infinite loop can still block an engine process.

External effects may run again after a crash between the effect and result
persistence. Pass `TaskContext.idempotency_key` to systems that support it.
Cancellation cannot kill arbitrary Python threads or undo already performed
side effects. `publish` confirms broker acceptance, not consumer business success.

The alpha PostgreSQL implementation commits **one JSON aggregate per run**, with
CAS revisions. This simplifies atomic inbox/history/outbox updates, but has JSON
write amplification and scan-based reconciliation. It is not a high-throughput
claim, and materialized state does not replace replay history.

## Documentation

[API and execution semantics](docs/architecture.md) ·
[Operations and recovery](docs/operations.md) ·
[Acceptance and release status](docs/implementation_status.md) ·
[Original planning baseline](docs/specification.md) ·
[Independent implementation policy](PROVENANCE.md)

Infinitic was a high-level architectural reference only. This repository is not
an official Python edition and does not promise JVM wire/storage compatibility.
