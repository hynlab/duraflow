# 1. Getting started

[Index](0_index.md) · Next: [Writing workflows](2_workflows.md)

## Install the current source

Use Python 3.12 or newer. CI also exercises Python 3.13.

```bash
git clone https://github.com/hynlab/duraflow.git
cd duraflow
python -m venv .venv
source .venv/bin/activate
python -m pip install -e .
```

PowerShell activation is `.venv\Scripts\Activate.ps1`. Distributed process and
SIGKILL tests use POSIX process facilities; use Linux or WSL for those tests.

| Extras | Installation |
| --- | --- |
| PostgreSQL | `python -m pip install -e '.[postgres]'` |
| Pulsar | `python -m pip install -e '.[pulsar]'` |
| Development and both adapters | `python -m pip install -e '.[dev,postgres,pulsar]'` |

The Pulsar adapter needs a compatible `pulsar-client` wheel for your platform.

## Run the examples

From the repository root:

```bash
python examples/quickstart.py
python examples/broadcast_join.py
python -m examples.signals
```

Expected results are `22`, an event publication with final payload `27`, and
an approval workflow result of `14`, respectively. Publication receipt IDs vary.

These examples use `TestEnvironment`: a retained memory broker, virtual clock,
workflow state engine, workflow worker, task worker, and tag engines. They execute
the production message handlers in background tasks.

## Understand the basic application

```python
from duraflow import Registry, TaskRef, WorkflowContext, task, workflow

DOUBLE = TaskRef("double", int, int)


@task(ref=DOUBLE)
def double(value: int) -> int:
    return value * 2


@workflow(name="double-flow", build_id="double-flow-v1")
async def double_flow(ctx: WorkflowContext, value: int) -> int:
    return await ctx.call(DOUBLE, value)


registry = Registry(double_flow, double)
```

- `TaskRef` declares the task's stable name, version, and input/output types.
- `@task` associates that contract with an implementation.
- `ctx.call()` creates an operation; a task worker executes the function.
- `@workflow` pins the orchestration implementation with `build_id`.
- A `Registry` supplies implementations to workers and local tests.

To run it in a test, place this inside an async function:

```python
from duraflow.testing import TestEnvironment

async with TestEnvironment(registry) as env:
    handle = await env.client.start(double_flow, 7, request_id="example-1")
    assert await env.run(handle) == 14
```

## Dispatch versus accepted start

- `client.dispatch(...)` returns after the broker accepts the start command.
- `client.start(...)` additionally waits for the engine's acceptance response.
- `handle.result(timeout=...)` waits for a completion response through the broker.

Start idempotency is scoped to the workflow type and logical workflow ID. Within
that identity, repeating identical start parameters and request ID returns the
same run; changing the input or reusing the identity for another start conflicts.
Use a new logical identity for an intentional new execution.

A result timeout limits your wait and removes the result waiter; it does not
cancel the workflow. `describe()` and `history()` are request/response operations.

## Select your environment

| Environment | Purpose |
| --- | --- |
| `TestEnvironment` | Local examples and deterministic tests |
| `SQLiteMessageStore` | File-backed local state or task journals |
| `PostgresMessageStore` + `PulsarTransport` | Independent distributed processes |

Memory state disappears when its process exits. Continue with
[distributed services](5_distributed.md) for persistent workflows.
