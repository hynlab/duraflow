# Getting started

[Guide index](README.md) · Next: [Writing workflows](workflows.md)

## Requirements

- Python 3.12 or newer. CI exercises Python 3.12 and 3.13.
- Git to install the current repository version.
- Docker Compose only when running the distributed examples or native tests.

The local examples use in-memory storage and transport, so you can start without
PostgreSQL or Pulsar. The Pulsar adapter also requires a compatible
`pulsar-client` wheel for your Python version and platform.

## Install

```bash
git clone https://github.com/hynlab/duraflow.git
cd duraflow
python -m venv .venv
source .venv/bin/activate
python -m pip install -e .
```

On Windows, activate the environment with `.venv\Scripts\Activate.ps1` in
PowerShell. The distributed process examples and signal-based test suite use
POSIX facilities; use Linux or WSL for those commands.

Choose extras as needed:

| Installation | Includes |
| --- | --- |
| `python -m pip install -e .` | Core API and local test environment |
| `python -m pip install -e '.[postgres]'` | PostgreSQL storage adapter |
| `python -m pip install -e '.[pulsar]'` | Apache Pulsar transport |
| `python -m pip install -e '.[dev,postgres,pulsar]'` | Development tools and both adapters |

## Run the first example

From the repository root:

```bash
python examples/quickstart.py
```

Expected output:

```text
22
```

The [example source](../examples/quickstart.py) defines a typed `double` task and
a workflow that calls it twice in parallel. Its execution has four parts:

1. `TaskRef("double", int, int)` declares the task's name and types.
2. `@task(ref=DOUBLE)` registers the implementation metadata.
3. `ctx.gather()` waits for two durable `ctx.call()` operations.
4. `TestEnvironment` drives the engine and worker until the result is available.

Add both implementations to a `Registry`; the test environment uses that registry
to resolve the workflow and execute its tasks.

## Start identity and results

Inside an async function with a configured `client`:

```python
handle = await client.start(
    example,
    5,
    request_id="calculation-42",
    workflow_id="calculation-42",
    tags=("calculations",),
)
state = await handle.describe()
result = await handle.result(timeout=30)
```

Here `example` is the workflow from the quick start. A running engine and worker
must make progress while `result()` waits. In `TestEnvironment`, use
`await env.run(handle)` to drive them instead.

Repeating a start with the same request ID and identical start parameters returns
the existing run. Reusing the identity with different input raises `Conflict`.
Use a new request ID and workflow ID for an intentional new execution.

The timeout on `result()` limits the caller's wait; it does not cancel the workflow.

## Try an event-driven workflow

```bash
python examples/broadcast_join.py
```

This example sends an integer to three handlers, joins their results, and
publishes the sum. With input `7`, the final payload is `27`. The publication
receipt is generated for the run and will vary.

See [Broadcast and join](broadcast-and-join.md) for how participants and worker
subscriptions are declared.

## Choose your runtime

| Runtime | Use |
| --- | --- |
| `TestEnvironment` | Fast, deterministic local examples and tests |
| `SQLiteStore` | File-backed development and recovery tests |
| `PostgresStore` + `PulsarTransport` | Separate engine and worker processes |

`TestEnvironment` defaults to memory: its state does not survive process exit.
Continue with [Running distributed services](distributed.md) to run a persistent
workflow across independent processes.
