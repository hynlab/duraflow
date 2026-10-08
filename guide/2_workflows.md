# 2. Writing workflows

[Index](0_index.md) · Previous: [Getting started](1_getting_started.md)

## Contracts and implementations

Keep contracts in a module that clients and workflow code can import without
importing service implementations:

```python
from duraflow import TaskRef, WorkflowRef

NORMALIZE = TaskRef("normalize", str, str)
NORMALIZE_NAME = WorkflowRef("normalize-name", str, str, build_id="normalize-v1")
```

Implement a task and workflow in their respective worker applications:

```python
from duraflow import Registry, WorkflowContext, task, workflow


@task(ref=NORMALIZE)
def normalize(value: str) -> str:
    return value.strip().lower()


@workflow(name="normalize-name", build_id="normalize-v1")
async def normalize_name(ctx: WorkflowContext, value: str) -> str:
    return await ctx.call(NORMALIZE, value)


registry = Registry(normalize_name, normalize)
```

The two snippets belong in an application that imports the contracts. A distributed
client can use `Client(transport)` and `client.start(NORMALIZE_NAME, ...)` with
no implementation registry or workflow DB connection.

Inputs and outputs use typed JSON contracts. Use serializable values, dataclasses,
or supported Pydantic models. Each inline application payload is limited to 256 KiB.

## Deterministic orchestration

Each workflow activation reconstructs the coroutine and supplies recorded results
at durable boundaries. Write replay-safe code:

- Put HTTP, database writes, filesystem access, and effects in tasks.
- Use `await ctx.now()` and `await ctx.uuid()` for recorded values.
- Use workflow operations for concurrency and waits, rather than arbitrary asyncio tasks.
- Keep changing environment values and nondeterministic globals out of decisions.
- Keep durable operations out of suspended finalizers and async cleanup.

Set an immutable `build_id` covering imported helper code as well as the function.
Workflow execution topics include its build identity. Deploy compatible old and
new workers on their own build routes until earlier executions finish.

## Calls, futures, and parallel work

Inside a workflow:

```python
first, second = await ctx.gather(
    ctx.call(NORMALIZE, value),
    ctx.call(NORMALIZE, "another name"),
)
```

To dispatch without immediately waiting, use a durable `Future`:

```python
pending = ctx.dispatch(NORMALIZE, value)
timer = ctx.timer(10)
await timer
normalized = await pending
```

Registrations are collected in code order and committed before their messages
are published. `Future.result()` produces a wait operation, and futures can be
passed to `ctx.gather()` or `ctx.race()`. Ordinary cold operation descriptors
remain single-use; `gather` requires distinct operations.

`ctx.race()` returns `RaceResult(index, value)` for the first accepted outcome.
An accepted failure is raised. Losing task operations continue; a timer winning
does not undo their business effects. Nested cold groups remain unsupported.

## Retries and deadlines

```python
from duraflow import RetryPolicy, TaskOptions

options = TaskOptions(
    retry=RetryPolicy(max_attempts=3, delay=1, multiplier=2, max_delay=10),
    schedule_timeout=30,
    attempt_timeout=60,
    overall_timeout=180,
)
```

Pass `options=options` to `ctx.call()` or `ctx.dispatch()`.

| Setting | Meaning |
| --- | --- |
| `max_attempts` | Application attempts, including the first; default 1 |
| `retry_codes` | Default: `TASK_ERROR`, `ATTEMPT_TIMEOUT` |
| `schedule_timeout` | Eligible attempt to execution start |
| `attempt_timeout` | Time from execution start |
| `overall_timeout` | Invocation budget across retries |
| `exhausted="block"` | Wait for operator repair and retry |

Durations are positive seconds. Application retries are delayed execution messages;
broker redelivery and lease takeover do not spend the application retry count.
Catch `TaskFailure` for explicit business compensation or alternative paths.

## Task effects and context

A task can accept `(context, input)` instead of just `(input)`. Its context exposes:

- `idempotency_key`: stable across retries and redelivery of the logical invocation.
- `heartbeat(progress)`: renew the service-owned execution lease and record progress.
- `check_cancelled()`: check cooperative cancellation.
- `defer(timeout=...)`: delegate completion and return the resulting `Deferred` marker.

An external executor completes a delegated task with
`client.complete_external(token, value, ref=TASK)`. Tokens are bearer secrets.
The task journal validates their hash, generation, expiry, and repeated outcomes.

For service tagging, use `ctx.call(TASK, value, tags=("campaign",))` or the equivalent
`dispatch`. A `TaskTagEngine` indexes these attempts; a client can issue
`cancel_tasks_tagged(TASK, "campaign", request_id="stop-42")`.

## Children and rollover

Declare a child `WorkflowRef` with an explicit `build_id` to avoid requiring its
implementation in the parent's registry:

```python
from duraflow import WorkflowRef

CHILD = WorkflowRef("child-flow", int, int, build_id="child-v1")
# Inside the parent workflow:
result = await ctx.child(CHILD, 7)
```

The child has independent state and an idempotent start identity. Parent closure
requests cancellation unless `abandon_on_parent_close=True`.

`await ctx.continue_as_new(next_input)` creates a new run under the same logical
ID. Resolve outstanding operations first. Compatible unconsumed channel values
are carried to matching registrations in the successor. A handle created with
`client.get_handle(CONTRACT, workflow_id)` targets the current logical run;
`result()` follows rollover by default.

Next: [Signals and channels](3_signals.md).
