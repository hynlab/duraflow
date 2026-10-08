# Writing workflows

[Guide index](README.md) · Previous: [Getting started](getting-started.md)

## Separate orchestration from work

A workflow is an async function with `(context, input)` parameters. A task can be
synchronous or asynchronous and accepts either `(input)` or `(context, input)`.

```python
from duraflow import Registry, TaskRef, WorkflowContext, task, workflow

NORMALIZE = TaskRef("normalize", str, str)


@task(ref=NORMALIZE)
def normalize(value: str) -> str:
    return value.strip().lower()


@workflow(name="normalize-name", version=1, build_id="normalize-name-v1")
async def normalize_name(ctx: WorkflowContext, value: str) -> str:
    return await ctx.call(NORMALIZE, value)


registry = Registry(normalize_name, normalize)
```

Task references describe contracts, so workflow code does not need to import
worker implementations. Larger applications can keep references in a shared
contracts module and deploy workflow and task implementations separately.

Inputs and outputs are validated against the declared types and encoded as JSON.
Use serializable values, dataclasses, or supported Pydantic models; do not pass
connections, file handles, or arbitrary Python objects between tasks.

## Replay rules

Duraflow re-executes workflow code from the beginning and supplies previously
recorded results at durable operations. Keep orchestration deterministic:

- Put HTTP calls, database writes, filesystem access, and other effects in tasks.
- Use `await ctx.now()` and `await ctx.uuid()` for recorded time and UUID values.
- Use `ctx.gather()` and `ctx.race()` instead of ordinary asyncio concurrency.
- Use `ctx.sleep()` instead of `asyncio.sleep()` in workflows.
- Avoid decisions based on changing globals, environment variables, or randomness.
- Do not put durable operations in suspended `finally` blocks or async cleanup.

An operation descriptor is single-use. Groups accept distinct, unused operations
from the same context. Nested groups and `continue_as_new` inside groups are not
supported; compose more complex work with child workflows.

Set an explicit, immutable `build_id` that accounts for imported helpers as well
as the workflow function. Keep matching old implementations running while their
workflows finish. An engine without the pinned implementation leaves a run for a
compatible engine; changed command history under the same identity blocks replay.

## Parallel work and races

Within the workflow above:

```python
first, second = await ctx.gather(
    ctx.call(NORMALIZE, value),
    ctx.call(NORMALIZE, "another name"),
)
```

Results follow declaration order. To select the first committed outcome:

```python
winner = await ctx.race(ctx.call(NORMALIZE, value), ctx.sleep(30))
if winner.index == 0:
    return winner.value
return "timed out waiting"
```

Race losers keep running. The timer winning does not cancel the task or undo
external effects. A winning failure is raised rather than returned as a value.

## Retries and deadlines

Configure a call with `TaskOptions` and `RetryPolicy`:

```python
from duraflow import RetryPolicy, TaskOptions

options = TaskOptions(
    retry=RetryPolicy(max_attempts=3, delay=1, multiplier=2, max_delay=10),
    schedule_timeout=30,
    attempt_timeout=60,
    overall_timeout=180,
)
```

Pass it as `await ctx.call(NORMALIZE, value, options=options)`.

| Setting | Meaning |
| --- | --- |
| `max_attempts` | Total application attempts, including the first; defaults to 1 |
| `retry_codes` | Retryable error codes; defaults to `TASK_ERROR` and `ATTEMPT_TIMEOUT` |
| `schedule_timeout` | Time from an attempt becoming eligible to its first claim |
| `attempt_timeout` | Time from the attempt's first claim |
| `overall_timeout` | Time budget for the logical invocation, across retries |
| `exhausted="block"` | Keep an exhausted invocation blocked for operator action |

Durations are positive seconds. Broker redelivery is distinct from an application
retry: a lost worker can cause another worker to claim the same attempt.

Catch `TaskFailure` inside a workflow when you need explicit compensation or an
alternative business path. Run compensating actions as tasks, with their own
idempotency handling.

## Idempotent task effects

A task accepting a context receives a `TaskContext`. Its `idempotency_key` remains
stable across retries and redeliveries of that invocation. Pass it to an external
service's idempotency mechanism or enforce uniqueness in your business database.

Task result persistence and an external API transaction are separate: a worker
can crash after the effect succeeds but before its result is recorded. Duraflow
does not make arbitrary external effects exactly-once.

Async tasks can report progress with `await ctx.heartbeat(50)` and check for
cooperative cancellation with `ctx.check_cancelled()`. Cancellation does not
forcibly stop an arbitrary Python thread or reverse an already committed effect.

## Timers and signals

This complete application waits for a typed approval signal:

```python
from duraflow import Registry, SignalRef, WorkflowContext, workflow

APPROVED = SignalRef("approved", bool)


@workflow(name="approval", build_id="approval-v1")
async def approval(ctx: WorkflowContext, order_id: str) -> bool:
    return await ctx.wait_signal(APPROVED, timeout=3600)


registry = Registry(approval)
signals = {"approved": APPROVED}
```

From a client, call `await handle.signal(APPROVED, True, signal_id="approval-42")`.
Signals can arrive before the workflow starts waiting. Repeating the same signal
ID and payload is idempotent; changing the payload under that ID is a conflict.
Export `signals` from an application module to use the CLI's `signal` command.

Use `await ctx.sleep(60)` for a persistent timer. The process need not stay alive
for the timer's duration; an engine reconciles it from durable state.

## Children and rollover

Declare a child contract with `WorkflowRef`, register its implementation, and
call `await ctx.child(CHILD, value)`. The parent waits on a separate durable
history. Parent closure requests child cancellation unless the call explicitly
sets `abandon_on_parent_close=True`.

For a long-running workflow, `await ctx.continue_as_new(next_input)` creates a
new run under the same logical workflow ID and carries forward unconsumed
signals. Finish outstanding operations before rollover.

Use `await client.current(workflow_id)` to find the current run,
`await client.signal_workflow(workflow_id, ref, value, signal_id=...)` to signal
across rollover, and `await handle.result(follow_continued=True)` to follow the
result chain.

Next: [Broadcast and join](broadcast-and-join.md).
