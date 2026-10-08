# 3. Signals and channels

[Index](0_index.md) · Previous: [Writing workflows](2_workflows.md)

Signals carry external information to workflows: an approval, shipment update,
payment callback, or user action. They travel through Pulsar into the workflow
engine; callers do not mutate workflow storage.

## Declare a typed channel

```python
from duraflow import ChannelRef

APPROVAL = ChannelRef("approval", bool)
```

Place channels in a shared contracts module. Export `signals = {"approval": APPROVAL}`
from the application or contracts module for CLI use.

## Register first, wait later

```python
@workflow(name="order", build_id="order-v1")
async def order(ctx: WorkflowContext, value: Order) -> Receipt:
    approvals = ctx.channel(APPROVAL).receive(max_signals=1)
    await ctx.call(SEND_APPROVAL_REQUEST, value)
    approved = await approvals.next(timeout=3600)
    return await ctx.call(FULFILL if approved else REJECT, value)
```

This orchestration fragment assumes the typed business contracts are declared.
The [runnable example](../examples/signals.py) uses the same registration pattern.

`receive()` is non-blocking. The replay worker records registration before the
external request task in the same command segment. The engine commits reception
before dispatch, so an immediate response can be buffered while the task runs.

### Reception rules

| Signal arrival | Behavior |
| --- | --- |
| Before the workflow exists | An acknowledged signal request returns `NotFound` |
| Before channel reception is registered | Discarded, with a recorded discard event |
| After registration, before `next()` | Buffered durably |
| While waiting in `next()` | Consumed to resolve that wait |
| Beyond `max_signals` | Discarded for that stream |
| After workflow termination | Rejected |

Events arriving while replay is in flight are persisted, then applied after that
activation's ordered registrations and commands have committed.

## Send by logical workflow identity

```python
from duraflow import Client
from examples.signal_contracts import APPROVAL, ORDER

client = Client(transport)
handle = client.get_handle(ORDER, "order-42")
await handle.signal(APPROVAL, True, signal_id="approval-42")
```

Run this inside an async function with a configured transport. The sender needs
only the contracts and broker credentials. Targeting the logical workflow follows
rollover; supplying `run_id=` to `get_handle()` pins a specific history.

Repeating the same signal ID and content is idempotent. Reusing the ID with a
different channel, schema, or payload conflicts. IDs are retained through rollover.

## Receive repeatedly

```python
events = ctx.channel(EVENTS).receive(max_signals=3)
first = await events.next()
second = await events.next()
third = await events.next()
```

`max_signals` limits accepted values, not the number of attempted reads. A read
that times out does not consume a later signal. Reads consume the next available
matching value and record which command consumed it, so replay does not read twice.

## Filter payloads

Declare serializable predicates rather than executable callbacks:

```python
from duraflow import ChannelRef, SignalFilter

EVENTS = ChannelRef("events", dict[str, int])
events = ctx.channel(EVENTS).receive(
    max_signals=2,
    filter=SignalFilter(equals={"account": 42}),
)
event = await events.next()
```

`equals` supports dotted paths into JSON objects. Comparisons preserve JSON types.
Unmatched values are not buffered for this registration. This Python API uses
declarative equality predicates rather than Infinitic's JVM JSONPath syntax.

For a channel supporting several types, select an accepted schema:

```python
MIXED = ChannelRef("mixed", int | str)
strings = ctx.channel(MIXED).receive(payload_type=str, max_signals=1)
# Sender explicitly identifies the selected schema:
await handle.signal(MIXED, "accepted", signal_id="string-1", payload_type=str)
```

## Send to a tag

Start workflows with `tags=("orders",)` and run a `TagEngine`:

```python
await client.signal_tagged(ORDER, "orders", APPROVAL, True, signal_id="approve-batch-42")
```

The tag engine snapshots its matching identities and sends in bounded pages.
Continuation messages survive a restart. A repeated batch signal ID cannot expand
the original snapshot or apply a value twice. Membership is indexed by workflow
type and tag. `start()` waits for tag registration; `dispatch()` waits only for
broker acceptance. Neither response guarantees that user code has reached `receive()`.

## Timers and cancellation

`next(timeout=...)` reserves a delayed timer message. The engine records the
accepted signal/timeout order. It refuses early timer delivery and leaves it for
redelivery rather than ACKing it away. `SIGNAL_TIMEOUT` is raised as `TaskFailure`
inside the workflow; callers see workflow failure if it is not handled.

Cancel or terminate requests go through the operator topic. Channels, buffered
values, and consumed-command mappings are persisted across engine and executor
restarts. Waiting requires no retained workflow coroutine or dedicated thread.

Next: [Broadcast and join](4_broadcast_and_join.md).
