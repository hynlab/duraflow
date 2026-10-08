# 4. Broadcast and join

[Index](0_index.md) · Previous: [Signals](3_signals.md)

Use a broadcast when one business event requires multiple named participants
before the workflow continues. The complete application is
[`examples/application.py`](../examples/application.py).

## Define the participants

```python
from duraflow import HandlerRef, TaskRef, TopicRef

VALUE = TopicRef("persistent://public/default/example-product", int)
OUTPUT = TopicRef("persistent://public/default/example-analyzed", int)
PRICE, SHIPPING, DEMAND = (
    TaskRef(label, int, int) for label in ("price", "shipping", "demand")
)
HANDLERS = tuple(HandlerRef(ref, ref.name) for ref in (PRICE, SHIPPING, DEMAND))
```

Handlers pair task contracts with distinct logical subscription names. Their input
types must match the business topic payload type.

## Orchestrate the join

```python
from duraflow import WorkflowContext, workflow


@workflow(name="product", build_id="example-release-1")
async def product(ctx: WorkflowContext, value: int) -> str:
    results = await ctx.broadcast(VALUE, value, handlers=HANDLERS)
    return await ctx.publish(OUTPUT, sum(results[handler] for handler in HANDLERS))
```

The engine persists the participant set before publishing one event. It provisions
each subscription before publication, even when its worker is offline.

```text
example-product
  ├─ df2-demo-price    → price task worker    → task_result ─┐
  ├─ df2-demo-shipping → shipping task worker → task_result ─┼→ workflow inbox → join
  └─ df2-demo-demand   → demand task worker   → task_result ─┘
```

Distinct subscriptions receive separate copies. Replicas of one handler share its
subscription. Multiple completions from the same participant count once.

## Bind task workers

An application exports its implementations and subscriptions:

```python
from duraflow import BroadcastBinding, Registry

# Implementations are defined in examples/application.py.
registry = Registry(product, price, shipping, demand)
broadcasts = tuple(BroadcastBinding(VALUE, handler) for handler in HANDLERS)
```

The task-worker CLI reads `broadcasts`. Independent applications can register only
the tasks and bindings they own. The engine handles the results as incoming
messages, rather than discovering them through a DB scan.

## Retry and publication semantics

- Successful results are retained when another participant retries.
- Application retries use the failed participant's direct task execution topic.
- Broker redelivery is distinct from an application retry.
- Business functions use their stable task idempotency key for external effects.
- `ctx.publish()` completes after broker acceptance and the publication confirmation
  is processed; it does not await downstream business processing.

Business topic payloads remain JSON bytes. Duraflow metadata is carried in the
reserved `duraflow-v2` property. Existing schema-managed topics require a compatible
codec integration.

```bash
python examples/broadcast_join.py
```

For input `7`, the final payload is `27`. Continue with
[distributed services](5_distributed.md) to run independent processes.
