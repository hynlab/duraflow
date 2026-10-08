# Broadcast and join

[Guide index](README.md) · Previous: [Writing workflows](workflows.md)

Use a broadcast when one event must be processed by several named handlers
before a workflow can continue. For example, a product can require pricing,
shipping, and demand calculations before an analysis event is published.

## Declare participants

The complete application lives in [`examples/application.py`](../examples/application.py).
Its contracts are:

```python
from duraflow import HandlerRef, TaskRef, TopicRef

VALUE = TopicRef("persistent://public/default/example-product", int)
OUTPUT = TopicRef("persistent://public/default/example-analyzed", int)
PRICE, SHIPPING, DEMAND = (
    TaskRef(label, int, int) for label in ("price", "shipping", "demand")
)
HANDLERS = tuple(HandlerRef(ref, ref.name) for ref in (PRICE, SHIPPING, DEMAND))
```

Each `HandlerRef` pairs a task contract with a logical subscription name. Names
must be distinct within a broadcast, and each task input type must match the
topic's payload type.

## Join the results

The workflow declares all required participants before publishing:

```python
from duraflow import WorkflowContext, workflow


@workflow(name="product", version=1, build_id="example-release-1")
async def product(ctx: WorkflowContext, value: int) -> str:
    result = await ctx.broadcast(VALUE, value, handlers=HANDLERS)
    return await ctx.publish(OUTPUT, sum(result[handler] for handler in HANDLERS))
```

The returned `BroadcastResult` is indexed by `HandlerRef`. Three completions from
the price handler count as one participant, not as completion of all three.
The workflow waits for the declared set, regardless of which workers happen to
be connected when the event is sent.

`ctx.publish()` returns a publication receipt after broker acceptance. It does
not wait for downstream business processing. Use a broadcast when that join is
part of the workflow.

## Connect workers

Register task implementations and export their bindings from the app module:

```python
from duraflow import BroadcastBinding, Registry

# price, shipping, and demand are the @task functions in examples/application.py.
registry = Registry(product, price, shipping, demand)
broadcasts = tuple(BroadcastBinding(VALUE, handler) for handler in HANDLERS)
```

The CLI worker reads this `broadcasts` tuple. A single worker can host all
handlers, or separate applications can register only the tasks and bindings
they own. Replicas of one handler share its namespace-scoped subscription;
different handlers receive separate copies of the event.

## Failure and recovery behavior

- The participant set and dispatch intent are saved before publication.
- Workers persist results automatically; task functions do not report join progress.
- Application retries target only the failed handler's direct task route.
- Results already committed for successful handlers are reused after a restart.
- Redelivery can repeat an uncommitted external effect; use task idempotency keys.

The Pulsar adapter uses JSON bytes and the reserved `duraflow` message property.
Use compatible byte-oriented topics; adapting existing schema-managed topics
requires a compatible codec integration.

## Run it

```bash
python examples/broadcast_join.py
```

The local harness produces a receipt and a final payload of `27` for input `7`.
For independent engine and worker processes, follow
[Running distributed services](distributed.md), which uses the same application.
