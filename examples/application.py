"""Generic example application; contracts can live in a separate worker package."""

from duraflow import BroadcastBinding, HandlerRef, Registry, TaskRef, TopicRef, WorkflowContext, task, workflow

VALUE = TopicRef("persistent://public/default/example-product", int)
OUTPUT = TopicRef("persistent://public/default/example-analyzed", int)
PRICE, SHIPPING, DEMAND = (TaskRef(label, int, int) for label in ("price", "shipping", "demand"))
HANDLERS = tuple(HandlerRef(ref, ref.name) for ref in (PRICE, SHIPPING, DEMAND))


@task(ref=PRICE)
def price(value: int) -> int:
    return value + 1


@task(ref=SHIPPING)
def shipping(value: int) -> int:
    return value + 2


@task(ref=DEMAND)
def demand(value: int) -> int:
    return value + 3


@workflow(name="product", version=1, build_id="example-release-1")
async def product(ctx: WorkflowContext, value: int) -> str:
    result = await ctx.broadcast(VALUE, value, handlers=HANDLERS)
    return await ctx.publish(OUTPUT, sum(result[handler] for handler in HANDLERS))


registry = Registry(product, price, shipping, demand)
broadcasts = tuple(BroadcastBinding(VALUE, handler) for handler in HANDLERS)
