"""Durable approval channel; also runnable using independent message workers."""

import asyncio

from duraflow import Registry, TaskRef, WorkflowContext, task, workflow
from duraflow.testing import TestEnvironment
from examples.signal_contracts import APPROVAL

VALIDATE = TaskRef("validate-order", int, int)


@task(ref=VALIDATE)
def validate(value: int) -> int:
    return value * 2


@workflow(name="approval-example", build_id="signals-v1")
async def order(ctx: WorkflowContext, value: int) -> int:
    approvals = ctx.channel(APPROVAL).receive(max_signals=1)
    validated = await ctx.call(VALIDATE, value)
    return validated if await approvals.next(timeout=60) else 0


registry = Registry(order, validate)
signals = {"approval": APPROVAL}


async def main() -> None:
    async with TestEnvironment(registry) as env:
        handle = await env.client.start(order, 7, request_id="approval-demo")
        await env.drain()
        await handle.signal(APPROVAL, True, signal_id="approved-1")
        print(await env.run(handle))  # 14


if __name__ == "__main__":
    asyncio.run(main())
