"""Run with an isolated interpreter (-I) after installing the built base wheel."""

import asyncio

from duraflow import Registry, TaskRef, WorkflowContext, task, workflow
from duraflow.testing import TestEnvironment

DOUBLE = TaskRef("installed-double", int, int)


@task(ref=DOUBLE)
def double(value: int) -> int:
    return value * 2


@workflow(name="installed-example", build_id="wheel-v1")
async def example(ctx: WorkflowContext, value: int) -> int:
    results = await ctx.gather(ctx.call(DOUBLE, value), ctx.call(DOUBLE, value + 1))
    return sum(results)


async def main():
    async with TestEnvironment(Registry(example, double)) as env:
        handle = await env.client.start(example, 5, request_id="installed-wheel")
        assert await env.run(handle) == 22
    print("Installed base wheel completed a message-driven workflow: 22")


if __name__ == "__main__":
    asyncio.run(main())
