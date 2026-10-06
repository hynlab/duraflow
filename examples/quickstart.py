"""Run after installing the project: python examples/quickstart.py."""

import asyncio

from duraflow import Registry, TaskRef, WorkflowContext, task, workflow
from duraflow.testing import TestEnvironment

DOUBLE = TaskRef("double", int, int)


@task(ref=DOUBLE)
def double(value: int) -> int:
    return value * 2


@workflow(name="example", version=1, build_id="example-release-1")
async def example(ctx: WorkflowContext, value: int) -> int:
    first, second = await ctx.gather(ctx.call(DOUBLE, value), ctx.call(DOUBLE, value + 1))
    return first + second


async def main() -> None:
    async with TestEnvironment(Registry(example, double)) as env:
        handle = await env.client.start(example, 5, request_id="demo-1")
        print(await env.run(handle))


if __name__ == "__main__":
    asyncio.run(main())
