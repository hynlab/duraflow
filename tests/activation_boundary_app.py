"""Importable real-process reproduction of the 1.0.3 response boundary."""

from duraflow import Registry, TaskRef, WorkflowContext, task, workflow

REF = TaskRef("activation-boundary-task", str, int)


@task(ref=REF)
async def implementation(value: str) -> int:
    return len(value)


@workflow(name="activation-boundary", build_id="boundary-v1")
async def flow(ctx: WorkflowContext, total: int) -> int:
    base, remainder = divmod(total, 64)
    values = await ctx.gather(*(ctx.call(REF, "x" * (base + (i < remainder))) for i in range(64)))
    return sum(values)


registry = Registry(flow, implementation)
