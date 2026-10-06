"""Importable adversarial workflows for isolated replay regression tests."""

from duraflow import Registry, WorkflowContext, workflow


@workflow(name="watchdog_loop", build_id="watchdog-v1")
async def endless(ctx: WorkflowContext, value: int) -> int:
    while True:
        pass


@workflow(name="watchdog_cleanup", build_id="watchdog-v1")
async def endless_cleanup(ctx: WorkflowContext, value: int) -> int:
    try:
        await ctx.sleep(1)
    finally:
        while True:
            pass
    return value


@workflow(name="watchdog_healthy", build_id="watchdog-v1")
async def healthy(ctx: WorkflowContext, value: int) -> int:
    return value + 1


@workflow(name="watchdog_sleep", build_id="watchdog-v1")
async def sleeping(ctx: WorkflowContext, value: int) -> int:
    await ctx.sleep(1)
    return value


registry = Registry(endless, endless_cleanup, healthy, sleeping)
