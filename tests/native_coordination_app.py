"""Importable workflows for native coordination and version-safe composition tests."""

from typing import Any

from duraflow import Registry, SignalRef, WorkflowContext, WorkflowRef, workflow
from tests.native_fault_app import FUNCTIONS, TASKS, pipeline

GO = SignalRef("qualification-go", bool)
BUFFERED = SignalRef("qualification-buffered", int)
CHILD = WorkflowRef("qualification_child", int, int)


@workflow(name="qualification_child", build_id="coordination-v1")
async def child(ctx: WorkflowContext, value: int) -> int:
    return await ctx.call(TASKS[0], value)


@workflow(name="qualification_rollover", build_id="coordination-v1")
async def rollover(ctx: WorkflowContext, value: dict[str, Any]) -> int:
    if value["round"] == 0:
        await ctx.wait_signal(GO)
        await ctx.continue_as_new({**value, "round": 1})
    return await ctx.child(CHILD, value["value"])


@workflow(name="qualification_waiter", build_id="coordination-v1")
async def waiter(ctx: WorkflowContext, value: int) -> bool:
    return await ctx.wait_signal(GO)


registry = Registry(pipeline, child, rollover, waiter, *FUNCTIONS)
