"""Importable compensation workflow for inline and subprocess replay tests."""

from duraflow import Registry, TaskFailure, TaskRef, WorkflowContext, task, workflow

FAILURE = TaskRef("audit-failure", int, int)


@task(ref=FAILURE)
async def failing(value):
    raise TaskFailure({"code": "UPSTREAM", "message": "x" * 1000})


@workflow(name="audit-compensation", build_id="audit-v1")
async def flow(ctx: WorkflowContext, value: int) -> int:
    try:
        await ctx.gather(*(ctx.call(FAILURE, i) for i in range(value)))
    except TaskFailure as exc:
        assert exc.code == "GROUP_FAILURE"
        assert len(exc.error["causes"]) + exc.error.get("omitted_causes", 0) == value
        return -1
    return 0


registry = Registry(flow, failing)
