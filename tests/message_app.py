"""Importable application for independent message-runtime processes."""

import asyncio
import os
import sqlite3
from pathlib import Path

from duraflow import ChannelRef, Registry, TaskRef, WorkflowContext, task, workflow

DOUBLE = TaskRef("message-double", int, int)
APPROVAL = ChannelRef("approval", bool)


@task(ref=DOUBLE)
async def double(ctx, value: int) -> int:
    directory = os.environ.get("DURAFLOW_MESSAGE_FIXTURE")

    async def checkpoint(point):
        if not directory or os.environ.get("DURAFLOW_MESSAGE_FAULT") != point or ctx.node_id != "1.0":
            return
        marker = Path(directory) / (point + ".ready")
        marker.write_text("ready")
        while True:
            await asyncio.sleep(0.02)

    if directory:
        with sqlite3.connect(Path(directory) / "effects.db") as conn:
            conn.executescript(
                "CREATE TABLE IF NOT EXISTS calls(key TEXT); CREATE TABLE IF NOT EXISTS effects(key TEXT PRIMARY KEY, value INTEGER);"
            )
            conn.execute("INSERT INTO calls VALUES(?)", (ctx.idempotency_key,))
    await checkpoint("before_effect")
    result = value * 2
    if directory:
        with sqlite3.connect(Path(directory) / "effects.db") as conn:
            conn.execute("INSERT OR IGNORE INTO effects VALUES(?,?)", (ctx.idempotency_key, result))
            result = conn.execute("SELECT value FROM effects WHERE key=?", (ctx.idempotency_key,)).fetchone()[0]
    await checkpoint("after_effect")
    return result


@workflow(name="message-order", build_id="message-v1")
async def order(ctx: WorkflowContext, value: int) -> int:
    approvals = ctx.channel(APPROVAL).receive(max_signals=1)
    first = await ctx.call(DOUBLE, value)
    if await approvals.next(timeout=30):
        return await ctx.call(DOUBLE, first)
    return first


@workflow(name="message-timer", build_id="message-v1")
async def timed(ctx: WorkflowContext, value: int) -> bool:
    return await ctx.channel(APPROVAL).receive(max_signals=1).next(timeout=0.5)


registry = Registry(order, timed, double)
signals = {"approval": APPROVAL}
