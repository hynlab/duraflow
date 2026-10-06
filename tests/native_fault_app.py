"""Disposable application with an independent, idempotent external effect ledger."""
from __future__ import annotations

import asyncio
import os
import sqlite3
import time
from pathlib import Path
from typing import Any

from duraflow import HandlerRef, Registry, TaskRef, TopicRef, WorkflowContext, task, workflow
from duraflow.runner import TaskContext

TASKS = tuple(TaskRef(f"fault_handler_{i}", int, int) for i in range(3))
HANDLERS = tuple(HandlerRef(ref, ref.name) for ref in TASKS)


def topic(namespace: str, suffix: str) -> TopicRef[int]:
    return TopicRef(f"persistent://public/default/df-fault-{namespace}-{suffix}", int)


def ledger_path() -> Path:
    return Path(os.environ["DURAFLOW_FAULT_DIRECTORY"]) / "external.sqlite"


def initialize_ledger(path: Path) -> None:
    with sqlite3.connect(path, timeout=10) as conn:
        conn.executescript("PRAGMA journal_mode=WAL; CREATE TABLE IF NOT EXISTS calls "
                           "(id INTEGER PRIMARY KEY, task_key TEXT NOT NULL, handler INTEGER NOT NULL); "
                           "CREATE TABLE IF NOT EXISTS effects "
                           "(task_key TEXT PRIMARY KEY, handler INTEGER NOT NULL, result INTEGER NOT NULL);")


async def checkpoint(point: str, handler: int) -> None:
    if os.environ.get("DURAFLOW_FAULT_POINT") != point or os.environ.get("DURAFLOW_FAULT_HANDLER") != str(handler):
        return
    root = Path(os.environ["DURAFLOW_FAULT_DIRECTORY"])
    (root / f"{handler}.{point}.ready").write_text("ready")
    deadline = time.monotonic() + 150
    while not (root / f"{handler}.{point}.release").exists():
        if time.monotonic() >= deadline:
            raise TimeoutError("Disposable fault barrier was not released")
        await asyncio.sleep(0.02)


async def effect(ctx: TaskContext, value: int, handler: int) -> int:
    def entered() -> None:
        with sqlite3.connect(ledger_path(), timeout=10) as conn:
            conn.execute("INSERT INTO calls(task_key, handler) VALUES (?, ?)", (ctx.idempotency_key, handler))
    await asyncio.to_thread(entered)
    await checkpoint("before_effect", handler)
    def commit() -> int:
        with sqlite3.connect(ledger_path(), timeout=10) as conn:
            conn.execute("INSERT OR IGNORE INTO effects(task_key,handler,result) VALUES (?,?,?)",
                         (ctx.idempotency_key, handler, value + handler))
            return int(conn.execute("SELECT result FROM effects WHERE task_key=?", (ctx.idempotency_key,)).fetchone()[0])
    result = await asyncio.to_thread(commit)
    await checkpoint("after_effect", handler)
    return result


@task(ref=TASKS[0])
async def handler_zero(ctx: TaskContext, value: int) -> int:
    return await effect(ctx, value, 0)


@task(ref=TASKS[1])
async def handler_one(ctx: TaskContext, value: int) -> int:
    return await effect(ctx, value, 1)


@task(ref=TASKS[2])
async def handler_two(ctx: TaskContext, value: int) -> int:
    return await effect(ctx, value, 2)


@workflow(name="native_fault_pipeline", build_id="qualification-v1")
async def pipeline(ctx: WorkflowContext, value: dict[str, Any]) -> list[int]:
    result = await ctx.broadcast(topic(value["namespace"], "input"), value["value"], handlers=HANDLERS)
    await ctx.publish(topic(value["namespace"], "output"), value["value"])
    return [result[handler] for handler in HANDLERS]


FUNCTIONS = (handler_zero, handler_one, handler_two)
registry = Registry(pipeline, *FUNCTIONS)
