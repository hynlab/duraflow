"""Phase 2: migrations, authoritative-time mutations, due queries, bounded retry."""
import ast
from pathlib import Path
from helpers import add_method, function, replace, write

replace('src/duraflow/contracts.py', 'import hashlib', 'from contextvars import ContextVar\n\nimport hashlib')
replace('src/duraflow/contracts.py', 'CODEC_VERSION = 1', 'CODEC_VERSION = 1\n_STORE_TIME: ContextVar[float | None] = ContextVar("duraflow_store_time", default=None)')
with Path('src/duraflow/contracts.py').open('a') as stream:
    stream.write('''\n\ndef clock_now(clock: Clock) -> float:
    """Server time inside atomic store mutations; the configured clock otherwise."""
    trusted = _STORE_TIME.get()
    return clock.now() if trusted is None else trusted
''')
function('src/duraflow/state.py', 'mutate', '''
async def mutate(store: Store, namespace: str, run_id: str, change: Callable[[State], Any]) -> Any:
    atomic = getattr(store, "mutate_atomic", None)
    if atomic is not None:
        return await atomic(namespace, run_id, change)
    for _ in range(64):
        state = await store.load(namespace, run_id)
        before = fingerprint(state)
        result = change(state)
        if fingerprint(state) == before or await store.save(state, state["revision"]):
            return result
    raise Conflict("Concurrent updates exceeded retry budget; retry the operation")
''')
for path in ('client.py', 'runner.py', 'coordinator.py'):
    target = Path('src/duraflow') / path
    text = target.read_text().replace('self.worker.clock.now()', 'clock_now(self.worker.clock)')
    text = text.replace('self.clock.now()', 'clock_now(self.clock)')
    text = text.replace('from __future__ import annotations', 'from __future__ import annotations\n\nfrom .contracts import clock_now')
    target.write_text(text)
replace('src/duraflow/runner.py', 'digest, now = fingerprint([payload, meta]), clock_now(self.clock)', 'digest = fingerprint([payload, meta])')
# Time must be sampled after the PostgreSQL row lock, not captured by the caller.
p = Path('src/duraflow/runner.py')
text = p.read_text()
cls = next(n for n in ast.parse(text).body if isinstance(n, ast.ClassDef) and n.name == 'Worker')
method = next(n for n in cls.body if isinstance(n, ast.AsyncFunctionDef) and n.name == '_claim')
change = next(n for n in method.body if isinstance(n, ast.FunctionDef) and n.name == 'change')
lines = text.splitlines(keepends=True)
lines.insert(change.body[0].lineno - 1, '            now = clock_now(self.clock)\n')
p.write_text(''.join(lines))
replace('src/duraflow/coordinator.py', '        original, now = fingerprint(state), clock_now(self.clock)', '        original = fingerprint(state)\n        native_now = getattr(self.store, "now", None)\n        now = await native_now() if native_now is not None else clock_now(self.clock)\n        state.setdefault("reconcile_interval", self.reconcile_interval)')
replace('src/duraflow/coordinator.py', '        result = await child.describe()', '        result = await child.describe()\n        node["child_started"] = True')
function('src/duraflow/coordinator.py', 'Engine._close_children', '''
async def _close_children(self, run_id: str) -> None:
    state = await self.store.load(self.namespace, run_id)
    if state["status"] not in TERMINAL | {"CANCELLING"}:
        return
    for node in state["nodes"].values():
        if (node["spec"]["kind"] != "child" or node["spec"].get("abandon")
                or node.get("child_close_confirmed")):
            continue
        try:
            child_id = node["child_run_id"]
            child = await self.store.load(self.namespace, child_id)
            for _ in range(100):
                if child["status"] != "CONTINUED":
                    break
                child_id = child["continued_run_id"]
                child = await self.store.load(self.namespace, child_id)
            else:
                raise Conflict("Child continuation chain exceeds the reconciliation bound")
            if child["status"] not in TERMINAL | {"CANCELLING"}:
                await self.client.get_handle(child_id).cancel(actor="parent", reason="Parent closed",
                                                              request_id=f"parent-close/{run_id}")
            def confirmed(parent: State) -> None:
                current = parent["nodes"].get(node["id"])
                if current is not None:
                    current["child_close_confirmed"] = True
            await mutate(self.store, self.namespace, run_id, confirmed)
        except (Conflict, NotFound):
            # Missing is not evidence that a concurrent child-start cannot commit.
            pass
''')
function('src/duraflow/coordinator.py', 'Engine.flush', '''
async def flush(self, run_id: str, limit: int = 32) -> None:
    for _ in range(limit):
        owner = str(uuid4())
        def claim(state: State) -> Any:
            now = clock_now(self.clock)
            for event_id, item in state["outbox"].items():
                if item["delivered"] or item["lease_until"] > now:
                    continue
                if (item["metadata"]["kind"] != "wake"
                        and state["status"] in {"CANCELLING", "CANCELLED", "TERMINATED"}):
                    item["delivered"], item["suppressed"] = True, True
                    continue
                if item.get("next_attempt_at", 0) > now:
                    continue
                item["owner"], item["lease_until"] = owner, now + 30
                item["attempts"] += 1
                return event_id, clone(item)
            return None
        claimed = await mutate(self.store, self.namespace, run_id, claim)
        if claimed is None:
            return
        event_id, item = claimed
        try:
            await asyncio.wait_for(self.transport.publish(
                item["topic"], canonical(item["payload"]).encode(),
                {"duraflow": canonical(item["metadata"])}), timeout=10)
        except Exception as exc:
            self.metrics["transport_errors"] += 1
            error_type = type(exc).__name__
            def release(state: State) -> None:
                current = state["outbox"].get(event_id)
                if current and current["owner"] == owner:
                    current["lease_until"], current["owner"] = 0, None
                    current["next_attempt_at"] = clock_now(self.clock) + min(60, 2 ** min(current["attempts"] - 1, 6))
                    current["last_error_type"] = error_type
            await mutate(self.store, self.namespace, run_id, release)
            raise
        def delivered(state: State) -> None:
            current = state["outbox"].get(event_id)
            if current and current["owner"] == owner:
                current["delivered"], current["lease_until"] = True, 0
                current["next_attempt_at"] = 0
        await mutate(self.store, self.namespace, run_id, delivered)
        self.metrics["publications"] += 1
''')
function('src/duraflow/coordinator.py', 'Engine.tick', '''
async def tick(self) -> int:
    due = getattr(self.store, "scan_due", None)
    async def page(after: str) -> list[State]:
        if due is not None:
            return await due(self.namespace, after, self.batch_size,
                             manifests=tuple(d.manifest for d in self.registry.workflows.values()))
        return await self.store.scan(self.namespace, after, self.batch_size)
    rows = await page(self.cursor)
    if not rows and self.cursor:
        self.cursor = ""
        rows = await page("")
    self.cursor = rows[-1]["run_id"] if len(rows) == self.batch_size else ""
    semaphore = asyncio.Semaphore(4)
    failures = []
    async def process(state: State) -> None:
        async with semaphore:
            try:
                # A stuck route cannot hold the entire polling batch indefinitely.
                async with asyncio.timeout(30):
                    await self.advance(state["run_id"])
                    await self.flush(state["run_id"])
                    refresh = getattr(self.store, "refresh_projection", None)
                    if refresh is not None:
                        await refresh(self.namespace, state["run_id"])
            except Exception as exc:
                failures.append(exc)
    await asyncio.gather(*(process(state) for state in rows))
    if failures:
        raise failures[0]
    return len(rows)
''')
replace('tests/test_recovery.py', '        env.transport.publish = original\n        assert await env.run(h) == 9', '        env.transport.publish = original\n        env.clock.advance(1)\n        assert await env.run(h) == 9')
# Confine native blocking calls to a bounded pool; timeout does not free capacity
# until the actual native operation finishes.
replace('src/duraflow/transport.py', 'import asyncio', 'import asyncio\nfrom concurrent.futures import ThreadPoolExecutor\nfrom functools import partial')
replace('src/duraflow/transport.py', '        self.lock = asyncio.Lock()', '        self.lock = asyncio.Lock()\n        self.native_pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="duraflow-pulsar")\n        self.native_slots = asyncio.Semaphore(8)')
p = Path('src/duraflow/transport.py')
text = p.read_text().replace('await asyncio.to_thread(', 'await self._native(')
p.write_text(text)
add_method('src/duraflow/transport.py', 'PulsarTransport', '''
async def _native(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
    await self.native_slots.acquire()
    try:
        future = asyncio.get_running_loop().run_in_executor(self.native_pool, partial(fn, *args, **kwargs))
    except BaseException:
        self.native_slots.release()
        raise
    def released(done: Any) -> None:
        self.native_slots.release()
        if not done.cancelled():
            done.exception()
    future.add_done_callback(released)
    return await asyncio.shield(future)
''')
replace('src/duraflow/transport.py', 'max_pending_messages=64,', 'max_pending_messages=64,\n                    send_timeout_millis=5000,')
function('src/duraflow/transport.py', 'PulsarTransport.close', '''
async def close(self) -> None:
    try:
        await self._native(self.client.close)
    finally:
        self.native_pool.shutdown(wait=False, cancel_futures=True)
''')
replace('src/duraflow/cli.py', 'for command in ("init", "health", "engine", "worker"):', 'for command in ("init", "migrate", "health", "engine", "worker"):')
replace('src/duraflow/cli.py', '        if args.command == "init":', '        if args.command in {"init", "migrate"}:')
replace('src/duraflow/cli.py', '            return {"schema_version": 1, "initialized": True}', '            version = getattr(store, "schema_version", None)\n            return {"schema_version": await version() if version is not None else 1, "initialized": True}')
write('docs/hardening/phase2.md', '''
# Phase 2 — storage and scheduling

B01: explicit transactional PostgreSQL schema migration 1 -> 2. Existing JSON
run documents are preserved; next_due/status/implementation projections and a
partial due-work index are added. Unknown ledgers and downgrades fail closed.
The CLI init/migrate operation is separate from runtime startup. Stop old runtime
writers before migration: this is a bounded maintenance upgrade, NOT an online
mixed-alpha/new-runtime migration. Multiple workflow builds on the new runtime
are independent of this database runtime-version restriction.

B02: internal state mutations use SELECT FOR UPDATE and sample PostgreSQL
clock_timestamp AFTER acquiring the row lock. A context-local trusted clock
makes claim, heartbeat, observation and operator changes use that same clock.
No user callback, broker call or replay runs inside these transactions. Workflow
advancement remains optimistic CAS, with database time sampled for deadline
checks. Request creation and rollover creation are stamped by PostgreSQL.

B03: runtime polling selects due run IDs using an indexed projection and exact
manifest capability filter. Manual SDK list/scan remains separate. Finished
status alone never suppresses pending outbox, cancellation or child-close work.
Signal/task observations update the projection atomically. Quiet completed runs
and long sleeps are excluded; a conservative child recheck remains bounded at
one second. Missing child records remain reconciliation obligations because a
concurrent child start might still commit.

B04: PostgreSQL pool/lock/statement/operation bounds; native Pulsar calls run in
a bounded eight-thread pool. Cancelling a caller does not release a native-call
slot until that call actually exits. Producer sends have a native five-second
timeout. Outbox retries persist 1..60 second backoff and sanitized error class.
The next send retains its stable event ID. Engine batches process at most four
runs concurrently, with per-run 30-second bounds. There is no infinite immediate
retry loop and one failed route cannot permanently monopolize other runs.

These changes do not establish PITR, broker disaster recovery or production
capacity. Those remain separate native-failure and load acceptance tasks.
''')
