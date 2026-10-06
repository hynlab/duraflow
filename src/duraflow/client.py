"""Trusted shared-store SDK; no HTTP server or implicit worker implementation imports."""

from __future__ import annotations

from .contracts import clock_now

import asyncio
import hashlib
import hmac
from typing import Any
from uuid import uuid4

from .contracts import (
    Archived,
    Clock,
    Conflict,
    Registry,
    SignalRef,
    TERMINAL,
    TaskFailure,
    TaskRef,
    WorkflowBlocked,
    WorkflowFailed,
    decode,
    duration,
    encode,
    fingerprint,
    name,
    schema_id,
)
from .state import attempt, event, mutate, new_run, wake
from .storage import State, Store
from .lifecycle import record_observation


class Client:
    def __init__(
        self, store: Store, registry: Registry | None = None, *, namespace: str = "default", clock: Clock | None = None
    ):
        self.store, self.registry = store, registry or Registry()
        self.namespace, self.clock = name(namespace), clock or Clock()

    async def start(
        self,
        workflow: Any,
        value: Any,
        *,
        request_id: str,
        workflow_id: str | None = None,
        tags: tuple[str, ...] = (),
        _run_id: str | None = None,
    ) -> WorkflowHandle:
        if not request_id or len(request_id) > 256:
            raise ValueError("request_id must contain 1..256 characters")
        workflow_id = workflow_id or request_id
        if len(workflow_id) > 256 or len(tags) > 32:
            raise ValueError("Identity/tag limit exceeded")
        for tag in tags:
            name(tag)
        definition = self.registry.resolve(workflow)
        value = encode(value, definition.ref.input_type)
        digest = fingerprint([definition.manifest, workflow_id, value, sorted(set(tags))])
        state = new_run(
            definition, self.namespace, _run_id or str(uuid4()), workflow_id, value, clock_now(self.clock), tags
        )
        saved = await self.store.create(state, request_id, digest)
        return WorkflowHandle(self, saved["run_id"], definition.ref.output_type)

    def get_handle(self, run_id: str, output_type: Any = Any) -> WorkflowHandle:
        return WorkflowHandle(self, run_id, output_type)

    async def current(self, workflow_id: str) -> WorkflowHandle:
        return self.get_handle(await self.store.head(self.namespace, workflow_id))

    async def list(self, *, after: str = "", limit: int = 100, tags: tuple[str, ...] = ()) -> list[State]:
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be in 1..1000")
        result: list[State] = []
        cursor = after
        while len(result) < limit:
            rows = await self.store.scan(self.namespace, cursor, min(100, limit - len(result)))
            if not rows:
                break
            result.extend(row for row in rows if set(tags) <= set(row["tags"]))
            cursor = rows[-1]["run_id"]
        return result

    async def control_tagged(
        self,
        action: str,
        *,
        tags: tuple[str, ...],
        actor: str,
        reason: str,
        request_id: str,
        after: str = "",
        limit: int = 100,
    ) -> dict[str, Any]:
        if action not in {"cancel", "terminate", "resume"} or not tags or not request_id:
            raise ValueError("Specify tags, a request_id and cancel/terminate/resume")
        if len(request_id) > 128:
            raise ValueError("Batch request_id exceeds 128 characters")
        snapshot = await self.list(after=after, limit=limit, tags=tags)
        outcomes = {}
        for row in snapshot:
            handle = self.get_handle(row["run_id"])
            try:
                await handle._control(action, actor=actor, reason=reason, request_id=f"{request_id}/{row['run_id']}")
                outcomes[row["run_id"]] = "accepted"
            except Conflict:
                outcomes[row["run_id"]] = "conflict"
        return {"outcomes": outcomes, "next_after": snapshot[-1]["run_id"] if snapshot else None}

    async def signal_workflow(self, workflow_id: str, ref: SignalRef[Any], value: Any, *, signal_id: str) -> None:
        run_id = await self.store.head(self.namespace, workflow_id)
        for _ in range(100):
            try:
                await self.get_handle(run_id).signal(ref, value, signal_id=signal_id)
                return
            except Conflict:
                state = await self.store.load(self.namespace, run_id)
                if state["status"] != "CONTINUED":
                    raise
                run_id = state["continued_run_id"]
        raise Conflict("Too many concurrent rollovers")

    async def complete_external(
        self, token: str, value: Any = None, *, ref: TaskRef[Any, Any], error: dict[str, Any] | None = None
    ) -> bool:
        try:
            run_id, node_id, number, epoch, _ = token.split("/", 4)
            number_int, epoch_int = int(number), int(epoch)
        except (ValueError, AttributeError):
            raise ValueError("Invalid completion token") from None
        digest = hashlib.sha256(token.encode()).hexdigest()
        observation = {
            "result": encode(value, ref.output_type) if error is None else None,
            "error": TaskFailure(error).error if error is not None else None,
        }

        def change(state: State) -> bool:
            node = state["nodes"].get(node_id)
            if node is None or node["spec"]["kind"] != "call" or node["spec"]["ref"] != ref.descriptor():
                raise Conflict("Unknown or incompatible delegated invocation")
            current = node["attempts"][-1]
            deferred = current.get("deferred")
            if (
                current["number"] != number_int
                or current["epoch"] != epoch_int
                or not deferred
                or not hmac.compare_digest(deferred["token_hash"], digest)
            ):
                raise Conflict("Completion token is invalid or superseded")
            now = clock_now(self.clock)
            if current["observation"] is not None:
                return record_observation(state, node, observation, now, kind="external_observation")
            if (
                node["state"] != "pending"
                or state["status"] in (TERMINAL - {"COMPLETED", "FAILED"})
                or state["status"] == "CANCELLING"
                or deferred["expires_at"] <= now
            ):
                raise Conflict("Delegated invocation no longer accepts completion")
            changed = record_observation(state, node, observation, now, kind="external_observation")
            if changed:
                wake(state, f"external/{node_id}/{number_int}/{epoch_int}", now)
            return changed

        return bool(await mutate(self.store, self.namespace, run_id, change))


class WorkflowHandle:
    def __init__(self, client: Client, run_id: str, output_type: Any = Any):
        self.client, self.run_id, self.output_type = client, run_id, output_type

    async def describe(self) -> State:
        return await self.client.store.load(self.client.namespace, self.run_id)

    async def history(self, *, after: int = 0, limit: int = 100) -> list[dict[str, Any]]:
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be in 1..1000")
        state = await self.describe()
        if state["archived"]:
            raise Archived(self.run_id)
        return [row for row in state["history"] if row["sequence"] > after][:limit]

    async def result(
        self, *, timeout: float | None = None, poll_interval: float = 0.1, follow_continued: bool = False
    ) -> Any:
        duration(poll_interval)

        async def wait() -> Any:
            handle = self
            while True:
                state = await handle.describe()
                if state["archived"]:
                    raise Archived(handle.run_id)
                if state["status"] == "COMPLETED":
                    return decode(state["result"], self.output_type)
                if state["status"] == "BLOCKED":
                    raise WorkflowBlocked(str(state["blocked_reason"]))
                if state["status"] == "CONTINUED" and follow_continued:
                    handle = self.client.get_handle(state["continued_run_id"], self.output_type)
                elif state["status"] in TERMINAL:
                    raise WorkflowFailed(f"{state['status']}: {state['error']}")
                await asyncio.sleep(poll_interval)

        async with asyncio.timeout(timeout):
            return await wait()

    async def signal(self, ref: SignalRef[Any], value: Any, *, signal_id: str) -> None:
        if not signal_id or len(signal_id) > 256:
            raise ValueError("signal_id must contain 1..256 characters")
        payload = encode(value, ref.payload_type)
        descriptor = {"id": signal_id, "name": ref.name, "schema": schema_id(ref.payload_type), "payload": payload}
        digest = fingerprint(descriptor)

        def change(state: State) -> None:
            previous = state["signal_keys"].get(signal_id)
            if previous is not None:
                if previous != digest:
                    raise Conflict("Signal ID reused with different data")
                return
            if state["status"] in TERMINAL or state["archived"]:
                raise Conflict("Run no longer accepts new signals")
            if len(state["signal_keys"]) >= 10_000 or sum(not s["consumed"] for s in state["signals"]) >= 1000:
                raise Conflict("Signal mailbox/idempotency quota exceeded")
            if any(s["name"] == ref.name and s["schema"] != descriptor["schema"] for s in state["signals"]):
                raise Conflict("Signal channel schema is already pinned to another contract")
            sequence = event(state, "signal_received", self.client.clock.now(), signal_id=signal_id, channel=ref.name)
            state["signals"].append({**descriptor, "sequence": sequence, "consumed": False})
            state["signal_keys"][signal_id] = digest
            wake(state, f"signal/{signal_id}", self.client.clock.now())

        await mutate(self.client.store, self.client.namespace, self.run_id, change)

    async def _control(
        self, action: str, *, actor: str, reason: str, request_id: str, node_id: str | None = None
    ) -> None:
        if not actor or not reason or not request_id or len(actor) > 128 or len(reason) > 1000 or len(request_id) > 256:
            raise ValueError("Controls require bounded actor, reason and request_id")
        if action not in {"cancel", "terminate", "resume", "retry"}:
            raise ValueError("Unsupported operator action")
        digest = fingerprint([action, actor, reason, node_id])

        def change(state: State) -> None:
            if request_id in state["actions"]:
                if state["actions"][request_id] != digest:
                    raise Conflict("Operator request key reused")
                return
            if state["status"] in TERMINAL:
                raise Conflict("Terminal executions cannot be reopened")
            if len(state["actions"]) >= 1000:
                raise Conflict("Operator action quota exceeded")
            now = self.client.clock.now()
            if action in {"cancel", "terminate"}:
                state["status"] = "CANCELLING" if action == "cancel" else "TERMINATED"
                for node in state["nodes"].values():
                    if node["state"] in {"pending", "blocked"}:
                        node["cancel_requested"] = True
                        node["cancel_policy"] = 2
                        node.setdefault("cancel_requested_seq", state["sequence"] + 1)
                        if node["state"] == "blocked":
                            node["state"] = "pending"
                if action == "terminate":
                    state["finished_at"] = now
            elif action == "resume":
                if state["status"] != "BLOCKED" or any(n["state"] == "blocked" for n in state["nodes"].values()):
                    raise Conflict(
                        "Resume requires a blocked execution without an exhausted task; retry that task first"
                    )
                state["status"], state["blocked_reason"] = "WAITING", None
            elif action == "retry":
                node = state["nodes"].get(node_id)
                if state["status"] != "BLOCKED" or node is None or node["state"] != "blocked":
                    raise Conflict("Retry requires a blocked invocation")
                node["state"] = "pending"
                node["attempts"].append(attempt(node["attempts"][-1]["number"] + 1, now))
                remaining = any(n["state"] == "blocked" for n in state["nodes"].values())
                state["status"] = "BLOCKED" if remaining else "WAITING"
                if not remaining:
                    state["blocked_reason"] = None
            state["actions"][request_id] = digest
            event(state, "operator_action", now, action=action, actor=actor, reason=reason, node_id=node_id)
            wake(state, f"control/{request_id}", self.client.clock.now())

        await mutate(self.client.store, self.client.namespace, self.run_id, change)

    async def cancel(self, *, actor: str, reason: str, request_id: str) -> None:
        await self._control("cancel", actor=actor, reason=reason, request_id=request_id)

    async def terminate(self, *, actor: str, reason: str, request_id: str) -> None:
        await self._control("terminate", actor=actor, reason=reason, request_id=request_id)

    async def resume_blocked(self, *, actor: str, reason: str, request_id: str) -> None:
        await self._control("resume", actor=actor, reason=reason, request_id=request_id)

    async def retry_blocked_task(self, node_id: str, *, actor: str, reason: str, request_id: str) -> None:
        await self._control("retry", actor=actor, reason=reason, request_id=request_id, node_id=node_id)

    async def archive(self, *, actor: str, reason: str, safety_horizon: float, retention: float) -> None:
        duration(safety_horizon)
        duration(retention)
        if not actor or not reason or retention < safety_horizon:
            raise ValueError("Require actor/reason and retention >= redelivery safety horizon")

        def change(state: State) -> None:
            if state["archived"]:
                return
            if (
                state["status"] not in TERMINAL
                or state["finished_at"] is None
                or self.client.clock.now() - state["finished_at"] < retention
                or any(not msg["delivered"] for msg in state["outbox"].values())
                or any(node["state"] in {"pending", "blocked"} for node in state["nodes"].values())
            ):
                raise Conflict("Run is not safe to archive")
            state["archived"] = True
            state["input"], state["result"] = None, None
            for key in ("commands", "history", "signals"):
                state[key] = []
            for key in ("nodes", "outbox", "inbox"):
                state[key] = {}
            event(state, "archived", self.client.clock.now(), actor=actor, reason=reason)

        await mutate(self.client.store, self.client.namespace, self.run_id, change)
