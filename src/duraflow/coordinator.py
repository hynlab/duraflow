"""Durable state coordination, retry scheduling and transactional outbox dispatch."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from .client import Client
from .contracts import (
    Clock,
    Conflict,
    NotFound,
    ProtocolError,
    Registry,
    TERMINAL,
    WorkflowBlocked,
    canonical,
    decode,
    duration,
    encode,
    fingerprint,
    name,
)
from .replay import replay
from .state import attempt, event, finish_node, identity, mutate, new_run, outbox, route, subscription
from .storage import State, Store, clone
from .transport import Transport

log = logging.getLogger("duraflow.engine")


def error_data(code: str, message: str | None = None) -> dict[str, Any]:
    return {"code": code, "message": message or code}


def settle_task(state: State, node: State, now: float) -> None:
    """Pure task transition; caller commits the decision with an expected revision."""
    current, options = node["attempts"][-1], node["spec"]["options"]
    observation = current["observation"]
    error = None
    if state["status"] == "TERMINATED":
        error = error_data("TERMINATED")
    elif node.get("cancel_requested") and (current["started_at"] is None or observation is not None):
        error = error_data("CANCELLED")
    elif options["overall_timeout"] is not None and now >= node["created_at"] + options["overall_timeout"]:
        error = error_data("OVERALL_TIMEOUT")
    elif (
        options["attempt_timeout"] is not None
        and current["started_at"] is not None
        and now >= current["started_at"] + options["attempt_timeout"]
    ):
        error = error_data("ATTEMPT_TIMEOUT")
    elif (
        options["schedule_timeout"] is not None
        and current["started_at"] is None
        and now >= current["not_before"] + options["schedule_timeout"]
    ):
        error = error_data("SCHEDULE_TIMEOUT")
    elif current["deferred"] is not None and now >= current["deferred"]["expires_at"]:
        error = error_data("DELEGATION_TIMEOUT")
    elif observation is not None:
        error = observation["error"]
    else:
        return
    if error is None:
        finish_node(state, node, now, result=observation["result"])
        return
    policy = options["retry"]
    can_retry = (
        error["code"] in policy["retry_codes"]
        and error["code"] not in {"OVERALL_TIMEOUT", "CANCELLED", "TERMINATED"}
        and not node.get("cancel_requested")
    )
    current["final_error"] = clone(error)
    if can_retry and current["number"] < policy["max_attempts"]:
        next_attempt = attempt(current["number"] + 1, now)
        delay = min(policy["max_delay"], policy["delay"] * policy["multiplier"] ** min(current["number"] - 1, 30))
        next_attempt["not_before"] = now + delay
        node["attempts"].append(next_attempt)
        event(state, "retry_scheduled", now, node_id=node["id"], attempt=next_attempt["number"])
    elif can_retry and policy["exhausted"] == "block" and state["status"] not in TERMINAL:
        node["state"], state["status"] = "blocked", "BLOCKED"
        state["blocked_reason"] = f"Retry exhausted for {node['id']}"
        event(state, "task_blocked", now, node_id=node["id"])
    else:
        finish_node(state, node, now, error=error)


def collect(state: State, now: float) -> None:
    for record in state["commands"]:
        if record["state"] != "pending":
            continue
        nodes = [state["nodes"][node_id] for node_id in record["members"]]
        kind = record["spec"]["kind"]
        if kind == "race":
            candidates = [(i, node) for i, node in enumerate(nodes) if node["state"] in {"done", "error"}]
            if not candidates:
                continue
            i, winner = min(candidates, key=lambda pair: pair[1]["accepted_seq"])
            record["state"], record["error"] = winner["state"], winner["error"]
            record["result"] = {"index": i, "value": clone(winner["result"])}
        elif all(node["state"] in {"done", "error"} for node in nodes):
            failures = [node["error"] for node in nodes if node["state"] == "error"]
            if failures:
                record["state"] = "error"
                record["error"] = (
                    failures[0]
                    if len(nodes) == 1
                    else {"code": "GROUP_FAILURE", "message": "One or more group members failed", "causes": failures}
                )
            else:
                record["state"] = "done"
                values = [clone(node["result"]) for node in nodes]
                record["result"] = values if kind in {"gather", "broadcast"} else values[0]
        else:
            continue
        event(state, "command_resolved", now, command_id=record["id"], outcome=record["state"])


class Engine:
    def __init__(
        self,
        store: Store,
        transport: Transport,
        registry: Registry,
        *,
        namespace: str = "default",
        clock: Clock | None = None,
        batch_size: int = 100,
        max_commands: int = 1000,
        reconcile_interval: float = 10.0,
    ):
        duration(reconcile_interval)
        if not 1 <= batch_size <= 1000 or not 1 <= max_commands <= 10000:
            raise ValueError("Invalid batch/history limits")
        self.store, self.transport, self.registry = store, transport, registry
        self.namespace, self.clock = name(namespace), clock or Clock()
        self.batch_size, self.max_commands = batch_size, max_commands
        self.reconcile_interval, self.cursor = reconcile_interval, ""
        self.client = Client(store, registry, namespace=namespace, clock=self.clock)
        self.metrics = {"activations": 0, "cas_conflicts": 0, "publications": 0, "transport_errors": 0}

    async def _schedule(self, state: State, spec: dict[str, Any], now: float) -> None:
        if len(state["commands"]) >= self.max_commands:
            raise ProtocolError("Command history limit reached; use bounded children or continue_as_new")
        members = spec.get("members", [spec])
        if len(members) > 1000:
            raise ProtocolError("Group size exceeds 1000")
        for member in members:
            if member["kind"] == "call":
                await self.transport.ensure(route(self.namespace, member["ref"]), "workers")
        if spec["kind"] == "broadcast":
            for member in members:
                await self.transport.ensure(spec["topic"], subscription(self.namespace, member["handler"]))
        index = str(len(state["commands"]))
        record = {"id": index, "spec": clone(spec), "state": "pending", "members": [], "result": None, "error": None}
        handler_map = {}
        for position, member in enumerate(members):
            node_id = f"{index}.{position}"
            node = {
                "id": node_id,
                "spec": clone(member),
                "state": "pending",
                "created_at": now,
                "result": None,
                "error": None,
                "accepted_seq": None,
                "cancel_requested": False,
            }
            kind = member["kind"]
            if kind == "call":
                node["task_id"], node["attempts"] = identity(state["run_id"], f"task/{node_id}"), [attempt(1, now)]
                if spec["kind"] == "broadcast":
                    node["initial_topic"] = spec["topic"]
                    node["attempts"][0]["dispatched"], node["attempts"][0]["last_dispatch"] = True, now
                    handler_map[subscription(self.namespace, member["handler"])] = node_id
            elif kind == "sleep":
                node["due_at"] = now + member["seconds"]
            elif kind == "signal":
                node["due_at"] = None if member["timeout"] is None else now + member["timeout"]
            elif kind == "publish":
                node["outbox_id"] = outbox(
                    state,
                    f"publication/{node_id}",
                    "publication",
                    member["topic"],
                    member["input"],
                    now,
                    node_id=node_id,
                )
            elif kind == "child":
                node["child_run_id"] = identity(state["run_id"], f"child/{node_id}")
            elif kind == "now":
                finish_node(state, node, now, result=datetime.fromtimestamp(now, timezone.utc).isoformat())
            elif kind == "uuid":
                finish_node(state, node, now, result=str(uuid4()))
            record["members"].append(node_id)
            state["nodes"][node_id] = node
        state["commands"].append(record)
        if spec["kind"] == "broadcast":
            outbox(
                state,
                f"broadcast/{index}",
                "broadcast",
                spec["topic"],
                spec["input"],
                now,
                handlers=handler_map,
                attempt=1,
            )
        state["status"] = "WAITING"
        event(state, "command_scheduled", now, command_id=index, operation=spec["kind"])

    async def _dispatch_task(self, state: State, node: State, now: float) -> None:
        current = node["attempts"][-1]
        if (
            node.get("cancel_requested")
            or current["observation"] is not None
            or current["deferred"] is not None
            or current["not_before"] > now
            or current["lease_until"] > now
        ):
            return
        if current["dispatched"] and now - current["last_dispatch"] < self.reconcile_interval:
            return
        topic = route(self.namespace, node["spec"]["ref"])
        await self.transport.ensure(topic, "workers")
        event_id = outbox(
            state,
            f"task/{node['id']}/{current['number']}",
            "task",
            topic,
            node["spec"]["input"],
            now,
            node_id=node["id"],
            attempt=current["number"],
        )
        state["outbox"][event_id]["delivered"] = False
        current["dispatched"], current["last_dispatch"] = True, now

    async def _child(self, state: State, node: State, now: float) -> None:
        spec = node["spec"]
        definition = self.registry.resolve(f"{spec['ref']['name']}:v{spec['ref']['version']}")
        if definition.ref.descriptor() != spec["ref"]:
            raise ProtocolError("Child contract mismatch")
        try:
            child = await self.client.start(
                definition.ref,
                decode(spec["input"], definition.ref.input_type),
                request_id=f"child/{state['run_id']}/{node['id']}",
                _run_id=node["child_run_id"],
            )
        except Conflict as exc:
            raise WorkflowBlocked("Child identity or pinned implementation conflict") from exc
        result = await child.describe()
        for _ in range(100):
            if result["status"] != "CONTINUED":
                break
            result = await self.store.load(self.namespace, result["continued_run_id"])
        if result["status"] == "COMPLETED":
            finish_node(state, node, now, result=result["result"])
        elif result["status"] in TERMINAL:
            finish_node(state, node, now, error=error_data("CHILD_FAILED", result["status"]))
        elif result["status"] == "BLOCKED":
            raise WorkflowBlocked(f"Child {child.run_id} is blocked")

    async def _rollover(self, state: State, node: State, now: float) -> bool:
        if any(
            other["id"] != node["id"] and other["state"] in {"pending", "blocked"} for other in state["nodes"].values()
        ):
            raise ProtocolError("Resolve outstanding operations before continue_as_new")
        definition = self.registry.resolve(f"{state['manifest']['name']}:v{state['manifest']['version']}")
        if definition.manifest != state["manifest"]:
            raise ProtocolError("Pinned implementation mismatch during rollover")
        value = encode(decode(node["spec"]["input"], definition.ref.input_type), definition.ref.input_type)
        run_id = identity(state["run_id"], "continue")
        new = new_run(definition, self.namespace, run_id, state["workflow_id"], value, now, tuple(state["tags"]))
        new["signals"] = clone([signal for signal in state["signals"] if not signal["consumed"]])
        new["signal_keys"] = clone(state["signal_keys"])
        state["signals"] = [signal for signal in state["signals"] if signal["consumed"]]
        finish_node(state, node, now)
        collect(state, now)
        state["status"], state["finished_at"], state["continued_run_id"] = "CONTINUED", now, run_id
        event(state, "continued", now, next_run_id=run_id)
        return await self.store.rollover(state, state["revision"], new)

    async def advance(self, run_id: str) -> None:
        state = await self.store.load(self.namespace, run_id)
        if state["archived"] or state["status"] == "BLOCKED":
            return
        original, now = fingerprint(state), self.clock.now()
        try:
            for node in state["nodes"].values():
                if node["state"] != "pending":
                    continue
                kind = node["spec"]["kind"]
                if state["status"] in {"CANCELLING", "TERMINATED"}:
                    node["cancel_requested"] = True
                if kind == "call":
                    settle_task(state, node, now)
                    if node["state"] == "pending":
                        await self._dispatch_task(state, node, now)
                elif node.get("cancel_requested"):
                    finish_node(state, node, now, error=error_data("CANCELLED"))
                elif kind == "sleep" and node["due_at"] <= now:
                    finish_node(state, node, now)
                elif kind == "signal":
                    candidate = next(
                        (
                            signal
                            for signal in state["signals"]
                            if not signal["consumed"] and signal["name"] == node["spec"]["name"]
                        ),
                        None,
                    )
                    if candidate is not None:
                        if candidate["schema"] != node["spec"]["schema"]:
                            raise ProtocolError("Buffered signal contract mismatch")
                        candidate["consumed"], candidate["node_id"] = True, node["id"]
                        finish_node(state, node, now, result=candidate["payload"])
                    elif node["due_at"] is not None and node["due_at"] <= now:
                        finish_node(state, node, now, error=error_data("SIGNAL_TIMEOUT"))
                elif kind == "publish" and state["outbox"][node["outbox_id"]]["delivered"]:
                    finish_node(state, node, now, result=node["outbox_id"])
                elif kind == "child":
                    await self._child(state, node, now)
                elif kind == "continue":
                    if not await self._rollover(state, node, now):
                        self.metrics["cas_conflicts"] += 1
                    return
            collect(state, now)
            if state["status"] == "CANCELLING":
                if all(node["state"] != "pending" for node in state["nodes"].values()):
                    state["status"], state["finished_at"] = "CANCELLED", now
                    event(state, "cancelled", now)
            elif state["status"] not in TERMINAL and state["status"] != "BLOCKED":
                definition = self.registry.resolve(f"{state['manifest']['name']}:v{state['manifest']['version']}")
                activation = replay(definition, state)
                self.metrics["activations"] += 1
                if activation.kind == "schedule":
                    await self._schedule(state, activation.value, now)
                elif activation.kind == "completed":
                    state["status"], state["result"], state["finished_at"] = "COMPLETED", activation.value, now
                    event(state, "completed", now)
                elif activation.kind == "failed":
                    state["status"], state["error"], state["finished_at"] = "FAILED", activation.value, now
                    event(state, "failed", now, code=activation.value["code"])
                else:
                    state["status"] = "WAITING"
        except (ProtocolError, WorkflowBlocked) as exc:
            if state["status"] not in TERMINAL:
                state["status"], state["blocked_reason"] = "BLOCKED", str(exc)[:1000]
                event(state, "blocked", now, reason=state["blocked_reason"])
        if fingerprint(state) != original and not await self.store.save(state, state["revision"]):
            self.metrics["cas_conflicts"] += 1
        await self._close_children(run_id)

    async def _close_children(self, run_id: str) -> None:
        state = await self.store.load(self.namespace, run_id)
        if state["status"] not in TERMINAL | {"CANCELLING"}:
            return
        for node in state["nodes"].values():
            if node["spec"]["kind"] != "child" or node["spec"].get("abandon"):
                continue
            try:
                child_id = node["child_run_id"]
                child = await self.store.load(self.namespace, child_id)
                for _ in range(100):
                    if child["status"] != "CONTINUED":
                        break
                    child_id = child["continued_run_id"]
                    child = await self.store.load(self.namespace, child_id)
                if child["status"] not in TERMINAL | {"CANCELLING"}:
                    await self.client.get_handle(child_id).cancel(
                        actor="parent", reason="Parent closed", request_id=f"parent-close/{run_id}"
                    )
            except (Conflict, NotFound):
                pass

    async def flush(self, run_id: str, limit: int = 32) -> None:
        for _ in range(limit):
            owner, now = str(uuid4()), self.clock.now()

            def claim(state: State) -> Any:
                for event_id, item in state["outbox"].items():
                    if item["delivered"] or item["lease_until"] > now:
                        continue
                    if item["metadata"]["kind"] != "wake" and state["status"] in {
                        "CANCELLING",
                        "CANCELLED",
                        "TERMINATED",
                    }:
                        item["delivered"], item["suppressed"] = True, True
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
                await self.transport.publish(
                    item["topic"], canonical(item["payload"]).encode(), {"duraflow": canonical(item["metadata"])}
                )
            except Exception:
                self.metrics["transport_errors"] += 1

                def release(state: State) -> None:
                    current = state["outbox"].get(event_id)
                    if current and current["owner"] == owner:
                        current["lease_until"], current["owner"] = 0, None

                await mutate(self.store, self.namespace, run_id, release)
                raise

            def delivered(state: State) -> None:
                current = state["outbox"].get(event_id)
                if current and current["owner"] == owner:
                    current["delivered"], current["lease_until"] = True, 0

            await mutate(self.store, self.namespace, run_id, delivered)
            self.metrics["publications"] += 1

    async def tick(self) -> int:
        rows = await self.store.scan(self.namespace, self.cursor, self.batch_size)
        if not rows and self.cursor:
            self.cursor = ""
            rows = await self.store.scan(self.namespace, "", self.batch_size)
        self.cursor = rows[-1]["run_id"] if len(rows) == self.batch_size else ""
        failures = []
        for state in rows:
            try:
                await self.advance(state["run_id"])
                await self.flush(state["run_id"])
            except Exception as exc:
                failures.append(exc)
        if failures:
            raise failures[0]
        return len(rows)

    async def run(self, stop: asyncio.Event, *, poll_interval: float = 0.1) -> None:
        duration(poll_interval)
        while not stop.is_set():
            try:
                await self.tick()
            except Exception as exc:
                log.error("Engine iteration failed", extra={"error_type": type(exc).__name__})
            try:
                await asyncio.wait_for(stop.wait(), timeout=poll_interval)
            except TimeoutError:
                pass
