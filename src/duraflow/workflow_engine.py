"""Message-driven workflow state owner. All transitions are synchronous and atomic."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from .channels import SignalFilter
from .contracts import Conflict, ProtocolError, Registry, TERMINAL, fingerprint, duration
from .retention import RetentionPolicy
from .transitions import collect, settle_task
from .message_runtime import Consumer, OutboxRelay
from .message_store import MessageStore
from .messaging import Message, Publication, Topics, RetryLater
from .state import attempt, event, finish_node, identity
from .storage import clone
from .transport import Delivery, Transport


def new_state(body: dict[str, Any], now: float) -> dict[str, Any]:
    state = {
        "namespace": body["namespace"],
        "workflow_id": body["workflow_id"],
        "run_id": body["run_id"],
        "manifest": body["manifest"],
        "input": body["input"],
        "status": "PENDING",
        "execution_protocol": 2,
        "lifecycle_version": 2,
        "codec_version": 1,
        "commands": [],
        "nodes": {},
        "history": [],
        "sequence": 0,
        "channels": {},
        "signal_keys": {},
        "tags": body.get("tags", []),
        "result": None,
        "error": None,
        "blocked_reason": None,
        "created_at": now,
        "finished_at": None,
        "continued_run_id": None,
        "archived": False,
        "actions": {},
    }
    event(state, "started", now)
    return state


class Transition:
    """One message's state transition and outgoing intent, executed under a lock."""

    def __init__(
        self,
        aggregate: dict[str, Any],
        message: Message,
        now: float,
        topics: Topics,
        retention_policy: RetentionPolicy | None = None,
        max_commands: int = 1000,
    ):
        self.aggregate, self.message, self.now, self.topics = aggregate, message, now, topics
        self.outgoing: list[Publication] = []
        self.retention_policy = retention_policy or RetentionPolicy()
        self.max_commands = max_commands

    @property
    def state(self) -> dict[str, Any]:
        return self.aggregate["runs"][self.aggregate["current"]]

    def emit(
        self,
        kind: str,
        body: dict[str, Any],
        *,
        topic: str | None = None,
        key: str | None = None,
        subscription: str | None = "state",
        deliver_at: float | None = None,
    ) -> str:
        self.aggregate["emitted"] = self.aggregate.get("emitted", 0) + 1
        if deliver_at is not None:
            body = {**body, "not_before": deliver_at}
        message = Message(
            kind,
            key or self.message.key,
            clone(body),
        )
        # Allocate a new timeline identity inside the state/outbox transaction.
        # Once committed, relay retries retain this exact envelope. Deriving it
        # from a counter would reuse discarded messages after database rollback.
        target = topic or self.topics.workflow(self.aggregate["workflow"])
        if deliver_at is not None and topic is None:
            target, subscription = self.topics.topic("timer", self.aggregate["workflow"]), "timers"
        self.outgoing.append(Publication(target, message, deliver_at, subscription))
        return message.id

    def respond(self, request: dict[str, Any], value: Any = None, error: str | None = None) -> None:
        if "reply_to" in request and "correlation_id" in request:
            self.emit(
                "response",
                {"correlation_id": request["correlation_id"], "value": value, "error": error},
                topic=request["reply_to"],
                key=request["correlation_id"],
                subscription="client",
            )

    def activate(self) -> None:
        state = self.state
        if self.aggregate.get("active") or state["status"] in TERMINAL | {"BLOCKED", "CANCELLING"}:
            return
        self.aggregate["activation_count"] = self.aggregate.get("activation_count", 0) + 1
        activation_id = str(uuid4())
        self.aggregate["active"] = {"id": activation_id, "run_id": state["run_id"]}
        self.emit(
            "activate",
            {
                "snapshot": state,
                "activation_id": activation_id,
                "reply_to": self.topics.workflow(self.aggregate["workflow"]),
            },
            topic=self.topics.replay(self.aggregate["workflow"], state["manifest"]["build_id"]),
            subscription="replay",
        )

    def start(self, body: dict[str, Any]) -> None:
        digest = fingerprint([body["manifest"], body["input"], sorted(body.get("tags", []))])
        if self.aggregate.get("runs"):
            previous = self.aggregate["requests"].get(body["request_id"])
            if previous is None or previous["digest"] != digest:
                self.respond(body, error="Conflict")
            elif self.aggregate.get("tags_pending"):
                self.aggregate["start_waiters"].append(body)
            else:
                self.respond(body, {"run_id": previous["run_id"]})
            return
        if body["namespace"] != self.topics.namespace:
            raise ProtocolError("Wrong namespace")
        expected = self.topics.instance(body["manifest"]["name"], body["workflow_id"])
        if expected != self.message.key:
            raise ProtocolError("Workflow identity does not match message key")
        state = new_state(body, self.now)
        self.aggregate.update(
            workflow=body["manifest"]["name"],
            workflow_id=body["workflow_id"],
            current=body["run_id"],
            runs={body["run_id"]: state},
            requests={body["request_id"]: {"digest": digest, "run_id": body["run_id"]}},
            active=None,
            buffered=[],
            waiters=[],
            parent=body.get("parent"),
        )
        self.aggregate["tags_pending"] = list(state["tags"])
        self.aggregate["start_waiters"] = [body] if state["tags"] else []
        if not state["tags"]:
            self.respond(body, {"run_id": state["run_id"]})
        for tag in state["tags"]:
            self.emit(
                "tag_add",
                {
                    "tag": tag,
                    "workflow": self.aggregate["workflow"],
                    "workflow_id": state["workflow_id"],
                    "reply_to": self.topics.workflow(self.aggregate["workflow"]),
                    "workflow_key": self.message.key,
                },
                topic=self.topics.tags(),
                key=f"{self.topics.namespace}/{tag}",
                subscription="tags",
            )
        if not state["tags"]:
            self.activate()
        for query in self.aggregate.pop("early_queries", []):
            self.query(query, result=True)

    def query(self, body: dict[str, Any], *, result: bool = False) -> None:
        run_id = body.get("run_id") or self.aggregate.get("current")
        state = self.aggregate.get("runs", {}).get(run_id)
        if state is None:
            if result and body.get("request_id") and not self.aggregate.get("runs"):
                pending = self.aggregate.setdefault("early_queries", [])
                if len(pending) >= 1000:
                    self.respond(body, error="WaiterLimit")
                else:
                    pending.append(body)
            else:
                self.respond(body, error="NotFound")
        elif result and state["archived"]:
            self.respond(body, error="Archived")
        elif result and state["status"] not in TERMINAL | {"BLOCKED"}:
            if len(self.aggregate["waiters"]) >= 1000:
                self.respond(body, error="WaiterLimit")
            else:
                self.aggregate["waiters"].append({**body, "run_id": run_id})
        elif result:
            self.respond(
                body,
                {
                    "status": state["status"],
                    "result": state["result"],
                    "error": state["error"],
                    "blocked_reason": state["blocked_reason"],
                    "continued_run_id": state["continued_run_id"],
                },
            )
        else:
            self.respond(body, state)

    def reserve_task(self, node: dict[str, Any]) -> None:
        state, current = self.state, node["attempts"][-1]
        ref = node["spec"]["ref"]
        dispatch_id = identity(state["run_id"], f"task/{node['id']}/{current['number']}")
        current["dispatch_id"] = dispatch_id
        current["dispatched"] = True
        body = {
            "run_id": state["run_id"],
            "node_id": node["id"],
            "task_id": node["task_id"],
            "attempt": current["number"],
            "dispatch_id": dispatch_id,
            "ref": ref,
            "input": node["spec"]["input"],
            "namespace": self.topics.namespace,
            "reply_to": self.topics.workflow(self.aggregate["workflow"]),
            "workflow_key": self.message.key,
            "tags": node["spec"].get("tags", []),
        }
        self.emit(
            "execute_task",
            body,
            topic=self.topics.task(ref["name"], ref["version"]),
            key=dispatch_id,
            subscription="tasks",
            deliver_at=current["not_before"] if current["not_before"] > self.now else None,
        )
        options = node["spec"]["options"]
        for setting, base in (("overall_timeout", node["created_at"]), ("schedule_timeout", current["not_before"])):
            if options[setting] is not None:
                self.emit(
                    "deadline",
                    {"run_id": state["run_id"], "node_id": node["id"], "dispatch_id": dispatch_id},
                    deliver_at=base + options[setting],
                )

    def command(self, spec: dict[str, Any]) -> None:
        state = self.state
        if len(state["commands"]) >= self.max_commands:
            raise ProtocolError("Workflow history limit reached")
        index = str(len(state["commands"]))
        members = spec.get("members", [spec])
        if len(members) > 1000:
            raise ProtocolError("Group size exceeds 1000")
        record = {"id": index, "spec": clone(spec), "state": "pending", "members": [], "result": None, "error": None}
        for position, member in enumerate(members):
            node_id = f"{index}.{position}"
            node = {
                "id": node_id,
                "spec": clone(member),
                "state": "pending",
                "created_at": self.now,
                "result": None,
                "error": None,
                "accepted_seq": None,
                "cancel_requested": False,
            }
            state["nodes"][node_id] = node
            record["members"].append(node_id)
            kind = member["kind"]
            if kind == "call":
                node["task_id"], node["attempts"] = identity(state["run_id"], f"task/{node_id}"), [attempt(1, self.now)]
                if spec["kind"] != "broadcast":
                    self.reserve_task(node)
                else:
                    node["attempts"][0]["dispatch_id"] = identity(state["run_id"], f"task/{node_id}/1")
            elif kind == "sleep":
                node["due_at"] = self.now + member["seconds"]
                self.emit("timer", {"run_id": state["run_id"], "node_id": node_id}, deliver_at=node["due_at"])
            elif kind == "signal":
                raise ProtocolError("Use a typed channel receive() in protocol 2")
            elif kind == "channel_open":
                stream_id = member["handle"]
                state["channels"][stream_id] = {**member, "received": [], "accepted": 0}
                carried = state.get("carried_signals", [])
                for item in list(carried):
                    if item["channel"] == member["channel"] and item["schema"] == member["schema"]:
                        state["channels"][stream_id]["received"].append(
                            {"id": item["id"], "payload": item["payload"], "consumed": False}
                        )
                        state["channels"][stream_id]["accepted"] += 1
                        carried.remove(item)
                finish_node(state, node, self.now)
            elif kind == "channel_next":
                if member["stream_id"] not in state["channels"]:
                    raise ProtocolError("Unknown channel stream")
                node["due_at"] = None if member["timeout"] is None else self.now + member["timeout"]
                if node["due_at"] is not None:
                    self.emit("timer", {"run_id": state["run_id"], "node_id": node_id}, deliver_at=node["due_at"])
            elif kind == "publish":
                self.emit(
                    "publish",
                    {
                        "payload": member["input"],
                        "confirmation": {"run_id": state["run_id"], "node_id": node_id},
                        "reply_to": self.topics.workflow(self.aggregate["workflow"]),
                    },
                    topic=member["topic"],
                    subscription=None,
                )
            elif kind == "now":
                finish_node(state, node, self.now, result=datetime.fromtimestamp(self.now, timezone.utc).isoformat())
            elif kind == "uuid":
                finish_node(state, node, self.now, result=str(uuid4()))
            elif kind == "future":
                pass
            elif kind == "send_signal":
                self.emit(
                    "signal",
                    {**member, "schema": member["schema"]},
                    topic=self.topics.workflow(member["workflow"]),
                    key=self.topics.instance(member["workflow"], member["workflow_id"]),
                )
                finish_node(state, node, self.now)
            elif kind == "child":
                child_id = identity(state["run_id"], f"child/{node_id}")
                node["child_run_id"] = child_id
                manifest = member["manifest"]
                self.emit(
                    "start",
                    {
                        "namespace": self.topics.namespace,
                        "manifest": manifest,
                        "run_id": child_id,
                        "workflow_id": child_id,
                        "request_id": child_id,
                        "input": member["input"],
                        "tags": [],
                        "parent": {
                            "topic": self.topics.workflow(self.aggregate["workflow"]),
                            "key": self.message.key,
                            "run_id": state["run_id"],
                            "node_id": node_id,
                        },
                    },
                    topic=self.topics.workflow(manifest["name"]),
                    key=self.topics.instance(manifest["name"], child_id),
                )
            elif kind == "continue":
                self.rollover(node)
            else:
                raise ProtocolError(f"Unsupported operation: {kind}")
        state["commands"].append(record)
        if spec["kind"] == "broadcast":
            handlers = {
                member["handler"]: {
                    "node_id": node_id,
                    "task_id": state["nodes"][node_id]["task_id"],
                    "dispatch_id": state["nodes"][node_id]["attempts"][0]["dispatch_id"],
                    "ref": member["ref"],
                }
                for member, node_id in zip(members, record["members"], strict=True)
            }
            self.emit(
                "broadcast",
                {
                    "payload": spec["input"],
                    "handlers": handlers,
                    "run_id": state["run_id"],
                    "namespace": self.topics.namespace,
                    "reply_to": self.topics.workflow(self.aggregate["workflow"]),
                    "workflow_key": self.message.key,
                },
                topic=spec["topic"],
                subscription=None,
            )
        state["status"] = "WAITING" if state["status"] not in TERMINAL else state["status"]
        event(state, "command_scheduled", self.now, command_id=index, operation=spec["kind"])

    def rollover(self, node: dict[str, Any]) -> None:
        state = self.state
        if any(n["id"] != node["id"] and n["state"] in {"pending", "blocked"} for n in state["nodes"].values()):
            raise ProtocolError("Resolve outstanding operations before rollover")
        run_id = identity(state["run_id"], "continue")
        new = new_state(
            {
                "namespace": self.topics.namespace,
                "workflow_id": state["workflow_id"],
                "run_id": run_id,
                "manifest": state["manifest"],
                "input": node["spec"]["input"],
                "tags": state["tags"],
            },
            self.now,
        )
        new["signal_keys"] = clone(state["signal_keys"])
        new["carried_signals"] = [
            {"channel": channel["channel"], "schema": channel["schema"], "payload": item["payload"], "id": item["id"]}
            for channel in state["channels"].values()
            for item in channel["received"]
            if not item["consumed"]
        ]
        finish_node(state, node, self.now)
        state["status"], state["finished_at"], state["continued_run_id"] = "CONTINUED", self.now, run_id
        self.aggregate["runs"][run_id] = new
        self.aggregate["current"] = run_id

    def signal(self, body: dict[str, Any]) -> None:
        state = self.state
        digest = fingerprint([body["channel"], body["schema"], body["payload"]])
        previous = state["signal_keys"].get(body["signal_id"])
        if previous is not None:
            self.respond(body, error="Conflict" if previous != digest else None)
            return
        if state["status"] in TERMINAL:
            self.respond(body, error="Conflict")
            return
        if len(state["signal_keys"]) >= 10000:
            self.respond(body, error="SignalLimit")
            return
        state["signal_keys"][body["signal_id"]] = digest
        accepted = 0
        for stream in state["channels"].values():
            if stream["channel"] != body["channel"] or stream["schema"] != body["schema"]:
                continue
            if stream["accepted_schema"] != stream["schema"] and stream["accepted_schema"] != body.get(
                "payload_schema"
            ):
                continue
            if stream["max_signals"] is not None and stream["accepted"] >= stream["max_signals"]:
                continue
            if not SignalFilter(stream["filter"]).matches(body["payload"]):
                continue
            if len(stream["received"]) >= 1000:
                raise ProtocolError("Channel buffer capacity exceeded")
            stream["accepted"] += 1
            stream["received"].append({"id": body["signal_id"], "payload": body["payload"], "consumed": False})
            accepted += 1
        event(
            state,
            "signal_received" if accepted else "signal_discarded",
            self.now,
            signal_id=body["signal_id"],
            channel=body["channel"],
        )
        self.respond(body, {"receivers": accepted})

    def settle(self) -> None:
        state = self.state
        for node in state["nodes"].values():
            if node["state"] != "pending":
                continue
            spec = node["spec"]
            if spec["kind"] == "channel_next":
                stream = state["channels"][spec["stream_id"]]
                item = next((entry for entry in stream["received"] if not entry["consumed"]), None)
                if item is not None:
                    item["consumed"] = True
                    item["consumed_by"] = node["id"]
                    finish_node(state, node, self.now, result=item["payload"])
            elif spec["kind"] == "future":
                target = next((n for n in state["nodes"].values() if n["spec"].get("handle") == spec["handle"]), None)
                if target is None:
                    raise ProtocolError("Unknown deferred operation")
                if target["state"] in {"done", "error"}:
                    finish_node(state, node, self.now, result=target["result"], error=target["error"])
                    node["accepted_seq"] = target["accepted_seq"]
        collect(state, self.now)

    def task_event(self, kind: str, body: dict[str, Any]) -> None:
        state = self.state
        node = state["nodes"].get(body["node_id"])
        if node is None:
            raise ProtocolError("Result arrived before its committed command")
        if node["spec"]["kind"] != "call" or node["state"] != "pending":
            return
        current = node["attempts"][-1]
        if body["dispatch_id"] != current["dispatch_id"]:
            return
        if kind == "task_started":
            if current["started_at"] is None:
                current["started_at"] = self.now
                timeout = node["spec"]["options"]["attempt_timeout"]
                if timeout is not None:
                    self.emit(
                        "deadline",
                        {"run_id": state["run_id"], "node_id": node["id"], "dispatch_id": current["dispatch_id"]},
                        deliver_at=self.now + timeout,
                    )
            return
        if current["started_at"] is None:
            current["started_at"] = body.get("started_at", self.now)
        if current["observation"] is not None:
            return
        current["observation"] = {
            "result": body.get("result"),
            "error": body.get("error"),
            "recorded_at": self.now,
            "sequence": event(state, "task_observed", self.now, node_id=node["id"]),
        }
        current["lease_until"] = self.now + 1
        settle_task(state, node, self.now)
        if node["state"] == "pending" and node["attempts"][-1] is not current:
            self.reserve_task(node)

    def control(self, body: dict[str, Any]) -> None:
        state = self.aggregate["runs"].get(body.get("run_id") or self.aggregate["current"], self.state)
        action = body["action"]
        digest = fingerprint([action, body["actor"], body["reason"], body.get("node_id")])
        previous = state["actions"].get(body["request_id"])
        if previous:
            self.respond(body, error="Conflict" if previous != digest else None)
            return
        if action == "archive":
            retention, horizon = body["retention"], body["safety_horizon"]
            duration(retention)
            duration(horizon)
            try:
                self.retention_policy.validate(retention, horizon)
            except Conflict:
                self.respond(body, error="Conflict")
                return
            if (
                retention < horizon
                or state["status"] not in TERMINAL
                or state["finished_at"] is None
                or self.now - state["finished_at"] < retention
                or any(n["state"] in {"pending", "blocked"} for n in state["nodes"].values())
            ):
                self.respond(body, error="Conflict")
                return
            state["archived"] = True
            state["input"], state["result"], state["error"] = None, None, None
            state["commands"], state["history"], state["channels"], state["nodes"] = [], [], {}, {}
            state["actions"][body["request_id"]] = digest
            event(state, "archived", self.now, actor=body["actor"], reason=body["reason"])
            self.respond(body)
            return
        if state["status"] in TERMINAL:
            self.respond(body, error="Conflict")
            return
        if action in {"cancel", "terminate"}:
            for node in state["nodes"].values():
                if node["state"] not in {"pending", "blocked"}:
                    continue
                if node["spec"]["kind"] == "call":
                    ref = node["spec"]["ref"]
                    self.emit(
                        "cancel_task",
                        {"dispatch_id": node["attempts"][-1]["dispatch_id"]},
                        topic=self.topics.task_control(ref["name"], ref["version"]),
                        key=node["attempts"][-1]["dispatch_id"],
                        subscription="tasks",
                    )
                elif node["spec"]["kind"] == "child" and not node["spec"].get("abandon"):
                    child_id = node["child_run_id"]
                    self.emit(
                        "control",
                        {
                            "action": "cancel",
                            "actor": "parent",
                            "reason": "Parent closed",
                            "request_id": identity(state["run_id"], node["id"] + "/cancel"),
                        },
                        topic=self.topics.control(node["spec"]["ref"]["name"]),
                        key=self.topics.instance(node["spec"]["ref"]["name"], child_id),
                    )
                node["state"] = "pending"
                finish_node(
                    state,
                    node,
                    self.now,
                    error={"code": "CANCELLED", "message": "Cancellation requested; external outcome may be unknown"},
                )
            state["status"] = "CANCELLED" if action == "cancel" else "TERMINATED"
            state["finished_at"] = self.now
        elif action == "resume":
            if state["status"] != "BLOCKED" or any(n["state"] == "blocked" for n in state["nodes"].values()):
                self.respond(body, error="Conflict")
                return
            state["status"], state["blocked_reason"] = "WAITING", None
        elif action == "retry":
            node = state["nodes"].get(body.get("node_id"))
            if state["status"] != "BLOCKED" or node is None or node["state"] != "blocked":
                self.respond(body, error="Conflict")
                return
            node["state"] = "pending"
            node["attempts"].append(attempt(node["attempts"][-1]["number"] + 1, self.now))
            self.reserve_task(node)
            state["status"] = "BLOCKED" if any(n["state"] == "blocked" for n in state["nodes"].values()) else "WAITING"
        else:
            raise ProtocolError("Unknown control")
        state["actions"][body["request_id"]] = digest
        event(state, "operator_action", self.now, action=action, actor=body["actor"], reason=body["reason"])
        self.respond(body)

    def handle(self, message: Message | None = None, *, buffered: bool = False) -> None:
        message = message or self.message
        body, kind = message.body, message.kind
        if kind == "start":
            self.start(body)
            return
        if kind in {"query", "result"}:
            self.query(body, result=kind == "result")
            return
        if kind == "remove_waiter":
            self.aggregate["waiters"] = [
                item for item in self.aggregate.get("waiters", []) if item["correlation_id"] != body["correlation_id"]
            ]
            self.aggregate["early_queries"] = [
                item
                for item in self.aggregate.get("early_queries", [])
                if item["correlation_id"] != body["correlation_id"]
            ]
            return
        if kind == "tag_registered" and self.aggregate.get("runs"):
            pending_tags = self.aggregate.get("tags_pending", [])
            if body["tag"] in pending_tags:
                pending_tags.remove(body["tag"])
            if not pending_tags:
                for request in self.aggregate.get("start_waiters", []):
                    self.respond(request, {"run_id": self.aggregate["requests"][request["request_id"]]["run_id"]})
                self.aggregate["start_waiters"] = []
                self.activate()
            return
        if not self.aggregate.get("runs"):
            if kind == "signal" or (kind == "control" and "correlation_id" in body):
                self.respond(body, error="NotFound")
                return
            # Retain causally early signals/results until their start arrives.
            pending = self.aggregate.setdefault("early", [])
            if len(pending) >= 1000:
                raise ProtocolError("Pre-start buffer capacity exceeded")
            pending.append(asdict(message))
            return
        if self.aggregate.get("active") and kind not in {"activation_result", "control"} and not buffered:
            if len(self.aggregate["buffered"]) >= 1000:
                raise ProtocolError("Activation buffer capacity exceeded")
            self.aggregate["buffered"].append(asdict(message))
            return
        state = self.state
        if body.get("run_id", state["run_id"]) not in {None, state["run_id"]} and not (
            kind == "control" and body.get("action") == "archive"
        ):
            self.respond(body, error="Conflict")
            return
        if kind == "activation_result":
            active = self.aggregate.get("active")
            if active is None or active["id"] != body["activation_id"]:
                return
            self.aggregate["active"] = None
            outcome = body["kind"]
            if state["status"] not in TERMINAL:
                if outcome == "schedule":
                    for spec in body["value"]:
                        self.command(spec)
                    self.settle()
                elif outcome in {"completed", "failed", "blocked"}:
                    state["status"] = {"completed": "COMPLETED", "failed": "FAILED", "blocked": "BLOCKED"}[outcome]
                    field = {"completed": "result", "failed": "error", "blocked": "blocked_reason"}[outcome]
                    state[field] = body["value"]
                    state["finished_at"] = self.now if outcome != "blocked" else None
                    event(state, outcome, self.now)
                elif outcome != "waiting":
                    raise ProtocolError("Unknown activation outcome")
            pending = self.aggregate.pop("early", []) + self.aggregate["buffered"]
            self.aggregate["buffered"] = []
            for item in pending:
                self.handle(Message(**item), buffered=True)
        elif state["status"] in TERMINAL and kind not in {
            "task_started",
            "task_result",
            "timer",
            "deadline",
            "published",
            "child_completed",
            "control",
        }:
            self.respond(body, error="Conflict")
        elif kind == "signal":
            self.signal(body)
        elif kind in {"task_started", "task_result"}:
            self.task_event(kind, body)
        elif kind == "task_delegated":
            node = state["nodes"].get(body["node_id"])
            if node and node["state"] == "pending" and node["attempts"][-1]["dispatch_id"] == body["dispatch_id"]:
                node["attempts"][-1]["deferred"] = {"expires_at": body["expires_at"]}
                self.emit(
                    "deadline",
                    {"run_id": state["run_id"], "node_id": node["id"], "dispatch_id": body["dispatch_id"]},
                    deliver_at=body["expires_at"],
                )
        elif kind in {"timer", "deadline"}:
            node = state["nodes"].get(body["node_id"])
            if node and node["state"] == "pending":
                if kind == "timer" and node.get("due_at") is not None and node["due_at"] <= self.now:
                    if node["spec"]["kind"] == "sleep":
                        finish_node(state, node, self.now)
                    elif node["spec"]["kind"] == "channel_next":
                        self.settle()
                        finish_node(
                            state, node, self.now, error={"code": "SIGNAL_TIMEOUT", "message": "Signal timeout"}
                        )
                elif kind == "deadline" and node["attempts"][-1]["dispatch_id"] == body["dispatch_id"]:
                    previous = node["attempts"][-1]
                    settle_task(state, node, self.now)
                    if node["state"] == "pending" and node["attempts"][-1] is not previous:
                        self.reserve_task(node)
        elif kind == "published":
            node = state["nodes"].get(body["node_id"])
            if node:
                finish_node(state, node, self.now, result=message.id)
        elif kind == "child_completed":
            node = state["nodes"].get(body["node_id"])
            if (
                node
                and node["spec"]["kind"] == "child"
                and node["child_run_id"] == body.get("child_id")
                and node["spec"]["ref"]["name"] == body.get("workflow")
            ):
                finish_node(
                    state,
                    node,
                    self.now,
                    result=body["result"],
                    error=None
                    if body["status"] == "COMPLETED"
                    else {"code": "CHILD_FAILED", "message": body["status"]},
                )
        elif kind == "control":
            self.control(body)
            if self.state["status"] in TERMINAL:
                self.aggregate["active"] = None
                for pending in self.aggregate["buffered"]:
                    self.respond(pending["body"], error="Conflict")
                self.aggregate["buffered"] = []
        else:
            raise ProtocolError("Unexpected workflow event")
        self.settle()
        self.notify()
        current = self.state
        if not buffered and (not current["commands"] or current["commands"][-1]["state"] != "pending"):
            self.activate()

    def notify(self) -> None:
        remaining = []
        for waiter in self.aggregate.get("waiters", []):
            target = self.aggregate["runs"][waiter["run_id"]]
            if target["status"] in TERMINAL | {"BLOCKED"}:
                self.query(waiter, result=True)
            else:
                remaining.append(waiter)
        self.aggregate["waiters"] = remaining
        state = self.state
        if state["status"] in TERMINAL and not state.get("notified"):
            state["notified"] = True
            for node in state["nodes"].values():
                if node["state"] != "pending":
                    continue
                if node["spec"]["kind"] in {"channel_next", "sleep", "future"}:
                    finish_node(state, node, self.now, error={"code": "CANCELLED", "message": "Workflow closed"})
                elif node["spec"]["kind"] == "child" and not node["spec"].get("abandon"):
                    child_id = node["child_run_id"]
                    self.emit(
                        "control",
                        {
                            "action": "cancel",
                            "actor": "parent",
                            "reason": "Parent closed",
                            "request_id": identity(state["run_id"], node["id"] + "/close"),
                        },
                        topic=self.topics.control(node["spec"]["ref"]["name"]),
                        key=self.topics.instance(node["spec"]["ref"]["name"], child_id),
                    )
            for tag in state["tags"]:
                self.emit(
                    "tag_remove",
                    {"tag": tag, "workflow": self.aggregate["workflow"], "workflow_id": state["workflow_id"]},
                    topic=self.topics.tags(),
                    key=f"{self.topics.namespace}/{tag}",
                    subscription="tags",
                )
            parent = self.aggregate.get("parent")
            if parent:
                self.emit(
                    "child_completed",
                    {
                        "run_id": parent["run_id"],
                        "node_id": parent["node_id"],
                        "status": state["status"],
                        "result": state["result"],
                        "child_id": state["workflow_id"],
                        "workflow": self.aggregate["workflow"],
                    },
                    topic=parent["topic"],
                    key=parent["key"],
                )


class WorkflowEngine(Consumer):
    def __init__(
        self,
        store: MessageStore,
        transport: Transport,
        workflows: Registry | list[str],
        *,
        topics: Topics | None = None,
        concurrency: int = 4,
        retention_policy: RetentionPolicy | None = None,
        max_commands: int = 1000,
    ):
        super().__init__(transport, topics or Topics(), concurrency=concurrency)
        self.store = store
        self.retention_policy = retention_policy or RetentionPolicy()
        if not 1 <= max_commands <= 10000:
            raise ValueError("Invalid workflow history limit")
        self.max_commands = max_commands
        names = (
            {d.ref.name for d in workflows.workflows.values()} if isinstance(workflows, Registry) else set(workflows)
        )
        self.routes = (
            [(self.topics.workflow(n), "state", True) for n in sorted(names)]
            + [(self.topics.commands(n), "state", True) for n in sorted(names)]
            + [(self.topics.control(n), "state", True) for n in sorted(names)]
            + [(self.topics.topic("timer", n), "timers", False) for n in sorted(names)]
        )
        self.relay = OutboxRelay(store, transport)

    async def handle(self, message: Message, delivery: Delivery) -> None:
        if delivery.topic not in {route[0] for route in self.routes}:
            raise ProtocolError("Wrong workflow route")
        from .contracts import parse_json

        key = parse_json(message.key)
        if not isinstance(key, list) or len(key) != 3 or key[0] != self.topics.namespace:
            raise ProtocolError("Invalid workflow key")
        if delivery.topic not in {
            self.topics.workflow(key[1]),
            self.topics.commands(key[1]),
            self.topics.control(key[1]),
            self.topics.topic("timer", key[1]),
        }:
            raise ProtocolError("Workflow key does not match topic")
        if message.kind == "control" and delivery.topic != self.topics.control(key[1]):
            raise ProtocolError("Control commands require the operator topic")
        if delivery.topic == self.topics.commands(key[1]) and message.kind not in {
            "start",
            "query",
            "result",
            "signal",
            "remove_waiter",
        }:
            raise ProtocolError("Public commands cannot inject executor events")
        if delivery.topic == self.topics.commands(key[1]) and "parent" in message.body:
            raise ProtocolError("Parent callbacks are reserved for internal child starts")
        if delivery.topic == self.topics.control(key[1]) and message.kind != "control":
            raise ProtocolError("Unexpected operator message")

        def update(aggregate: dict[str, Any], now: float) -> list[Publication]:
            if message.body.get("not_before", 0) > now:
                raise RetryLater()
            previous = clone(aggregate)
            transition = Transition(aggregate, message, now, self.topics, self.retention_policy, self.max_commands)
            try:
                transition.handle()
            except ProtocolError as exc:
                if (
                    message.kind != "activation_result"
                    or not previous.get("active")
                    or previous["active"]["id"] != message.body.get("activation_id")
                ):
                    raise
                aggregate.clear()
                aggregate.update(previous)
                aggregate["active"] = None
                transition = Transition(aggregate, message, now, self.topics, self.retention_policy, self.max_commands)
                transition.state["status"], transition.state["blocked_reason"] = "BLOCKED", str(exc)[:1000]
                transition.notify()
            except (KeyError, TypeError, ValueError) as exc:
                raise ProtocolError("Malformed workflow command") from exc
            return transition.outgoing

        await self.store.apply(message.key, message, update)

    async def step(self) -> bool:
        sent = await self.relay.step()
        consumed = await super().step()
        return bool(sent or consumed)
