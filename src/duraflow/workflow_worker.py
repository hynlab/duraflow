"""Stateless workflow executors; immutable snapshots in, replay decisions out."""

from __future__ import annotations

from typing import Any

from .contracts import ProtocolError, Registry, WorkflowBlocked
from .message_runtime import Consumer
from .messaging import Message, Topics
from .state import identity
from .transport import Delivery, Transport
from .workflow_replay import execute


class WorkflowWorker(Consumer):
    def __init__(
        self,
        transport: Transport,
        registry: Registry,
        *,
        topics: Topics | None = None,
        replay_executor: Any = None,
        concurrency: int = 2,
    ):
        super().__init__(transport, topics or Topics(), concurrency=concurrency)
        self.registry, self.replay_executor = registry, replay_executor
        self.routes = [
            (self.topics.replay(d.ref.name, d.build_id), "replay", True) for d in registry.workflows.values()
        ]

    async def handle(self, message: Message, delivery: Delivery) -> None:
        if message.kind != "activate":
            raise ProtocolError("Unexpected workflow execution message")
        message.require(snapshot=dict, activation_id=str, reply_to=str)
        body = message.body
        state = body["snapshot"]
        if not all(
            key in state for key in ("manifest", "commands", "nodes", "input", "run_id", "execution_protocol")
        ) or not isinstance(state["manifest"], dict):
            raise ProtocolError("Incomplete workflow snapshot")
        definition = self.registry.match_manifest(state["manifest"])
        if definition is None or delivery.topic != self.topics.replay(definition.ref.name, definition.build_id):
            raise ProtocolError("Workflow implementation route mismatch")
        try:
            activation = (
                await self.replay_executor.execute(definition, state)
                if self.replay_executor
                else execute(definition, state)
            )
            if activation.kind == "schedule":
                for spec in activation.value:
                    members = spec.get("members", [spec])
                    for member in members:
                        if member["kind"] == "child":
                            if "manifest" in member:
                                continue
                            child = self.registry.resolve(f"{member['ref']['name']}:v{member['ref']['version']}")
                            if child.ref.descriptor() != member["ref"]:
                                raise ProtocolError("Child contract mismatch")
                            member["manifest"] = child.manifest
            result = {"kind": activation.kind, "value": activation.value}
        except (ProtocolError, WorkflowBlocked) as exc:
            result = {"kind": "blocked", "value": type(exc).__name__}
        response = Message(
            "activation_result",
            message.key,
            {"run_id": state["run_id"], "activation_id": body["activation_id"], **result},
            id=identity(message.id, "result"),
        )
        await self.transport.publish(body["reply_to"], response.to_bytes(), {}, key=message.key)

    async def close(self) -> None:
        if self.replay_executor:
            await self.replay_executor.close()
