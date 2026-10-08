"""Broker-first Python SDK. Commands and results never require workflow DB access."""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from uuid import uuid4

from .channels import ChannelRef
from .contracts import (
    Archived,
    Conflict,
    NotFound,
    Registry,
    WorkflowBlocked,
    WorkflowFailed,
    WorkflowRef,
    decode,
    encode,
    schema_id,
)
from .messaging import Message, Topics
from .state import identity
from .transport import Transport


class Client:
    def __init__(
        self,
        transport: Transport,
        registry: Registry | None = None,
        *,
        namespace: str = "default",
        topics: Topics | None = None,
        client_id: str | None = None,
        timeout: float = 30,
    ):
        self.transport, self.registry = transport, registry or Registry()
        self.topics = topics or Topics(namespace=namespace)
        self.id = client_id or str(uuid4())
        self.timeout = timeout
        self.replies: dict[str, dict[str, Any]] = {}
        self.lock = asyncio.Lock()
        self.pending: set[str] = set()
        self.closed = False

    async def _send(self, topic: str, message: Message) -> None:
        await self.transport.publish(topic, message.to_bytes(), {}, key=message.key)

    def _contract(self, workflow: Any) -> tuple[WorkflowRef[Any, Any], dict[str, Any]]:
        definition = getattr(workflow, "__duraflow_workflow__", None)
        if definition is not None:
            return definition.ref, definition.manifest
        if isinstance(workflow, WorkflowRef) and workflow.build_id:
            return workflow, workflow.manifest
        definition = self.registry.resolve(workflow)
        return definition.ref, definition.manifest

    async def request(
        self,
        workflow: str,
        workflow_id: str,
        kind: str,
        body: dict[str, Any],
        *,
        timeout: float | None = None,
        topic: str | None = None,
    ) -> Any:
        correlation_id = str(uuid4())
        if self.closed:
            raise RuntimeError("Client is closed")
        reply_to = self.topics.reply(self.id)
        await self.transport.ensure(reply_to, "client")
        message = Message(
            kind,
            self.topics.instance(workflow, workflow_id),
            {**body, "reply_to": reply_to, "correlation_id": correlation_id},
        )
        self.pending.add(correlation_id)
        try:
            target = self.topics.control(workflow) if kind == "control" else self.topics.commands(workflow)
            await self._send(topic or target, message)
            async with asyncio.timeout(self.timeout if timeout is None else timeout):
                while correlation_id not in self.replies:
                    if self.closed:
                        raise RuntimeError("Client is closed")
                    async with self.lock:
                        if correlation_id in self.replies:
                            break
                        delivery = await self.transport.receive(reply_to, "client")
                        if delivery is not None:
                            response = Message.from_bytes(delivery.data)
                            if response.kind != "response":
                                raise ValueError("Unexpected client response")
                            if response.body["correlation_id"] in self.pending:
                                self.replies[response.body["correlation_id"]] = response.body
                            await self.transport.ack(delivery)
                    await asyncio.sleep(0.005)
            reply = self.replies.pop(correlation_id)
        finally:
            self.pending.discard(correlation_id)
            self.replies.pop(correlation_id, None)
            if kind == "result":
                try:
                    async with asyncio.timeout(2):
                        await self._send(
                            self.topics.commands(workflow),
                            Message(
                                "remove_waiter",
                                self.topics.instance(workflow, workflow_id),
                                {"correlation_id": correlation_id},
                            ),
                        )
                except Exception as exc:
                    logging.getLogger("duraflow.messages").warning(
                        "publication_failed", extra={"error_type": type(exc).__name__}
                    )
        error = reply.get("error")
        if error:
            raise {"NotFound": NotFound, "Conflict": Conflict, "Archived": Archived}.get(error, WorkflowFailed)(error)
        return reply.get("value")

    async def dispatch(
        self, workflow: Any, value: Any, *, request_id: str, workflow_id: str | None = None, tags: tuple[str, ...] = ()
    ) -> WorkflowHandle:
        ref, manifest = self._contract(workflow)
        if not request_id or len(request_id) > 256 or len(tags) > 32:
            raise ValueError("Invalid start identity or tags")
        workflow_id = workflow_id or request_id
        if not workflow_id or len(workflow_id) > 256:
            raise ValueError("Invalid workflow identity")
        from .contracts import name

        for tag in tags:
            name(tag)
        run_id = identity(self.topics.instance(ref.name, workflow_id), "run/" + request_id)
        body = {
            "namespace": self.topics.namespace,
            "manifest": manifest,
            "input": encode(value, ref.input_type),
            "run_id": run_id,
            "workflow_id": workflow_id,
            "request_id": request_id,
            "tags": sorted(set(tags)),
        }
        await self._send(
            self.topics.commands(ref.name),
            Message("start", self.topics.instance(ref.name, workflow_id), body),
        )
        return WorkflowHandle(self, ref.name, workflow_id, run_id, ref.output_type, request_id=request_id)

    async def start(
        self, workflow: Any, value: Any, *, request_id: str, workflow_id: str | None = None, tags: tuple[str, ...] = ()
    ) -> WorkflowHandle:
        ref, manifest = self._contract(workflow)
        # dispatch() offers broker acceptance; start() additionally waits for engine acceptance.
        workflow_id = workflow_id or request_id
        if not request_id or len(request_id) > 256 or not workflow_id or len(workflow_id) > 256 or len(tags) > 32:
            raise ValueError("Invalid start identity or tags")
        from .contracts import name

        for tag in tags:
            name(tag)
        run_id = identity(self.topics.instance(ref.name, workflow_id), "run/" + request_id)
        result = await self.request(
            ref.name,
            workflow_id,
            "start",
            {
                "namespace": self.topics.namespace,
                "manifest": manifest,
                "input": encode(value, ref.input_type),
                "run_id": run_id,
                "workflow_id": workflow_id,
                "request_id": request_id,
                "tags": sorted(set(tags)),
            },
        )
        return WorkflowHandle(self, ref.name, workflow_id, result["run_id"], ref.output_type)

    def get_handle(self, workflow: Any, workflow_id: str, *, run_id: str | None = None) -> WorkflowHandle:
        ref = workflow if isinstance(workflow, WorkflowRef) else self._contract(workflow)[0]
        return WorkflowHandle(self, ref.name, workflow_id, run_id, ref.output_type)

    async def signal(
        self,
        workflow: Any,
        workflow_id: str,
        channel: ChannelRef[Any],
        value: Any,
        *,
        signal_id: str,
        payload_type: Any = None,
    ) -> Any:
        return await self.get_handle(workflow, workflow_id).signal(
            channel, value, signal_id=signal_id, payload_type=payload_type
        )

    async def signal_tagged(
        self, workflow: Any, tag: str, channel: ChannelRef[Any], value: Any, *, signal_id: str, payload_type: Any = None
    ) -> None:
        ref = workflow if isinstance(workflow, WorkflowRef) else self._contract(workflow)[0]
        from .contracts import name

        name(tag)
        if not signal_id or len(signal_id) > 256:
            raise ValueError("Invalid signal ID")
        encoded = encode(value, channel.payload_type if payload_type is None else payload_type)
        decode(encoded, channel.payload_type)
        message = Message(
            "tag_signal",
            f"{self.topics.namespace}/{tag}",
            {
                "tag": tag,
                "workflow": ref.name,
                "channel": channel.name,
                "schema": schema_id(channel.payload_type),
                "payload_schema": schema_id(channel.payload_type if payload_type is None else payload_type),
                "payload": encoded,
                "signal_id": signal_id,
            },
        )
        await self._send(self.topics.tags(), message)

    async def complete_external(
        self, token: str, value: Any = None, *, ref: Any, error: dict[str, Any] | None = None
    ) -> bool:
        import base64
        from .contracts import parse_json

        try:
            body = parse_json(base64.urlsafe_b64decode(token))
            if body["ref"] != ref.descriptor():
                raise ValueError()
        except Exception:
            raise ValueError("Invalid external completion token") from None
        return bool(
            await self.request(
                "external",
                body["dispatch_id"],
                "complete_task",
                {**body, "result": encode(value, ref.output_type) if error is None else None, "error": error},
                topic=self.topics.task_completion(ref.name, ref.version),
            )
        )

    async def close(self) -> None:
        """Close the reply subscription after outstanding requests have finished."""
        self.closed = True
        self.replies.clear()
        if not self.pending:
            await self.transport.unsubscribe(self.topics.reply(self.id), "client")

    async def cancel_tasks_tagged(self, ref: Any, tag: str, *, request_id: str) -> None:
        from .contracts import name, canonical

        name(tag)
        if not request_id or len(request_id) > 256:
            raise ValueError("Invalid task tag request ID")
        message = Message(
            "task_tag_cancel",
            canonical([self.topics.namespace, ref.name, ref.version, tag]),
            {"request_id": request_id},
        )
        await self._send(self.topics.task_tags(control=True), message)


class WorkflowHandle:
    def __init__(
        self,
        client: Client,
        workflow: str,
        workflow_id: str,
        run_id: str | None,
        output_type: Any = Any,
        *,
        request_id: str | None = None,
    ):
        self.client, self.workflow, self.workflow_id, self.run_id, self.output_type = (
            client,
            workflow,
            workflow_id,
            run_id,
            output_type,
        )
        self.request_id = request_id

    async def describe(self) -> dict[str, Any]:
        return await self.client.request(self.workflow, self.workflow_id, "query", {"run_id": self.run_id})

    async def history(self, *, after: int = 0, limit: int = 100) -> list[dict[str, Any]]:
        if not 1 <= limit <= 1000:
            raise ValueError("Invalid history limit")
        return [item for item in (await self.describe())["history"] if item["sequence"] > after][:limit]

    async def result(self, *, timeout: float | None = None, follow_continued: bool = True) -> Any:
        async def wait() -> Any:
            handle = self
            while True:
                result = await self.client.request(
                    self.workflow,
                    self.workflow_id,
                    "result",
                    {"run_id": handle.run_id, "request_id": handle.request_id},
                    timeout=timeout or 86400 * 365,
                )
                if result["status"] == "COMPLETED":
                    return decode(result["result"], self.output_type)
                if result["status"] == "BLOCKED":
                    raise WorkflowBlocked(result["blocked_reason"])
                if result["status"] == "CONTINUED" and follow_continued:
                    handle = WorkflowHandle(
                        self.client, self.workflow, self.workflow_id, result["continued_run_id"], self.output_type
                    )
                    continue
                raise WorkflowFailed(result["status"])

        async with asyncio.timeout(timeout):
            return await wait()

    async def signal(self, channel: ChannelRef[Any], value: Any, *, signal_id: str, payload_type: Any = None) -> Any:
        if not signal_id or len(signal_id) > 256:
            raise ValueError("Invalid signal ID")
        encoded = encode(value, channel.payload_type if payload_type is None else payload_type)
        decode(encoded, channel.payload_type)
        return await self.client.request(
            self.workflow,
            self.workflow_id,
            "signal",
            {
                "run_id": self.run_id,
                "channel": channel.name,
                "schema": schema_id(channel.payload_type),
                "payload_schema": schema_id(channel.payload_type if payload_type is None else payload_type),
                "payload": encoded,
                "signal_id": signal_id,
            },
        )

    async def _control(
        self, action: str, *, actor: str, reason: str, request_id: str, node_id: str | None = None
    ) -> None:
        if not actor or not reason or not request_id or len(actor) > 128 or len(reason) > 1000 or len(request_id) > 256:
            raise ValueError("Controls require bounded audit fields")
        await self.client.request(
            self.workflow,
            self.workflow_id,
            "control",
            {
                "run_id": self.run_id,
                "action": action,
                "actor": actor,
                "reason": reason,
                "request_id": request_id,
                "node_id": node_id,
            },
        )

    async def cancel(self, *, actor: str, reason: str, request_id: str) -> None:
        await self._control("cancel", actor=actor, reason=reason, request_id=request_id)

    async def terminate(self, *, actor: str, reason: str, request_id: str) -> None:
        await self._control("terminate", actor=actor, reason=reason, request_id=request_id)

    async def resume_blocked(self, *, actor: str, reason: str, request_id: str) -> None:
        await self._control("resume", actor=actor, reason=reason, request_id=request_id)

    async def retry_blocked_task(self, node_id: str, *, actor: str, reason: str, request_id: str) -> None:
        await self._control("retry", actor=actor, reason=reason, request_id=request_id, node_id=node_id)

    async def archive(self, *, actor: str, reason: str, retention: float, safety_horizon: float) -> None:
        from .contracts import duration

        duration(retention)
        duration(safety_horizon)
        if not actor or not reason or retention < safety_horizon:
            raise ValueError("Archive requires audit fields and retention >= safety horizon")
        await self.client.request(
            self.workflow,
            self.workflow_id,
            "control",
            {
                "action": "archive",
                "run_id": self.run_id,
                "actor": actor,
                "reason": reason,
                "request_id": f"archive/{self.run_id or self.workflow_id}",
                "retention": retention,
                "safety_horizon": safety_horizon,
            },
        )
