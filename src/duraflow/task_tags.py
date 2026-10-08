"""Service-tag engine: a separate keyed index and resumable cancellation fan-out."""

from __future__ import annotations

from typing import Any

from .contracts import ProtocolError, fingerprint
from .message_runtime import Consumer, OutboxRelay
from .message_store import MessageStore
from .messaging import Message, Publication, Topics
from .state import identity
from .transport import Delivery, Transport


class TaskTagEngine(Consumer):
    def __init__(
        self, store: MessageStore, transport: Transport, *, topics: Topics | None = None, page_size: int = 100
    ):
        super().__init__(transport, topics or Topics())
        if not 1 <= page_size <= 5000:
            raise ValueError("Invalid fan-out page size")
        self.store, self.page_size = store, page_size
        self.routes = [(self.topics.task_tags(), "tags", True), (self.topics.task_tags(control=True), "tags", True)]
        self.relay = OutboxRelay(store, transport)

    async def handle(self, message: Message, delivery: Delivery) -> None:
        body = message.body
        if message.kind == "task_tag_cancel" and delivery.topic != self.topics.task_tags(control=True):
            raise ProtocolError("Task tag controls require the operator topic")

        def update(state: dict[str, Any], now: float) -> list[Publication]:
            members, closed = state.setdefault("members", {}), state.setdefault("closed", {})
            if message.kind == "task_tag_add":
                if body["dispatch_id"] not in closed:
                    members[body["dispatch_id"]] = body["topic"]
                return []
            if message.kind == "task_tag_remove":
                members.pop(body["dispatch_id"], None)
                closed[body["dispatch_id"]] = True
                return []
            if message.kind != "task_tag_cancel":
                raise ProtocolError("Unexpected task tag event")
            jobs = state.setdefault("jobs", {})
            job_id = body["request_id"]
            digest = fingerprint({key: value for key, value in body.items() if key != "offset"})
            if job_id not in jobs:
                if len(jobs) >= 10000:
                    raise ProtocolError("Task-tag request quota exceeded")
                jobs[job_id] = {"digest": digest, "targets": sorted(members.items()), "offset": 0}
            job = jobs[job_id]
            if job["digest"] != digest:
                raise ProtocolError("Task tag request identity conflict")
            if body.get("offset", 0) != job["offset"]:
                return []
            targets = job["targets"][job["offset"] : job["offset"] + self.page_size]
            publications = [
                Publication(
                    topic,
                    Message(
                        "cancel_task", dispatch_id, {"dispatch_id": dispatch_id}, id=identity(message.id, dispatch_id)
                    ),
                    subscription="tasks",
                )
                for dispatch_id, topic in targets
            ]
            job["offset"] += len(targets)
            if job["offset"] < len(job["targets"]):
                continuation = Message(
                    "task_tag_cancel", message.key, {**body, "offset": job["offset"]}, id=identity(message.id, "next")
                )
                publications.append(Publication(self.topics.task_tags(control=True), continuation, subscription="tags"))
            return publications

        await self.store.apply("task-tag/" + message.key, message, update)

    async def step(self) -> bool:
        sent = await self.relay.step()
        return bool(await super().step() or sent)
