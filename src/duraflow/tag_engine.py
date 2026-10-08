"""Persistent paginated signal fan-out by tag; no workflow-state database access."""

from __future__ import annotations

from typing import Any

from .contracts import ProtocolError, fingerprint
from .message_runtime import Consumer, OutboxRelay
from .message_store import MessageStore
from .messaging import Message, Publication, Topics
from .state import identity
from .transport import Delivery, Transport


class TagEngine(Consumer):
    def __init__(
        self, store: MessageStore, transport: Transport, *, topics: Topics | None = None, page_size: int = 100
    ):
        super().__init__(transport, topics or Topics())
        if not 1 <= page_size <= 5000:
            raise ValueError("Invalid tag page size")
        self.store, self.page_size = store, page_size
        self.routes = [(self.topics.tags(), "tags", True)]
        self.relay = OutboxRelay(store, transport)

    async def handle(self, message: Message, delivery: Delivery) -> None:
        body = message.body
        if message.key != f"{self.topics.namespace}/{body['tag']}":
            raise ProtocolError("Wrong tag identity")

        def update(state: dict[str, Any], now: float) -> list[Publication]:
            members = state.setdefault("members", {})
            workflow = body["workflow"]
            ids = members.setdefault(workflow, [])
            if message.kind == "tag_add":
                if body["workflow_id"] not in ids:
                    ids.append(body["workflow_id"])
                    ids.sort()
                response = Message(
                    "tag_registered", body["workflow_key"], {"tag": body["tag"]}, id=identity(message.id, "registered")
                )
                return [Publication(body["reply_to"], response, subscription="state")]
            if message.kind == "tag_remove":
                if body["workflow_id"] in ids:
                    ids.remove(body["workflow_id"])
                return []
            if message.kind != "tag_signal":
                raise ProtocolError("Unexpected tag command")
            jobs = state.setdefault("jobs", {})
            job_id = identity(workflow, body["signal_id"])
            digest = fingerprint({key: value for key, value in body.items() if key != "offset"})
            if job_id not in jobs:
                if len(jobs) >= 10000:
                    raise ProtocolError("Tag fan-out quota exceeded")
                jobs[job_id] = {"digest": digest, "targets": list(ids), "offset": 0, "complete": False}
            job = jobs[job_id]
            if job["digest"] != digest:
                raise ProtocolError("Tag signal ID reused with different content")
            if job["complete"] or body.get("offset", 0) != job["offset"]:
                return []
            targets = job["targets"][job["offset"] : job["offset"] + self.page_size]
            result = []
            for workflow_id in targets:
                signal = Message(
                    "signal",
                    self.topics.instance(workflow, workflow_id),
                    {key: body[key] for key in ("channel", "schema", "payload_schema", "payload", "signal_id")},
                    id=identity(job_id, workflow_id),
                )
                result.append(Publication(self.topics.workflow(workflow), signal, subscription="state"))
            job["offset"] += len(targets)
            if job["offset"] < len(job["targets"]):
                continuation = Message(
                    "tag_signal",
                    message.key,
                    {**body, "offset": job["offset"]},
                    id=identity(job_id, "offset/" + str(job["offset"])),
                )
                result.append(Publication(self.topics.tags(), continuation, subscription="tags"))
            else:
                job["complete"], job["targets"] = True, []
            return result

        await self.store.apply("tag/" + message.key, message, update)

    async def step(self) -> bool:
        return bool(await self.relay.step() or await super().step())
