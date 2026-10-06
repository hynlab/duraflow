"""Bounded dead-letter envelopes and audited replay of committed pending work."""

from __future__ import annotations

import base64
import hashlib
from typing import Any

from .contracts import Conflict, MAX_PAYLOAD_BYTES, ProtocolError, canonical, clock_now, fingerprint, parse_json
from .state import event, mutate, outbox, route, subscription
from .storage import State
from .transport import Delivery

MAX_ENVELOPE_BYTES = 2 * 1024 * 1024


def quarantine(delivery: Delivery, reason: str) -> bytes:
    digest = hashlib.sha256(delivery.data).hexdigest()
    bounded = (
        len(delivery.data) <= MAX_PAYLOAD_BYTES and len(canonical(delivery.properties).encode()) <= MAX_PAYLOAD_BYTES
    )
    envelope = {
        "version": 1,
        "reason": reason[:64],
        "source_topic": delivery.topic,
        "source_subscription": delivery.subscription,
        "sha256": digest,
        "properties": dict(delivery.properties) if bounded else {},
        "data_base64": base64.b64encode(delivery.data).decode() if bounded else None,
        "truncated": not bounded,
    }
    envelope["quarantine_id"] = fingerprint(envelope)
    encoded = canonical(envelope).encode()
    if len(encoded) > MAX_ENVELOPE_BYTES:
        raise ProtocolError("Quarantine envelope exceeds storage bound")
    return encoded


def decode_quarantine(raw: bytes | str) -> dict[str, Any]:
    if len(raw) > MAX_ENVELOPE_BYTES:
        raise ProtocolError("Oversized quarantine envelope")
    envelope = parse_json(raw)
    if not isinstance(envelope, dict) or type(envelope.get("version")) is not int or envelope.get("version") != 1:
        raise ProtocolError("Unsupported quarantine envelope")
    expected = fingerprint({key: value for key, value in envelope.items() if key != "quarantine_id"})
    if envelope.get("quarantine_id") != expected:
        raise ProtocolError("Quarantine envelope checksum mismatch")
    if not isinstance(envelope.get("source_topic"), str) or not isinstance(envelope.get("source_subscription"), str):
        raise ProtocolError("Missing quarantine route")
    return envelope


def summary(envelope: dict[str, Any]) -> dict[str, Any]:
    return {
        key: envelope[key]
        for key in ("quarantine_id", "reason", "source_topic", "source_subscription", "sha256", "truncated")
    }


async def replay_quarantine(client: Any, raw: bytes | str, *, actor: str, reason: str, request_id: str) -> bool:
    envelope = decode_quarantine(raw)
    if not actor or not reason or not request_id or len(request_id) > 128 or len(actor) > 128 or len(reason) > 1000:
        raise ValueError("Explicit bounded actor, reason and request_id are required")
    if envelope["truncated"]:
        raise ProtocolError("Truncated quarantined payloads require manual source reconciliation")
    try:
        data = base64.b64decode(envelope["data_base64"], validate=True)
        if hashlib.sha256(data).hexdigest() != envelope["sha256"]:
            raise ProtocolError("Quarantine payload checksum mismatch")
        payload = parse_json(data)
        meta = parse_json(envelope["properties"]["duraflow"])
        run_id = meta["run_id"]
        if meta["namespace"] != client.namespace or not isinstance(run_id, str):
            raise ProtocolError("Quarantine namespace mismatch")
    except (KeyError, TypeError, ValueError) as exc:
        raise ProtocolError("Malformed quarantined task") from exc
    key = "dlq/" + request_id
    digest = fingerprint([envelope["quarantine_id"], actor, reason])
    authorizer = getattr(client.store, "authorize_control", None)
    principal = await authorizer("dlq-replay") if authorizer is not None else "trusted-local"

    def change(state: State) -> bool:
        previous = state["actions"].get(key)
        if previous is not None:
            if previous["digest"] != digest:
                raise Conflict("DLQ replay request_id conflict")
            return False
        if len(state["actions"]) >= 5000:
            raise Conflict("Operator history limit reached")
        if state["archived"] or state["status"] in {"CANCELLING", "CANCELLED", "TERMINATED", "CONTINUED", "BLOCKED"}:
            raise Conflict("Run does not accept DLQ replay")
        original = state["outbox"].get(meta["event_id"])
        if (
            original is None
            or original["topic"] != envelope["source_topic"]
            or fingerprint([original["payload"], original["metadata"]]) != fingerprint([payload, meta])
        ):
            raise ProtocolError("Quarantine does not match an original committed dispatch")
        if meta["kind"] == "broadcast":
            node_id = meta["handlers"].get(envelope["source_subscription"])
        elif meta["kind"] == "task":
            node_id = meta["node_id"]
        else:
            raise ProtocolError("Only task/broadcast dead letters are replayable")
        node = state["nodes"].get(node_id)
        if node is None or node["spec"]["kind"] != "call":
            raise ProtocolError("Quarantine handler is not a committed task")
        expected_sub = (
            subscription(client.namespace, node["spec"]["handler"]) if meta["kind"] == "broadcast" else "workers"
        )
        if envelope["source_subscription"] != expected_sub:
            raise ProtocolError("Quarantine subscription mismatch")
        current = node["attempts"][-1]
        if node["state"] in {"done", "error"} or current["observation"] is not None:
            return False
        if current["number"] != meta["attempt"] or current["deferred"] is not None or node.get("cancel_requested"):
            raise Conflict("Attempt is superseded or not eligible for replay")
        now = clock_now(client.clock)
        if current["not_before"] > now:
            raise Conflict("The current retry is not due")
        if current["lease_until"] > now:
            raise Conflict("An execution owner still holds the lease")
        topic = route(client.namespace, node["spec"]["ref"])
        event_id = outbox(
            state,
            f"task/{node_id}/{current['number']}",
            "task",
            topic,
            node["spec"]["input"],
            now,
            node_id=node_id,
            attempt=current["number"],
        )
        pending = state["outbox"][event_id]
        if pending["lease_until"] > now:
            raise Conflict("A dispatcher still owns this message")
        pending.update(delivered=False, next_attempt_at=0)
        current["dispatched"], current["last_dispatch"] = True, now
        state["actions"][key] = {
            "digest": digest,
            "action": "dlq-replay",
            "actor": actor,
            "principal": principal,
            "reason": reason,
            "time": now,
        }
        event(state, "dlq_replayed", now, node_id=node_id, event_id=event_id)
        return True

    return bool(await mutate(client.store, client.namespace, run_id, change))
