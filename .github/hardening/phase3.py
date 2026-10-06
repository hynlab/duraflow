"""Phase 3: explicit settings, supervised drain, read-only probes and dead letters."""
from pathlib import Path
from helpers import add_method, function, replace, write

replace('src/duraflow/config.py', '            duration(getattr(self, key))', '            value = getattr(self, key)\n            if type(value) not in (int, float):\n                raise ValueError(f"Invalid {key}")\n            duration(value)')
replace('src/duraflow/observability.py', 'SAFE_FIELDS =', 'EVENTS = frozenset({"runtime_message", "engine_iteration_failed", "worker_iteration_failed", "task_claimed",\n    "task_observed", "workflow_state_saved", "shutdown_requested", "shutdown_incomplete", "runtime_started",\n    "runtime_stopped", "dependency_state", "message_quarantined", "dlq_requeued"})\nSAFE_FIELDS =')
replace('src/duraflow/observability.py', '        if not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", event):', '        if event not in EVENTS:')
replace('src/duraflow/observability.py', '    counts = health.stats.get("statuses", {})', '''    lines.append(f'duraflow_sampled_outbox_age_seconds{{role="{role}"}} {max(0, float(health.stats.get("outbox_age", 0))):.3f}')
    counts = health.stats.get("statuses", {})''')
replace('src/duraflow/supervision.py', '        done, _ = await asyncio.wait({running, requested}, return_when=asyncio.FIRST_COMPLETED)', '        done, _ = await asyncio.wait({running, requested, monitor_task}, return_when=asyncio.FIRST_COMPLETED)\n        if monitor_task in done and not stop.is_set():\n            await monitor_task\n            raise RuntimeError("Dependency monitor stopped unexpectedly")')
for filename, cls in (('coordinator.py', 'Engine'), ('runner.py', 'Worker')):
    path = 'src/duraflow/' + filename
    replace(path, '        self.metrics = {', '        self.accepting, self.draining = True, False\n        self.metrics = {')
replace('src/duraflow/coordinator.py', '            try:\n                await self.tick()', '            try:\n                if self.accepting and not self.draining:\n                    await self.tick()')
replace('src/duraflow/coordinator.py', 'log.error("Engine iteration failed"', 'log.error("engine_iteration_failed"')
replace('src/duraflow/runner.py', 'log.error("Worker iteration failed"', 'log.error("worker_iteration_failed"')
replace('src/duraflow/runner.py', 'from .transport import Delivery, Transport', 'from .transport import Delivery, Transport\nfrom .quarantine import quarantine')
replace('src/duraflow/runner.py', '        return TaskContext(self, run_id, node_id, number, epoch, task_id)', '        log.info("task_claimed", extra={"run_id": run_id, "node_id": node_id, "task_id": task_id, "attempt": number, "epoch": epoch})\n        return TaskContext(self, run_id, node_id, number, epoch, task_id)')
replace('src/duraflow/runner.py', '            await mutate(self.store, self.namespace, context.run_id, change)', '            await mutate(self.store, self.namespace, context.run_id, change)\n            log.info("task_observed", extra={"run_id": context.run_id, "node_id": context.node_id, "task_id": context.task_id, "attempt": context.attempt, "epoch": context.lease_epoch})')
function('src/duraflow/runner.py', 'Worker.process', '''
async def process(self, delivery: Delivery, ref: TaskRef[Any, Any]) -> None:
    async with self.semaphore:
        if self.draining or not self.accepting:
            await self.transport.nack(delivery)
            return
        try:
            context = await self._claim(delivery, ref)
            if context is not None:
                await self._execute(context, ref)
            await self.transport.ack(delivery)
        except (ProtocolError, NotFound) as exc:
            await self.transport.publish(delivery.topic + "-dlq", quarantine(delivery, type(exc).__name__),
                                         {"reason": type(exc).__name__, "source_subscription": delivery.subscription})
            await self.transport.ack(delivery)
            self.metrics["quarantined"] += 1
            log.warning("message_quarantined", extra={"error_type": type(exc).__name__})
        except BaseException:
            await self.transport.nack(delivery)
            raise
''')
replace('src/duraflow/runner.py', '    async def step(self) -> bool:\n        await self.prepare()', '    async def step(self) -> bool:\n        if self.draining or not self.accepting:\n            return False\n        await self.prepare()')
add_method('src/duraflow/transport.py', 'PulsarTransport', '''
async def ping(self) -> bool:
    topic = next(iter(self.provisioned))[0] if self.provisioned else "persistent://public/default/df-health"
    return bool(await self._native(self.client.get_topic_partitions, topic))
''')
add_method('src/duraflow/postgres.py', 'PostgresStore', '''
async def telemetry(self, namespace: str) -> dict[str, Any]:
    """Metadata counts plus a bounded 100-active-run outbox-age sample."""
    from sqlalchemy import func, select
    async with self._transaction() as conn:
        now = await self._now(conn)
        counts = (await conn.execute(select(self.runs.c.status, func.count()).where(
            self.runs.c.namespace == namespace).group_by(self.runs.c.status))).all()
        due = (await conn.execute(select(func.min(self.runs.c.next_due)).where(
            self.runs.c.namespace == namespace, self.runs.c.next_due > 0, self.runs.c.next_due <= now))).scalar()
        documents = (await conn.execute(select(self.runs.c.document).where(
            self.runs.c.namespace == namespace, self.runs.c.next_due.is_not(None))
            .order_by(self.runs.c.next_due).limit(100))).scalars().all()
        pending = [item["created_at"] for document in documents for item in document["outbox"].values()
                   if not item["delivered"]]
        return {"statuses": {row[0]: row[1] for row in counts},
                "due_lag": max(0, now - due) if due is not None else 0,
                "outbox_age": max(0, now - min(pending)) if pending else 0}
''')
replace('src/duraflow/cli.py', 'from .client import Client', 'from .client import Client\nfrom .config import RuntimeSettings\nfrom .observability import configure_logging\nfrom .quarantine import decode_quarantine, replay_quarantine, summary as quarantine_summary\nfrom .supervision import supervise')
replace('src/duraflow/cli.py', 'import sys', 'import sys\nfrom pathlib import Path')
replace('src/duraflow/cli.py', '    root.add_argument("--namespace", default="default")', '    root.add_argument("--namespace", default=os.environ.get("DURAFLOW_NAMESPACE", "default"))')
replace('src/duraflow/cli.py', '    commands = root.add_subparsers', '''    root.add_argument("--production", action="store_true", default=None)
    root.add_argument("--concurrency", type=int)
    root.add_argument("--lease-seconds", type=float)
    root.add_argument("--shutdown-timeout", type=float)
    root.add_argument("--probe-port", type=int)
    root.add_argument("--probe-host")
    commands = root.add_subparsers''')
replace('src/duraflow/cli.py', '    return root', '''    peek = commands.add_parser("dlq-peek")
    peek.add_argument("--topic", required=True)
    peek.add_argument("--limit", type=int, default=10)
    peek.add_argument("--include-payload", action="store_true")
    replay = commands.add_parser("dlq-replay")
    replay.add_argument("--file", required=True)
    replay.add_argument("--actor", required=True)
    replay.add_argument("--reason", required=True)
    replay.add_argument("--request-id", required=True)
    replay.add_argument("--yes", action="store_true", required=True)
    return root''')
with Path('src/duraflow/cli.py').open('a') as stream:
    # This helper is used during execute, which runs after module initialization
    # for installed CLI entrypoints; move the __main__ guard to the end below.
    stream.write('''\n\ndef runtime_settings(args: argparse.Namespace) -> RuntimeSettings:
    overrides = {"database_url": args.database, "broker_url": args.broker, "namespace": args.namespace}
    for field in ("production", "concurrency", "lease_seconds", "shutdown_timeout", "probe_port", "probe_host"):
        value = getattr(args, field, None)
        if value is not None:
            overrides[field] = value
    return RuntimeSettings.from_environment(os.environ, **overrides)
''')
function('src/duraflow/cli.py', 'open_store', '''
async def open_store(url: str, settings: RuntimeSettings | None = None) -> Store:
    settings = settings or RuntimeSettings(database_url=url)
    if url.startswith("sqlite:///"):
        return SQLiteStore(url.removeprefix("sqlite:///"))
    from .postgres import PostgresStore
    return PostgresStore(url, pool_size=settings.pool_size, operation_timeout=settings.operation_timeout,
                         lock_timeout=settings.lock_timeout, statement_timeout=settings.statement_timeout)
''')
function('src/duraflow/cli.py', 'service', '''
async def service(args: argparse.Namespace, store: Store, app: Any, registry: Registry) -> dict[str, bool]:
    if app is None:
        raise ValueError("--app is required for engine/worker")
    settings = runtime_settings(args)
    configure_logging()
    authentication = None
    token = os.environ.get("DURAFLOW_PULSAR_TOKEN")
    if token:
        import pulsar
        authentication = pulsar.AuthenticationToken(token)
    transport = PulsarTransport(settings.broker_url, authentication=authentication,
                                tls_trust_certs_file_path=os.environ.get("DURAFLOW_PULSAR_TLS_CA"),
                                receiver_queue_size=settings.receiver_queue_size, max_routes=settings.max_routes)
    if args.command == "engine":
        runtime: Any = Engine(store, transport, registry, namespace=settings.namespace,
                              batch_size=settings.batch_size, max_commands=settings.max_commands,
                              replay_executor=ProcessReplayExecutor(args.app, workers=settings.replay_workers,
                                                                     timeout=settings.replay_timeout))
    else:
        runtime = Worker(store, transport, registry, namespace=settings.namespace,
                         broadcasts=getattr(app, "broadcasts", ()), concurrency=settings.concurrency,
                         lease_seconds=settings.lease_seconds)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    signals = (signal.SIGINT, signal.SIGTERM)
    for sig in signals:
        loop.add_signal_handler(sig, stop.set)
    def hard_exit() -> None:
        # Only this dedicated CLI process may exit forcibly. Library APIs never do.
        # Lease fencing and business idempotency still govern recovery elsewhere.
        os._exit(75)
    try:
        await supervise(runtime, transport, store, registry, settings, stop, role=args.command,
                        on_hard_timeout=hard_exit)
    finally:
        for sig in signals:
            loop.remove_signal_handler(sig)
    return {"stopped": True}
''')
replace('src/duraflow/cli.py', '    store = await open_store(args.database)', '    settings = runtime_settings(args)\n    store = await open_store(settings.database_url, settings)')
replace('src/duraflow/cli.py', '        handle = client.get_handle(args.run_id)', '''        if args.command == "dlq-replay":
            return {"requeued": await replay_quarantine(client, Path(args.file).read_bytes(), actor=args.actor,
                       reason=args.reason, request_id=args.request_id)}
        if args.command == "dlq-peek":
            if not 1 <= args.limit <= 100:
                raise ValueError("DLQ inspection limit must be 1..100")
            authentication = None
            token = os.environ.get("DURAFLOW_PULSAR_TOKEN")
            if token:
                import pulsar
                authentication = pulsar.AuthenticationToken(token)
            broker = PulsarTransport(settings.broker_url, authentication=authentication,
                                     tls_trust_certs_file_path=os.environ.get("DURAFLOW_PULSAR_TLS_CA"))
            result, seen, receipts = [], set(), []
            try:
                for _ in range(args.limit):
                    delivery = await broker.receive(args.topic, "duraflow-operator-inspection")
                    if delivery is None:
                        break
                    receipts.append(delivery)
                    entry = decode_quarantine(delivery.data)
                    if entry["quarantine_id"] not in seen:
                        result.append(entry if args.include_payload else quarantine_summary(entry))
                        seen.add(entry["quarantine_id"])
                return result
            finally:
                for delivery in receipts:
                    await broker.nack(delivery)
                await broker.close()
        handle = client.get_handle(args.run_id)''')
p = Path('src/duraflow/cli.py')
text = p.read_text().replace('\nif __name__ == "__main__":\n    main()\n', '\n')
p.write_text(text + '\n\nif __name__ == "__main__":\n    main()\n')
write('docs/hardening/phase3.md', '''
# Phase 3 — operational tooling

C01: RuntimeSettings validates finite limits, timeout ordering, pool sizes and
lease margins. DURAFLOW_* environment variables and selected CLI flags configure
the runtime; --production rejects SQLite fallback. URL fields are hidden in repr.
Connection-security requirements are the next phase, not implied by this flag yet.

C02: service supervision stops admission before draining, keeps in-flight work
within the grace budget and bounds cleanup. The dedicated CLI process exits 75
when a task/native call cannot drain; library supervision raises ShutdownTimeout
and never kills its host process. Arbitrary Python threads cannot be killed safely.
A CPU-bound async task that blocks the entire event loop still requires the
external container/service supervisor's hard stop deadline. No rollback of an
external side effect is claimed. Task result-before-ACK ordering is unchanged.

C03: optional bounded read-only /live, /ready and /metrics probes. Readiness checks
schema, broker lookup and local registered implementations, expires when stale,
and becomes false before drain. Runtime admission pauses when dependencies fail.
Probe requests never consume business messages. Bind probes to a private network.

C04: explicit event names and allowlisted scalar correlation fields only. No
payloads, arbitrary exception strings, traceback locals, URLs or tokens are emitted
by the operational formatter. Applications remain responsible for their own logs.

C05: bounded role/status metric labels, runtime counters, due-deadline lag and a
clearly named 100-active-run sampled outbox-age gauge. This sample is not a global
proof that no older outbox exists; use per-run diagnostics when investigating.
Prometheus alert rules are provided for readiness, errors, blocked runs and delay.

C06: dead-letter envelopes retain bounded raw bytes, original metadata, source
route and checksums. Oversized payloads are hash-only/truncated and are NOT
replayable. dlq-peek uses a separate inspection subscription and does not ACK or
advance the operational queue. Payload display requires an explicit flag.
dlq-replay requires an explicit file, actor, reason, request-id and --yes; it
compares the original committed dispatch, validates the participant and requeues
only the pending handler on its direct route. Completed/superseded/cancelled work
is not reopened. Operator authorization is hardened separately in phase 4.
''')
write('docs/operations/alerts.yml', '''
groups:
  - name: duraflow
    rules:
      - alert: DuraflowNotReady
        expr: duraflow_ready == 0
        for: 2m
        labels: {severity: warning}
        annotations: {summary: "Duraflow is not accepting new work"}
      - alert: DuraflowTransportFailures
        expr: rate(duraflow_transport_errors_total[5m]) > 0.1
        for: 2m
        labels: {severity: warning}
        annotations: {summary: "Duraflow broker publication failures are recurring"}
      - alert: DuraflowBlockedRuns
        expr: duraflow_runs{status="BLOCKED"} > 0
        for: 5m
        labels: {severity: warning}
        annotations: {summary: "Duraflow has blocked runs requiring diagnosis"}
      - alert: DuraflowOutboxDelay
        expr: duraflow_sampled_outbox_age_seconds > 60
        for: 2m
        labels: {severity: warning}
        annotations: {summary: "Sampled Duraflow outbox entries are overdue"}
''')
