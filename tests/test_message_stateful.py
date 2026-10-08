"""Model-based journal histories: duplicates, conflicts, rollbacks and lease owners."""

import asyncio
import tempfile
from pathlib import Path

from hypothesis import settings, strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, rule

from duraflow import Conflict, ManualClock, SQLiteMessageStore
from duraflow.message_store import MemoryMessageStore
from duraflow.messaging import Message, Publication


class JournalModel(RuleBasedStateMachine):
    backend = "memory"

    def __init__(self):
        super().__init__()
        self.directory = tempfile.TemporaryDirectory(prefix="duraflow-model-")
        self.runner = asyncio.Runner()
        self.clock = ManualClock()
        self.store = (
            MemoryMessageStore(clock=self.clock)
            if self.backend == "memory"
            else SQLiteMessageStore(Path(self.directory.name) / "journal.db", clock=self.clock)
        )
        self.accepted = {}
        self.pending = {}
        self.total = 0

    @rule(slot=st.integers(0, 8), delta=st.integers(-10, 10), rollback=st.booleans())
    def deliver(self, slot, delta, rollback):
        message_id = f"input-{slot}"
        message = Message("add", "account", {"delta": delta}, id=message_id)

        def update(state, now):
            state["total"] = state.get("total", 0) + delta
            if rollback:
                raise RuntimeError("transaction interrupted")
            return [Publication("results", Message("receipt", "account", {"delta": delta}, id=f"output-{slot}"))]

        try:
            applied = self.runner.run(self.store.apply("account", message, update))
        except Conflict:
            assert slot in self.accepted and self.accepted[slot] != delta
        except RuntimeError:
            assert rollback and slot not in self.accepted
        else:
            assert applied == (slot not in self.accepted)
            if applied:
                assert not rollback
                self.accepted[slot] = delta
                self.total += delta
                self.pending[f"output-{slot}"] = (None, 0)

    @rule(owner=st.sampled_from(["one", "two"]), limit=st.integers(1, 3))
    def claim(self, owner, limit):
        eligible = {key for key, (_, until) in self.pending.items() if until <= self.clock.now()}
        items = self.runner.run(self.store.claim(owner, limit))
        identities = {item.message.id for item in items}
        assert len(items) == len(identities) == min(limit, len(eligible))
        assert identities <= eligible
        for identity in identities:
            self.pending[identity] = (owner, self.clock.now() + 30)

    @rule(slot=st.integers(0, 8), owner=st.sampled_from(["one", "two", "stale"]), delivered=st.booleans())
    def finish(self, slot, owner, delivered):
        identity = f"output-{slot}"
        operation = self.store.delivered if delivered else self.store.release
        self.runner.run(operation(identity, owner))
        if identity in self.pending and self.pending[identity][0] == owner:
            if delivered:
                del self.pending[identity]
            else:
                self.pending[identity] = (None, self.clock.now() + 1)

    @rule(seconds=st.sampled_from([0, 0.5, 1, 29, 30, 31, 100]))
    def advance(self, seconds):
        self.clock.advance(seconds)

    @invariant()
    def durable_total_matches_only_unique_committed_inputs(self):
        state = self.runner.run(self.store.read("account"))
        assert state == ({"total": self.total} if self.accepted else {})

    def teardown(self):
        self.runner.run(self.store.close())
        self.runner.close()
        self.directory.cleanup()


class SQLiteJournalModel(JournalModel):
    backend = "sqlite"


TestMemoryJournalHistories = JournalModel.TestCase
TestSQLiteJournalHistories = SQLiteJournalModel.TestCase
for case in (TestMemoryJournalHistories, TestSQLiteJournalHistories):
    case.settings = settings(max_examples=40, stateful_step_count=60, deadline=None, print_blob=True)
del case  # unittest also collects aliases that do not start with "Test".
