"""Fail-closed coverage gate, separate from intermediate development checks."""

import json
import sys
from pathlib import Path

CRITICAL = ("coordinator.py", "state.py", "replay.py", "runner.py")
MESSAGE_CRITICAL = (
    "message_store.py",
    "message_postgres.py",
    "message_runtime.py",
    "workflow_engine.py",
    "workflow_worker.py",
    "workflow_replay.py",
    "task_worker.py",
    "tag_engine.py",
    "task_tags.py",
    "client.py",
)


def check(data):
    failures = []
    totals = data["totals"]
    if totals["percent_covered"] < 80:
        failures.append("Overall branch-aware coverage must be at least 80%")
    for filename in CRITICAL:
        matches = [v["summary"] for k, v in data["files"].items() if k.endswith("/" + filename)]
        if len(matches) != 1 or not matches[0].get("num_branches"):
            failures.append(f"Missing critical branch evidence: {filename}")
        elif 100 * matches[0]["covered_branches"] / matches[0]["num_branches"] < 95:
            failures.append(f"Critical branch coverage must be at least 95%: {filename}")
    for filename in MESSAGE_CRITICAL:
        matches = [v["summary"] for k, v in data["files"].items() if k.endswith("/" + filename)]
        if len(matches) != 1 or not matches[0].get("num_branches"):
            failures.append(f"Missing protocol-2 branch evidence: {filename}")
        elif matches[0]["percent_covered"] < 80:
            failures.append(f"Protocol-2 branch-aware coverage must be at least 80%: {filename}")
    return failures


if __name__ == "__main__":
    failures = check(json.loads(Path(sys.argv[1]).read_text()))
    if failures:
        raise SystemExit("\n".join(failures))
    print("Coverage gate passed; native failure, soak and pilot evidence are separate gates")
