"""The outer watchdog must clean independent process groups after pytest dies."""

import os
import json
import subprocess
import sys
import time

import pytest

from scripts.pytest_guard import cleanup, record_resource, terminate_group


def test_guard_reaps_registered_replay_process_after_hard_exit(tmp_path):
    test = tmp_path / "test_abrupt.py"
    pid_file = tmp_path / "child.pid"
    test.write_text(
        "import os, signal, subprocess, sys\n"
        "from pathlib import Path\n"
        "from scripts.pytest_guard import record_resource\n"
        "def test_exit():\n"
        "    child = subprocess.Popen([sys.executable, '-m', 'duraflow.replay_child', '--app', 'tests.message_app', '--max-bytes', '16777216'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, start_new_session=True)\n"
        "    assert child.stdout.readline()\n"
        "    os.killpg(child.pid, signal.SIGSTOP)\n"
        "    record_resource('process', pid=child.pid)\n"
        f"    Path({str(pid_file)!r}).write_text(str(child.pid))\n"
        "    os._exit(3)\n"
    )
    env = {**os.environ, "PYTHONPATH": os.pathsep.join((os.path.abspath("src"), os.getcwd()))}
    env.pop("DURAFLOW_REQUIRE_NATIVE", None)
    result = subprocess.run(
        [sys.executable, "-m", "scripts.pytest_guard", str(test), "-q"],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 3, result.stdout + result.stderr
    pid = int(pid_file.read_text())
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        inspected = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True)
        if inspected.returncode or inspected.stdout.strip().startswith("Z"):
            break
        time.sleep(0.05)
    else:
        raise AssertionError("Owned replay process survived pytest's hard exit")


def test_released_resource_is_not_touched(tmp_path, monkeypatch):
    log = tmp_path / "resources.jsonl"
    monkeypatch.setenv("DURAFLOW_TEST_RESOURCE_LOG", str(log))
    record_resource("pitr", id="not-a-valid-path", name="unowned")
    record_resource("pitr", id="not-a-valid-path", name="unowned", released=True)
    assert cleanup(log) == []


def test_reused_pid_from_another_invocation_is_not_killed(tmp_path, monkeypatch):
    log = tmp_path / "resources.jsonl"
    monkeypatch.setenv("DURAFLOW_TEST_RESOURCE_LOG", str(log))
    monkeypatch.setenv("DURAFLOW_TEST_RUN_ID", "1" * 32)
    record_resource("process", pid=123456)

    def run(args, **kwargs):
        if args[1] == "-axo":
            output = "123456 123456 S python -m tests.message_process engine 0\n"
        else:
            output = "python -m tests.message_process engine 0 DURAFLOW_TEST_RUN_ID=" + "2" * 32
        return subprocess.CompletedProcess(args, 0, stdout=output)

    monkeypatch.setattr("scripts.pytest_guard.subprocess.run", run)
    monkeypatch.setattr("scripts.pytest_guard.os.killpg", lambda *args: pytest.fail("Killed another invocation"))
    assert cleanup(log) == ["process: RuntimeError"]


def test_failed_environment_probe_does_not_authorize_killing_a_live_group(monkeypatch):
    def run(args, **kwargs):
        if args[1] == "-axo":
            return subprocess.CompletedProcess(
                args, 0, stdout="123456 123456 S python -m tests.message_process engine 0\n"
            )
        if args[1] == "eww":
            return subprocess.CompletedProcess(args, 2, stdout="")
        return subprocess.CompletedProcess(args, 0, stdout="S\n")

    monkeypatch.setattr("scripts.pytest_guard.subprocess.run", run)
    monkeypatch.setattr("scripts.pytest_guard.os.killpg", lambda *args: pytest.fail("Killed without ownership proof"))
    with pytest.raises(RuntimeError, match="verify surviving process"):
        terminate_group(123456, "1" * 32)


@pytest.mark.parametrize("legacy", [False, True])
async def test_repeated_cluster_cleanup_does_not_reuse_retired_group_ids(monkeypatch, legacy):
    from types import SimpleNamespace
    from tests.message_cluster import Processes
    from tests.native_cluster import NativeCluster

    cls = NativeCluster if legacy else Processes
    cluster = cls.__new__(cls)
    cluster.run_id, cluster.terminated = "1" * 32, set()
    key = "engine-0" if legacy else ("engine", 0)
    cluster.processes = {key: SimpleNamespace(pid=123456, wait=lambda timeout: 0, returncode=0)}
    calls = []
    module = "tests.native_cluster" if legacy else "tests.message_cluster"
    monkeypatch.setattr(module + ".terminate_group", lambda *args: calls.append(args))
    monkeypatch.setattr(module + ".record_resource", lambda *args, **kwargs: None)
    for _ in range(2):
        if legacy:
            await cluster.kill(key)
        else:
            await cluster.kill(*key)
    assert len(calls) == 1


async def test_failed_restore_removal_remains_registered(tmp_path, monkeypatch):
    from tests.physical_restore import PhysicalRestore

    recovery = PhysicalRestore(tmp_path)
    released = []
    monkeypatch.setattr("tests.physical_restore.record_resource", lambda *args, **kwargs: released.append(kwargs))
    monkeypatch.setattr("tests.physical_restore.project_name", lambda: "owned")
    monkeypatch.setattr("tests.physical_restore.compose", lambda *args, **kwargs: b"")

    def docker(*args):
        if args[0] == "ps":
            return b"restore-id"
        if args[0] == "inspect":
            return json.dumps([{"Config": {"Labels": {"io.duraflow.qualification": "owned"}}}]).encode()
        raise RuntimeError("removal interrupted")

    monkeypatch.setattr("tests.physical_restore.docker", docker)
    with pytest.raises(RuntimeError, match="removal interrupted"):
        await recovery.__aexit__()
    assert released == []


def test_pitr_supervisor_recovers_stopped_primary(tmp_path, monkeypatch):
    log = tmp_path / "resources.jsonl"
    monkeypatch.setenv("DURAFLOW_TEST_RESOURCE_LOG", str(log))
    record_resource("pitr", id="/tmp/df-pitr-0123456789ab", name="duraflow-pitr-0123456789ab")
    operations = []

    def owned(service, *, require_running=True):
        assert service == "postgres" and not require_running
        return "primary"

    def run(args, **kwargs):
        output = json.dumps([{"State": {"Running": False}}]).encode() if args[:2] == ["docker", "inspect"] else ""
        return subprocess.CompletedProcess(args, 0, stdout=output)

    monkeypatch.setattr("scripts.fault_guard.owned_service", owned)
    monkeypatch.setattr("scripts.fault_guard.restart", lambda service: operations.append("restart"))
    monkeypatch.setattr("scripts.fault_guard.compose", lambda *args, **kwargs: operations.append(args))
    monkeypatch.setattr("scripts.pytest_guard.subprocess.run", run)
    assert cleanup(log) == []
    assert operations[0] == "restart"
    assert operations[-1][-4:] == ("rm", "-rf", "--", "/tmp/df-pitr-0123456789ab")
