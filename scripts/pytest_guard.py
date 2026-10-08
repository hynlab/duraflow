"""Supervise pytest and clean registered test-owned resources after a hard exit."""

import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
from uuid import uuid4


def record_resource(kind, *, released=False, **fields):
    path = os.getenv("DURAFLOW_TEST_RESOURCE_LOG")
    if path:
        if kind == "process":
            fields["run_id"] = os.environ["DURAFLOW_TEST_RUN_ID"]
        data = json.dumps({"kind": kind, "released": released, **fields}).encode() + b"\n"
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            os.write(fd, data)
        finally:
            os.close(fd)


def terminate_group(group, run_id, sig=signal.SIGKILL):
    """Signal only members carrying this invocation's identity; fail closed."""
    listing = subprocess.run(
        ["ps", "-axo", "pid=,pgid=,stat=,command=", "-ww"], check=True, capture_output=True, text=True, timeout=10
    ).stdout
    members = []
    for line in listing.splitlines():
        fields = line.strip().split(None, 3)
        if len(fields) != 4 or not fields[0].isdigit() or not fields[1].isdigit():
            continue
        pid, pgid, status, command = fields
        if int(pgid) == group and not status.startswith("Z"):
            members.append((pid, command))
    allowed = (" -m tests.message_process ", " -m tests.native_fault_process ", " -m duraflow.replay_child ")
    verified = False
    for pid, command in members:
        if not any(module in command for module in allowed) or not re.fullmatch(r"[0-9a-f]{32}", run_id):
            raise RuntimeError("Unrecognized process-group owner")
        environment = subprocess.run(
            ["ps", "eww", "-p", pid, "-o", "command="], capture_output=True, text=True, timeout=10
        )
        if environment.returncode:
            exists = subprocess.run(["ps", "-p", pid, "-o", "stat="], capture_output=True, text=True, timeout=10)
            if exists.returncode == 1 or (exists.returncode == 0 and exists.stdout.strip().startswith("Z")):
                continue
            raise RuntimeError("Could not verify surviving process ownership")
        if not re.search(r"(?:^|\s)DURAFLOW_TEST_RUN_ID=" + run_id + r"(?:\s|$)", environment.stdout):
            raise RuntimeError("Registered PID now belongs to a different test invocation")
        verified = True
    if verified:
        try:
            os.killpg(group, sig)
        except ProcessLookupError:
            pass


def inspect_container(container):
    return json.loads(
        subprocess.run(["docker", "inspect", container], check=True, capture_output=True, timeout=10).stdout
    )[0]


def recover_service(service):
    from scripts.fault_guard import compose, owned_service, restart, wait_healthy

    container = owned_service(service, require_running=False)
    state = inspect_container(container)["State"]
    if state.get("Paused"):
        compose("unpause", service)
    if not state.get("Running"):
        restart(service)
    else:
        wait_healthy(service)


def recover_pitr(resource):
    from scripts.fault_guard import compose, project_name, restart

    root, name = resource["id"], resource["name"]
    if not re.fullmatch(r"/tmp/df-pitr-[0-9a-f]{12}", root) or not re.fullmatch(r"duraflow-pitr-[0-9a-f]{12}", name):
        raise RuntimeError("Unrecognized restore resource identity")
    containers = subprocess.run(
        ["docker", "ps", "-aq", "--filter", "name=^" + name + "$"],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    ).stdout.split()
    try:
        for container in containers:
            if inspect_container(container)["Config"]["Labels"].get("io.duraflow.qualification") != project_name():
                raise RuntimeError("Refusing cleanup of unowned restore container")
            subprocess.run(["docker", "rm", "-f", container], check=True, capture_output=True, timeout=30)
    finally:
        recover_service("postgres")
        for setting in ("archive_command", "archive_mode"):
            compose(
                "exec",
                "-T",
                "--user",
                "postgres",
                "postgres",
                "psql",
                "-U",
                "duraflow",
                "-d",
                "duraflow",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                f"ALTER SYSTEM RESET {setting}",
            )
        compose("restart", "postgres")
        restart("postgres")
        compose("exec", "-T", "--user", "postgres", "postgres", "rm", "-rf", "--", root)


def cleanup(path):
    resources = {}
    if path.exists():
        for line in path.read_text().splitlines():
            resource = json.loads(line)
            key = resource["kind"], resource.get("id", resource.get("pid", resource.get("service")))
            if resource["released"]:
                resources.pop(key, None)
            else:
                resources[key] = resource
    errors = []
    priority = {"process": 0, "service": 1, "pitr": 2}
    for resource in sorted(resources.values(), key=lambda item: priority[item["kind"]]):
        try:
            if resource["kind"] == "process":
                terminate_group(resource["pid"], resource.get("run_id", ""))
            elif resource["kind"] == "service":
                recover_service(resource["service"])
            else:
                recover_pitr(resource)
        except Exception as exc:
            errors.append(f"{resource['kind']}: {type(exc).__name__}")
    return errors


def main():
    root = Path(__file__).resolve().parents[1]
    with tempfile.TemporaryDirectory(prefix="duraflow-pytest-guard-") as directory:
        resources = Path(directory) / "resources.jsonl"
        env = {**os.environ, "DURAFLOW_TEST_RESOURCE_LOG": str(resources), "DURAFLOW_TEST_RUN_ID": uuid4().hex}
        env["PYTHONPATH"] = os.pathsep.join((str(root / "src"), str(root), env.get("PYTHONPATH", "")))
        process = subprocess.Popen([sys.executable, "-m", "pytest", *sys.argv[1:]], env=env, start_new_session=True)
        status = 1
        try:
            status = process.wait(timeout=1800)
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
            print("pytest supervisor interrupted; cleaning owned resources", file=sys.stderr)
        finally:
            # Non-detached descendants share the new group owned by this Popen.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            errors = cleanup(resources)
            if errors:
                print("Resource cleanup failed: " + "; ".join(errors), file=sys.stderr)
                status = 1
        return status


if __name__ == "__main__":
    raise SystemExit(main())
