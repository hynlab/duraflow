"""Destructive tests are restricted to explicitly opted-in local Compose services."""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
from urllib.parse import urlsplit


def project_name() -> str:
    project = os.environ.get("COMPOSE_PROJECT_NAME", "")
    if os.environ.get("DURAFLOW_ALLOW_DESTRUCTIVE_TESTS") != "isolated-compose":
        raise RuntimeError("Destructive tests require an explicit isolated-compose opt-in")
    if not re.fullmatch(r"duraflow-qualification-[a-z0-9-]{1,50}", project):
        raise RuntimeError("Refusing to target a non-qualification Compose project")
    for key, scheme in (("DURAFLOW_TEST_POSTGRES", "postgresql+psycopg"), ("DURAFLOW_TEST_PULSAR", "pulsar")):
        parsed = urlsplit(os.environ.get(key, ""))
        if parsed.scheme != scheme or parsed.hostname not in {"localhost", "127.0.0.1"} or not parsed.port:
            raise RuntimeError("Fault tests require the isolated local test service ports")
    return project


def compose(*args: str, timeout: float = 120, input_data: bytes | None = None) -> bytes:
    project = project_name()
    return subprocess.run(
        ["docker", "compose", "-p", project, *args], input=input_data, capture_output=True, check=True, timeout=timeout
    ).stdout


def owned_service(service: str, *, require_running: bool = True) -> str:
    if service not in {"postgres", "pulsar"}:
        raise RuntimeError("Unknown disposable service")
    ids = compose("ps", "--all", "-q", service).decode().split()
    if len(ids) != 1:
        raise RuntimeError("Expected exactly one disposable service container")
    info = json.loads(
        subprocess.run(["docker", "inspect", ids[0]], capture_output=True, check=True, timeout=10).stdout
    )[0]
    labels = info["Config"].get("Labels", {})
    if (
        labels.get("com.docker.compose.project") != project_name()
        or labels.get("com.docker.compose.service") != service
    ):
        raise RuntimeError("Container is not owned by the isolated qualification project")
    key, container_port = {
        "postgres": ("DURAFLOW_TEST_POSTGRES", "5432/tcp"),
        "pulsar": ("DURAFLOW_TEST_PULSAR", str(urlsplit(os.environ["DURAFLOW_TEST_PULSAR"]).port) + "/tcp"),
    }[service]
    expected_port = str(urlsplit(os.environ[key]).port)
    bindings = info.get("HostConfig", {}).get("PortBindings", {}).get(container_port, []) or []
    if not any(
        binding.get("HostIp") == "127.0.0.1" and binding.get("HostPort") == expected_port for binding in bindings
    ):
        raise RuntimeError("Configured endpoint is not the disposable container's loopback port")
    if require_running and not info.get("State", {}).get("Running"):
        raise RuntimeError("Disposable service is not running")
    return ids[0]


def interrupt(service: str) -> None:
    from scripts.pytest_guard import record_resource

    owned_service(service)
    record_resource("service", service=service)
    compose("kill", "-s", "SIGKILL", service)


def restart(service: str) -> None:
    owned_service(service, require_running=False)
    compose("up", "-d", "--wait", "--wait-timeout", "180", service, timeout=210)


def wait_healthy(service: str, timeout: float = 120) -> None:
    """Wait after unpausing without racing a second Compose convergence operation."""
    container = owned_service(service, require_running=False)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        state = json.loads(
            subprocess.run(
                ["docker", "inspect", container],
                check=True,
                capture_output=True,
                timeout=10,
            ).stdout
        )[0]["State"]
        if state.get("Running") and not state.get("Paused") and state.get("Health", {}).get("Status") == "healthy":
            return
        time.sleep(0.2)
    raise TimeoutError(f"Disposable {service} did not become healthy")


if __name__ == "__main__":
    for service in ("postgres", "pulsar"):
        owned_service(service)
    print("Isolated local fault-test targets verified")
