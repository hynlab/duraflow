"""Destructive tests are restricted to explicitly opted-in local Compose services."""
from __future__ import annotations

import json
import os
import re
import subprocess
from urllib.parse import urlsplit


def project_name() -> str:
    project = os.environ.get("COMPOSE_PROJECT_NAME", "")
    if os.environ.get("DURAFLOW_ALLOW_DESTRUCTIVE_TESTS") != "isolated-compose":
        raise RuntimeError("Destructive tests require an explicit isolated-compose opt-in")
    if not re.fullmatch(r"duraflow-qualification-[a-z0-9-]{1,50}", project):
        raise RuntimeError("Refusing to target a non-qualification Compose project")
    for key, port in (("DURAFLOW_TEST_POSTGRES", 5432), ("DURAFLOW_TEST_PULSAR", 6650)):
        parsed = urlsplit(os.environ.get(key, ""))
        if parsed.hostname not in {"localhost", "127.0.0.1"} or parsed.port != port:
            raise RuntimeError("Fault tests require the isolated local test service ports")
    return project


def compose(*args: str, timeout: float = 120, input_data: bytes | None = None) -> bytes:
    project = project_name()
    return subprocess.run(["docker", "compose", "-p", project, *args], input=input_data,
                          capture_output=True, check=True, timeout=timeout).stdout


def owned_service(service: str) -> str:
    if service not in {"postgres", "pulsar"}:
        raise RuntimeError("Unknown disposable service")
    ids = compose("ps", "--all", "-q", service).decode().split()
    if len(ids) != 1:
        raise RuntimeError("Expected exactly one disposable service container")
    info = json.loads(subprocess.run(["docker", "inspect", ids[0]], capture_output=True,
                                    check=True, timeout=10).stdout)[0]
    labels = info["Config"].get("Labels", {})
    if (labels.get("com.docker.compose.project") != project_name()
            or labels.get("com.docker.compose.service") != service):
        raise RuntimeError("Container is not owned by the isolated qualification project")
    return ids[0]


def interrupt(service: str) -> None:
    owned_service(service)
    compose("kill", "-s", "SIGKILL", service)


def restart(service: str) -> None:
    owned_service(service)
    compose("up", "-d", "--wait", "--wait-timeout", "180", service, timeout=210)


if __name__ == "__main__":
    for service in ("postgres", "pulsar"):
        owned_service(service)
    print("Isolated local fault-test targets verified")
