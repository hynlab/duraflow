import pytest
import json
from types import SimpleNamespace
from scripts import fault_guard
from scripts.fault_guard import project_name


def test_destructive_faults_require_explicit_local_project_opt_in(monkeypatch):
    monkeypatch.delenv("DURAFLOW_ALLOW_DESTRUCTIVE_TESTS", raising=False)
    with pytest.raises(RuntimeError):
        project_name()
    monkeypatch.setenv("DURAFLOW_ALLOW_DESTRUCTIVE_TESTS", "isolated-compose")
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", "production")
    with pytest.raises(RuntimeError):
        project_name()
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", "duraflow-qualification-test")
    monkeypatch.setenv("DURAFLOW_TEST_POSTGRES", "postgresql+psycopg://u:p@database.example/production")
    monkeypatch.setenv("DURAFLOW_TEST_PULSAR", "pulsar://localhost:6650")
    with pytest.raises(RuntimeError):
        project_name()
    monkeypatch.setenv("DURAFLOW_TEST_POSTGRES", "postgresql+psycopg://u:p@localhost:5432/test")
    assert project_name() == "duraflow-qualification-test"


@pytest.mark.parametrize("issue", ["project", "service", "port", "host", "stopped", "valid"])
def test_fault_guard_requires_the_owned_running_endpoint(monkeypatch, issue):
    monkeypatch.setenv("DURAFLOW_ALLOW_DESTRUCTIVE_TESTS", "isolated-compose")
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", "duraflow-qualification-test")
    monkeypatch.setenv("DURAFLOW_TEST_POSTGRES", "postgresql+psycopg://u:p@localhost:54332/test")
    monkeypatch.setenv("DURAFLOW_TEST_PULSAR", "pulsar://localhost:56650")
    info = {
        "Config": {
            "Labels": {
                "com.docker.compose.project": "duraflow-qualification-test",
                "com.docker.compose.service": "postgres",
            }
        },
        "HostConfig": {"PortBindings": {"5432/tcp": [{"HostIp": "127.0.0.1", "HostPort": "54332"}]}},
        "State": {"Running": True},
    }
    if issue in {"project", "service"}:
        info["Config"]["Labels"]["com.docker.compose." + issue] = "production"
    elif issue == "port":
        info["HostConfig"]["PortBindings"]["5432/tcp"][0]["HostPort"] = "5432"
    elif issue == "host":
        info["HostConfig"]["PortBindings"]["5432/tcp"][0]["HostIp"] = "0.0.0.0"
    elif issue == "stopped":
        info["State"]["Running"] = False
    monkeypatch.setattr(fault_guard, "compose", lambda *args, **kwargs: b"owned-container\n")
    monkeypatch.setattr(
        fault_guard.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(stdout=json.dumps([info]).encode())
    )
    if issue == "valid":
        assert fault_guard.owned_service("postgres") == "owned-container"
    else:
        with pytest.raises(RuntimeError):
            fault_guard.owned_service("postgres")
    if issue == "stopped":
        assert fault_guard.owned_service("postgres", require_running=False) == "owned-container"
