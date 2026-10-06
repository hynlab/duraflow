import pytest
from scripts.fault_guard import project_name


def test_destructive_faults_require_explicit_local_project_opt_in(monkeypatch):
    monkeypatch.delenv('DURAFLOW_ALLOW_DESTRUCTIVE_TESTS', raising=False)
    with pytest.raises(RuntimeError):
        project_name()
    monkeypatch.setenv('DURAFLOW_ALLOW_DESTRUCTIVE_TESTS', 'isolated-compose')
    monkeypatch.setenv('COMPOSE_PROJECT_NAME', 'production')
    with pytest.raises(RuntimeError):
        project_name()
    monkeypatch.setenv('COMPOSE_PROJECT_NAME', 'duraflow-qualification-test')
    monkeypatch.setenv('DURAFLOW_TEST_POSTGRES', 'postgresql+psycopg://u:p@database.example/production')
    monkeypatch.setenv('DURAFLOW_TEST_PULSAR', 'pulsar://localhost:6650')
    with pytest.raises(RuntimeError):
        project_name()
    monkeypatch.setenv('DURAFLOW_TEST_POSTGRES', 'postgresql+psycopg://u:p@localhost:5432/test')
    assert project_name() == 'duraflow-qualification-test'
