.PHONY: check test integration native-check lint typecheck format format-check build production-gate
check: format-check lint typecheck test
lint:
	ruff check src tests examples scripts
format:
	ruff format src tests examples scripts
format-check:
	ruff format --check src tests examples scripts
typecheck:
	mypy src/duraflow
test:
	PYTHONPATH=src:. pytest -m 'not integration' --cov=duraflow --cov-report=term-missing --junitxml=test-results.xml
integration:
	PYTHONPATH=src:. pytest -m integration -v
native-check:
	python scripts/codec_matrix.py
	DURAFLOW_REQUIRE_NATIVE=1 PYTHONPATH=src:. pytest -m 'not fault' -v --cov=duraflow --cov-report=term-missing --cov-report=xml --cov-report=json --junitxml=native-results.xml
production-gate:
	python scripts/production_gate.py coverage.json
build:
	python -m build

.PHONY: codec-check
codec-check:
	python scripts/codec_matrix.py

.PHONY: native-fault-check
native-fault-check:
	python scripts/fault_guard.py
	DURAFLOW_REQUIRE_NATIVE=1 PYTHONPATH=src:. pytest -m fault -v --junitxml=fault-results.xml
