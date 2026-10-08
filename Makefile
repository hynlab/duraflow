.PHONY: check test integration native-check lint typecheck format format-check build production-gate
PYTHON ?= python
check: format-check lint typecheck test
lint:
	$(PYTHON) -m ruff check src tests examples scripts
format:
	$(PYTHON) -m ruff format src tests examples scripts
format-check:
	$(PYTHON) -m ruff format --check src tests examples scripts
typecheck:
	$(PYTHON) -m mypy src/duraflow
test:
	PYTHONPATH=src:. $(PYTHON) -m scripts.pytest_guard -m 'not integration' --cov=duraflow --cov-report=term-missing --junitxml=test-results.xml
integration:
	PYTHONPATH=src:. $(PYTHON) -m scripts.pytest_guard -m integration -v
native-check:
	$(PYTHON) scripts/codec_matrix.py
	DURAFLOW_REQUIRE_NATIVE=1 PYTHONPATH=src:. $(PYTHON) -m scripts.pytest_guard -m 'not fault' -v --cov=duraflow --cov-report=term-missing --cov-report=xml --cov-report=json --junitxml=native-results.xml
production-gate:
	$(PYTHON) scripts/production_gate.py coverage.json
build:
	$(PYTHON) -m build

.PHONY: codec-check
codec-check:
	$(PYTHON) scripts/codec_matrix.py

.PHONY: native-fault-check
native-fault-check:
	$(PYTHON) scripts/fault_guard.py
	DURAFLOW_REQUIRE_NATIVE=1 PYTHONPATH=src:. $(PYTHON) -m scripts.pytest_guard -m fault -v --cov=duraflow --cov-append --cov-report=json --cov-report=xml --junitxml=fault-results.xml

.PHONY: qualification-check
qualification-check:
	$(MAKE) native-check
	$(MAKE) native-fault-check
	$(MAKE) production-gate

.PHONY: message-soak-check extended-check
SOAK_SECONDS ?= 1800
message-soak-check:
	PYTHONPATH=src:. $(PYTHON) -m scripts.message_soak --seconds $(SOAK_SECONDS)
extended-check:
	$(MAKE) check
	$(MAKE) qualification-check
	$(MAKE) message-soak-check
	$(MAKE) build
