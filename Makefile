.PHONY: check test integration lint typecheck format build
check: lint typecheck test
lint:
	ruff check src tests examples
format:
	ruff check --select I --fix src tests examples
	ruff format src tests examples
typecheck:
	mypy src/duraflow
test:
	PYTHONPATH=src:. pytest -m 'not integration' --cov=duraflow --cov-report=term-missing
integration:
	PYTHONPATH=src:. pytest -m integration -v
build:
	python -m build
