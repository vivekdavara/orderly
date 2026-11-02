PY := .venv/bin/python

.PHONY: venv lint test check

venv:
	python3.11 -m venv .venv
	$(PY) -m pip install -q --upgrade pip
	$(PY) -m pip install -q -e '.[dev]'

lint:
	$(PY) -m ruff check .
	$(PY) -m ruff format --check .
	$(PY) -m mypy

test:
	$(PY) -m pytest

check: lint test
