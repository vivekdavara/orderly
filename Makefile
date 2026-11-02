PY := .venv/bin/python

.PHONY: venv lint test check services-up services-down

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

# The local RabbitMQ node (127.0.0.1:5673) and the databases on the local PostgreSQL 14 (localhost:5432).
services-up:
	scripts/rabbitmq.sh start
	scripts/postgres.sh create

services-down:
	scripts/rabbitmq.sh stop
