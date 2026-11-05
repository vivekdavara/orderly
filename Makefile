PY := .venv/bin/python

.PHONY: venv lint test check services-up services-down load-test

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

# The day-4 load test: 10,000 orders through each target, open loop at 100/s, then closed loop with 64 clients
# (scripts/load_test.py; needs services-up and the ports 8112-8114 free). About 8 minutes.
load-test:
	$(PY) scripts/load_test.py monolith --orders 10000 --rate 100 --json results/load/monolith-rate100.json
	$(PY) scripts/load_test.py services --orders 10000 --rate 100 --json results/load/services-rate100.json
	$(PY) scripts/load_test.py monolith --orders 10000 --concurrency 64 --json results/load/monolith-c64.json
	$(PY) scripts/load_test.py services --orders 10000 --concurrency 64 --json results/load/services-c64.json
