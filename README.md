# Orderly

An order-processing monolith split into event-driven services: an **order** service, a **payment** service and a
**notification** service, each with its own PostgreSQL database, talking through RabbitMQ. The interesting part is the
plumbing that keeps such a split correct: a transactional outbox, idempotent consumers, a saga with compensations
(including a payment reply that never comes), retries, dead letters, and measurements of what the split costs against
the monolith.

Python 3.11, FastAPI, RabbitMQ 4.3 (aio-pika), PostgreSQL 14 (psycopg 3), pytest. MIT licensed.

**Status: day 1 of 5.** The design, the message contracts, the RabbitMQ topology, the database schemas and their
migrations, the local RabbitMQ and PostgreSQL scripts, and CI are done. The monolith and the services come on days 2
and 3 ([DESIGN.md](DESIGN.md) "Days").

## Architecture

```mermaid
flowchart LR
    C[Client] -->|POST /orders| O[order service :8112]
    O --- ODB[(orderly_orders)]
    P[payment service :8113] --- PDB[(orderly_payments)]
    N[notification service :8114] --- NDB[(orderly_notifications)]
    O -->|outbox relay| X{{RabbitMQ :5673}}
    P -->|outbox relay| X
    X -->|payment.commands| P
    X -->|order.payment-events| O
    X -->|notification.order-events| N
```

- The order service runs the saga (orchestration): it reserves stock, sends `payment.charge`, and confirms or cancels
  on the reply; if no reply comes by a deadline it cancels and sends `payment.refund`, because "no answer" doesn't mean
  "not charged". The payment service makes charge and refund commute, so their order doesn't matter.
- Notifications are choreographed: the notification service subscribes to `order.confirmed` and `order.cancelled`.
- No service publishes directly: each writes messages to an outbox table in the same transaction as its change, and
  a relay publishes them (at least once). Every consumer deduplicates by message id.

[DESIGN.md](DESIGN.md) is the full specification: the monolith baseline and what it gets wrong, orchestration vs
choreography, the saga's states, the message table, the topology, the outbox, the failure table.

## What's here (day 1)

| Path | What it is |
|---|---|
| `src/orderly/messages.py` | The contract: a JSON envelope around seven typed payloads; `decode` is strict and says why it refuses, `encode` is canonical |
| `docs/message-schema.json` | The same contract as a JSON Schema, generated from the code (`scripts/export_message_schema.py`) and checked by tests |
| `src/orderly/topology.py` | Exchanges, quorum queues, dead-letter queues and bindings as code; `routes()` says where each type goes |
| `src/orderly/broker.py` | Persistent, mandatory, confirmed publishing on a channel where a returned message is an error |
| `src/orderly/service.py` | The skeleton every service shares: migrate and declare at startup, `/health` (200 only with its database, schema and broker right), ports 8112-8114 |
| `src/orderly/migrate.py`, `src/orderly/migrations/` | Numbered SQL migrations per service, applied in one transaction under an advisory lock; edited history refused |
| `scripts/rabbitmq.sh` | A local RabbitMQ node on 127.0.0.1:5673, kept apart from any other RabbitMQ on the machine |
| `scripts/postgres.sh` | Creates, migrates, lists and drops Orderly's databases on the local PostgreSQL 14 (`ORDERLY_PG_PREFIX` renames them) |
| `scripts/measure_test_fixture.py` | Times CREATE DATABASE against CREATE SCHEMA (why each test gets a schema) |
| `tests/` | 231 tests: unit, property and integration tests against the real servers |
| `.github/workflows/ci.yml` | Lint, mypy --strict and the suite with PostgreSQL 14 and RabbitMQ 4.3 service containers |

## Run it

Needs Python 3.11, RabbitMQ (`brew install rabbitmq`) and a PostgreSQL 14 server on localhost:5432.

```bash
make venv              # .venv with the package and its dev tools
make services-up       # scripts/rabbitmq.sh start + scripts/postgres.sh create (databases, migrations)
make check             # ruff, ruff format --check, mypy --strict, pytest
make services-down     # stop the RabbitMQ node (the databases stay)
```

Run a service (it migrates its database and declares the topology first):

```bash
.venv/bin/python -m orderly.service orders     # 127.0.0.1:8112; payments 8113, notifications 8114
curl -s http://127.0.0.1:8112/health           # {"service":"orders","schema_expected":1,"schema":1,"status":"ok","problems":{}}
```

The scripts on their own:

```bash
scripts/rabbitmq.sh start | stop | status | url | logs [n] | reset
scripts/postgres.sh create | migrate | status | drop
.venv/bin/python -m orderly.migrate orders --url postgresql://localhost:5432/orderly_orders
```

`rabbitmq.sh` keeps its node's data, logs and config in `.rabbitmq/`, ignores Homebrew's `rabbitmq-env.conf` and
`enabled_plugins` (which would add the management UI, MQTT, STOMP and streams, each on another port), and binds AMQP,
Erlang distribution (25673) and epmd to loopback.

## Tests

`make check` runs 231 tests in about 29 s here (more while the machine is busy):

- **Messages** (84): the canonical bytes pinned; malformed inputs each refused for its own reason (duplicate keys,
  NaN, a full-width digit in a time, booleans and floats as integers, non-canonical UUIDs, a 5,000-digit number,
  100,000 nested brackets, a correlation id that isn't the order, a message naming itself as its cause...); the
  boundaries next to them accepted; Hypothesis round trips over all seven types; changed fields that decode or are
  refused, never raise anything else; arbitrary bytes; the published JSON Schema matching the code, accepting every
  encoded message as exactly one variant, refusing what it can express, and laxer than the decoder exactly where
  documented.
- **Topology** (28, on a real RabbitMQ): every type reaches exactly the queues `routes()` names; the queue arguments,
  exchange types and durability pinned (the broker refuses a redeclaration with others); an unroutable publish is an
  error, and `publish` refuses a channel that would only log it; rejected and poison messages are dead-lettered; the
  delivery limit's actual behaviour (below); `delete()` leaves nothing; `topic_matches` agrees with the broker on
  1,665 (pattern, key) pairs, the empty pattern and key among them.
- **Migrations** (25) and **schemas** (31, on PostgreSQL 14): applying once and in order; a failing migration leaves
  nothing, not even the bookkeeping; edited (whitespace included), renamed, newer and gapped histories refused, and
  history read in version order whatever the storage order; four migrators started at once apply once; a connection
  inside a transaction refused; each invariant of DESIGN.md "Data" enforced by the database itself.
- **The service skeleton** (14): each service starts by migrating its database and declaring the topology, then
  `/health` says 200; a schema behind or ahead of the code, a lost database pool, a database slower than the 2 s
  timeout, a closed or reconnecting broker connection each give 503 naming the problem; edited migration history or
  an unreachable broker stops the start; a pool that can't fill is closed; shutdown closes the pool and the broker.
- **The scripts**: `rabbitmq.sh` (10, each starting its own node, and a fake `lsof` printing 1 MB: start/stop/status, loopback-only listeners, a busy
  port, a foreign directory left alone, a live pid that isn't the node neither signalled nor forgotten, a stale pid
  file, a symlinked directory, a long `lsof` listing) and `postgres.sh` (10, on databases under a random `ORDERLY_PG_PREFIX`: create makes
  and migrates them once, with `PYTHON=python` as CI passes it; status; drop asks, then drops only its own four; a
  prefix that isn't a plain identifier refused).
- **The review's tests** (28, `tests/review_d1/` and `tests/review_d1r2/`): each failed on the code it was written
  against and passes now.
- And one that the package's version matches `pyproject.toml`.

A full run leaves nothing behind on the shared servers (no queue, exchange, schema or database; checked after a run
with `rabbitmqctl list_queues` / `list_exchanges` and `pg_namespace` / `pg_database`).

Tests that need a server find it at `ORDERLY_AMQP_URL` and `ORDERLY_TEST_DATABASE_URL` (defaults: the local ones) and
are skipped when it isn't there, except with `ORDERLY_REQUIRE_SERVICES=1` (CI sets it), where a missing server fails
them. Each test isolates itself on the shared servers: a random prefix on every exchange and queue it declares, and a
private schema in the test database.

## Results (day 1)

Measured on an Apple M3 laptop (macOS 26.6.2, on battery), PostgreSQL 14.20, RabbitMQ 4.3.6 (Erlang 28).

| What | Measured | Command |
|---|---|---|
| A poison message (its consumer dies before acking, every time) | dead-lettered after 1 + 10 deliveries, reason `delivery_limit` | `pytest tests/test_topology.py -k keeps_dying` |
| `basic.nack` with requeue, 20 times | never counted toward the limit (no `x-delivery-count`), never dead-lettered | `pytest tests/test_topology.py -k requeueing` |
| CREATE + DROP DATABASE vs SCHEMA | 36-198 + 12-21 ms vs 0.1-0.6 + 0.1-0.3 ms (5 runs each) | `.venv/bin/python scripts/measure_test_fixture.py` |
| Local RabbitMQ node, start to AMQP port open | 1.58, 1.58, 1.59 s | `ORDERLY_RABBITMQ_DIR=.scratch/rmq-measure ORDERLY_RABBITMQ_PORT=24601 ORDERLY_RABBITMQ_NODE=orderly-measure@localhost /usr/bin/time -p scripts/rabbitmq.sh start` (then `stop`; three times) |

The second row is the day's most useful finding: on RabbitMQ 4, requeueing a message is "released", not "failed",
so a consumer that requeues on a transient error would loop forever without the delivery limit ever stopping it. The
retries (day 3) therefore never requeue: they go through wait queues with their own attempt count.

## Review of day 1

One review agent, in its own git worktree, read the code at `5edf504` (15:33-15:48) with a correctness brief and
wrote each finding as a failing test (`tests/review_d1/`). Seven defects, all fixed:

| Defect | Fix |
|---|---|
| CI's last step could never pass: `postgres.sh` tested `[ -x "$PYTHON" ]`, false for `PYTHON=python` (a clean-clone rehearsal of the CI steps found it too) | `5f21e1e` |
| `postgres.sh create` and `drop` exited 0 after a failed statement (`q ... && echo`: `set -e` ignores the left of `&&`) | `d30e814` |
| The dead-letter queues had RabbitMQ 4's default delivery limit (20) and nothing behind them: a message whose inspector crashed on it 21 times was dropped | `b71cc16` |
| Under `default_transaction_isolation = 'repeatable read'` a migrator that waited for the lock didn't see the other's work and failed with DuplicateTable | `073733b` |
| `topic_matches` treated the empty routing key as one word, unlike RabbitMQ | `b82315e` |
| An e-mail with NUL or another control character decoded, then could never be stored | `c1f5ed1` |
| A `correlation_id` other than the payload's `order_id` decoded | `bf15f1a` |

Its mutation pass listed the non-equivalent survivors in `messages.py`, `topology.py` and `migrate.py` (22 mutants as
re-applied here); each now has a test, and each mutant was re-applied and fails it (`2d43faa`, `dcdd337`). Its suspicions (a self-caused message,
`publish` on a channel that only logs returns, `lsof | grep -q` under `pipefail`, a test that could leak an exchange)
were settled in `3c83729`. What held: no input made `decode` raise anything but `InvalidMessage`; the round trip held
for years 1 to 9999; the routing, the confirmed mandatory publish and both delivery-limit claims; the history checks
under READ COMMITTED; the scripts refusing what isn't theirs.

A second round (16:19-16:29, at `031b35f`) asked what the fixes and the new code broke. Three defects, all fixed:
`/health` answered 503 once per pooled connection the server had ended (no pool check; `ac9da17`); the e-mail
pattern's `\s` meant different sets in Rust, Python and ECMA-262, so the published schema refused an e-mail with
U+FEFF that `decode` accepted (whitespace now spelled out, checked against Node; `ea77e71`); and `postgres.sh` read a
failed existence check as "missing" and still exited 0 (`ebdd7ec`). Its surviving mutants (shutdown closing the
broker, the health timeout, the reconnecting state, the `lsof` pipe, the C1 boundary) are tested now, and "a schema
ahead of the code is unhealthy" is pinned as a decision (`dd3e265`). What held: READ COMMITTED with a SERIALIZABLE
connection and with the lock already held, `-1` really meaning no limit, cleanup on every failed start.

## Design decisions

- **Orchestration for payment, choreography for notifications**: the flow with a timeout and a refund lives in one
  pure transition function and one row per order; the side effect nobody waits for is a subscription.
- **Charge and refund commute** in the payment service (a refund first voids the payment), so retries and dead-letter
  replays can reorder them safely.
- **A strict decoder**: a message that doesn't decode never will, so it is dead-lettered at once, never retried;
  being strict about the canonical form means two services can't disagree on what a message says.
- **Quorum queues with at-least-once dead-lettering** and a delivery limit as the backstop for poison messages.
- **Mandatory, confirmed publishing on a channel that raises on returns**: aio-pika only logs a returned message by
  default, so an unroutable message looked like a success until the first test showed it.
- **The database enforces the invariants too** (stock never negative, a cancellation always has a reason, one payment
  per order...): the code is the first line, the schema the last.
- **Migrations in one transaction under an advisory lock**, history checked by SHA-256 and refused if it differs.

## Limitations (day 1)

- No service logic yet: each service starts, migrates and reports health, nothing more. The monolith and the order
  service are day 2, payment and notification day 3.
- The RabbitMQ script tests run only where RabbitMQ is installed; CI uses a service container and skips them.
- One RabbitMQ node: quorum queues are replicated only on a cluster, which this project doesn't run.
- The orders schema checks e-mails with PostgreSQL's `[:space:]`, the messages with Rust's `\s`; they differ on
  U+0085 (locale-dependent), so the database could hold an e-mail no `order.confirmed` can carry. The order service
  (day 2) will validate requests with the message types, leaving the database check as the backstop.

## License

MIT. See [LICENSE](LICENSE).
