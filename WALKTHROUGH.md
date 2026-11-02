# Orderly: walkthrough

The questions an interviewer is likely to ask about this project, with short answers grounded in the code. Day 1
covers the design, the message contracts, the RabbitMQ topology, the migrations and the scripts; later days add the
services.

## The project

**What is Orderly, in two sentences?**
An order-processing backend split from one monolith into an order, a payment and a notification service, each with its
own PostgreSQL database, talking through RabbitMQ. It is about the plumbing that keeps the split correct: a
transactional outbox, idempotent consumers, a saga with compensations, retries and dead letters, and measuring what the
split costs against the monolith it came from.

**Why keep the monolith?**
It is the baseline (DESIGN.md "The monolith"): one transaction does everything, so it is simple and strongly
consistent. Its problems are concrete: the payment provider is called with a transaction open, holding the stock rows'
locks and a pooled connection for the whole provider latency; an e-mail failure fails the order; everything deploys
together. The day-4 load test runs the same orders through both.

**What does the split cost?**
Consistency becomes eventual (an order is "awaiting payment" for a while and a client sees that), messages can arrive
twice or out of order, and there are more things to run and to watch. Every mechanism in the design exists to pay one
of those costs.

## The saga

**Orchestration or choreography, and why?**
Both, for different steps. Payment is orchestrated: the order service holds the saga's state in its own row and sends
`payment.charge` and `payment.refund` commands. The hard cases (no reply by a deadline, a success that arrives after
the order was cancelled) then live in one pure transition function that can be tested over every (state, message) pair,
and "where is order X?" has one answer. Notifications are choreographed: the notification service subscribes to
`order.confirmed` and `order.cancelled`, because nothing waits on it and nothing ever undoes it. The price of
orchestration is that the order service knows the payment service's commands.

**Walk me through an order whose payment reply never comes.**
Creation reserves stock and writes `payment.charge` to the outbox in one transaction, with a payment deadline. A
sweeper finds the order still awaiting payment after its deadline, releases the stock, cancels it with
`payment_timeout`, and writes both `order.cancelled` and `payment.refund`. The refund is there because no answer
doesn't mean no charge: the charge may have happened and its reply be stuck in a retry. If `payment.succeeded`
arrives later, the order stays cancelled and a refund is sent if one hasn't been.

**What if the refund reaches the payment service before the charge?**
Charge and refund commute there (DESIGN.md's table): a refund with no payment records a *voided* payment for the
refund's amount and replies `refunded` with 0 charged; a later charge for that order is refused (`voided`) and never
reaches the provider. That is why `payment.refund` carries the amount and currency (`messages.RefundPayment`): a
refund that comes first has nothing else to learn them from.

**Which steps are compensatable, which is the pivot?**
Reserving stock (undone by releasing it) and charging (undone by refunding) are compensatable; confirming the order is
the pivot, after which the saga only moves forward; the notification is retriable: it must eventually happen and
nothing is undone if it is late.

## Messages

**What does a message look like?**
A JSON envelope (`message_id`, `type`, `version`, `occurred_at`, `correlation_id`, `causation_id`) around a typed
payload; seven types (`messages.PAYLOADS`). The message id is chosen when the message is written to the outbox, so a
re-publish carries the same id: that is what consumers deduplicate on. The correlation id is the order id on every
message of its saga.

**Why such a strict decoder?**
A message that doesn't decode never will, so it shouldn't be retried: it goes straight to the dead-letter queue. And
being strict about the one canonical form means two services can't read the same bytes differently. `decode` refuses
duplicate keys (JSON parsers usually keep the last one silently), NaN and Infinity, booleans or floats where integers
belong (`True == 1` in Python), UUIDs and times in any but one spelling, unknown fields, and says why
(`InvalidMessage.reason`). The tests pin 39 such inputs and fuzz the rest.

**Give me an example of a subtle one.**
Times: Python's `strptime` accepts a full-width digit (`２026-...`) as a year, so the pattern says `[0-9]`, not `\d`
(`test_malformed_messages_are_refused_with_a_reason`). And encoding uses `isoformat`, not `strftime`, because
`strftime("%Y")` doesn't pad years before 1000 on Linux (it does on macOS), which would make a message encoded on one
machine undecodable on another.

## RabbitMQ

**What is the topology?**
`topology.py`: a direct exchange for commands and a topic exchange for events; three work queues (`payment.commands`,
`order.payment-events`, `notification.order-events`), each a quorum queue with a dead-letter queue behind it. Every
service declares it all at startup; redeclaring is harmless, and a queue that exists with other settings is an error,
so a drifted broker is noticed at once.

**Why quorum queues?**
They are RabbitMQ's durable, replicated queue type, and they have a delivery limit, which stops a poison message from
cycling forever. Dead-lettering is set to at-least-once so a message can't be lost on its way to the dead-letter queue.

**What does the delivery limit actually count?**
Measured on RabbitMQ 4.3 and pinned by two tests: a delivery whose consumer dies before acknowledging counts (the 11th
such delivery is dead-lettered with reason `delivery_limit`); a `nack` with `requeue=true` does not count at all
(RabbitMQ 4 treats it as "released"). So a handler that requeues on a transient error would loop forever. Retries
therefore never requeue: they go through wait queues with their own attempt count (day 3), and the limit is only the
backstop for a message that crashes its consumer.

**How do you know a published message wasn't dropped?**
Three things in `broker.py`: publisher confirms (the broker has taken it), `mandatory` (a message no queue is bound for
comes back), and a channel opened with `on_return_raises` (aio-pika otherwise only logs a returned message, so an
unroutable publish looked like a success: the first run of `test_a_message_no_queue_takes_is_an_error_not_a_drop`
showed it).

## Data and migrations

**How do migrations work?**
`migrate.py`: numbered SQL files per service, all pending ones applied in one transaction (PostgreSQL's DDL is
transactional, so a failure leaves nothing behind, not even the bookkeeping row), under a transaction-level advisory
lock taken before `CREATE TABLE IF NOT EXISTS schema_migrations` (which races on its own). Each applied file's name and
SHA-256 are recorded; a database whose history differs from the files (edited, renamed, gapped, newer) is refused, not
repaired. A misnamed file in the folder is an error rather than silently skipped.

**What do the schemas enforce?**
The invariants the code must keep anyway, as constraints: stock never below zero (and the reservation is a guarded
`UPDATE ... WHERE stock >= q` that matches no row instead), a cancelled order always has a reason and only a cancelled
one does, an idempotency key always has its request hash, a payment's provider references match its status, one
payment per order, one notification of each kind per order (`tests/test_schemas.py`).

## Testing

**How do tests share one RabbitMQ and one PostgreSQL without interfering?**
Every test declares its own copy of the topology under a random name prefix (`Topology(prefix=...)`) and deletes it
after, and gets a private schema in the test database first on its `search_path`. Not a database each: PostgreSQL
14's `CREATE DATABASE` forces a checkpoint, 36-198 ms against 0.1-0.6 ms for a schema
(`scripts/measure_test_fixture.py`).

**How do you stop CI from passing by skipping?**
Tests that need a server skip without one locally, but CI sets `ORDERLY_REQUIRE_SERVICES=1`, which turns a missing
server into a failure.

**How do you know the tests can fail?**
For the checks that matter most I removed the code and ran the tests: without the advisory lock the four-migrator test
fails; comparing only names (not SHA-256) fails the edited-history test; without the open-log-file proof in
`rabbitmq.sh` the stranger-pid test fails.

## The scripts

**What does `rabbitmq.sh` guard against?**
Another RabbitMQ on the machine (Homebrew's own config enables the management UI, MQTT, STOMP and streams, each on a
port; the script uses its own config and no plugins), listening beyond loopback (AMQP, distribution and epmd are bound
to 127.0.0.1), and pid reuse: a live pid in a pid file proves nothing after a crash or reboot. RabbitMQ 4 reads its
directories from the environment, not the command line, so the proof that a pid is this directory's node is that it
holds this directory's log file open (`lsof`).

## What went wrong on day 1

- aio-pika logs a returned message instead of raising, so my first "unroutable is an error" test failed; fixed with
  `on_return_raises`.
- I assumed `nack(requeue=True)` counted toward the delivery limit; the broker showed it doesn't. That changed the
  retry design before any retry code existed.
- RabbitMQ 4.3 refuses a topic binding key with more than two `#`, which my random-pattern test hit at once.
- The script tests' port picker asked the OS for a port and added 20,000 for the distribution port: macOS's ephemeral
  ports start at 49152, so there was never room.
- A commit message claimed 2,000+ pairs for the topic differential; it was 1,584 (corrected in the README's commit).

## What's next

Day 2: the monolith (`orderly_monolith`, one transaction per order), the order service extracted from it, events
through RabbitMQ, and the outbox relay (claim in id order with `SKIP LOCKED`, publish with confirms, mark, never cancel
a mark on stop).
