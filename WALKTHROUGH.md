# Orderly: walkthrough

The questions an interviewer is likely to ask about this project, with short answers grounded in the code. Day 1
covers the design, the message contracts, the RabbitMQ topology, the migrations and the scripts; day 2 the monolith,
taking an order, the order service and the outbox; later days add the other services.

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
(`InvalidMessage.reason`). The tests pin 57 such inputs and fuzz the rest.

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
`rabbitmq.sh` the stranger-pid test fails. The review's mutation pass then found 22 changes nothing caught; each has a
test now and fails it.

## The services (skeleton)

**What does a service do before it has any logic?**
`service.py`: at startup it migrates its own database (the advisory lock covers several instances starting together)
and declares the topology; if either fails, it doesn't start. `/health` is 200 only when the database answers within
2 s at the migration this code expects and the broker connection is open, and 503 naming each problem otherwise, so
a load balancer never sends traffic to an instance running against a stale schema.

## The monolith and taking an order (day 2)

**Walk me through the monolith's POST /orders.**
`monolith.py`, one transaction: replay the Idempotency-Key if it was used (`ordering.take_order`), reserve each line's
stock in sku order, insert the order as AWAITING_PAYMENT and its lines, call the fake provider, record the payment,
confirm (or cancel and release the stock), send the e-mail, record it, commit. 201 confirmed, 402 declined, 409 out of
stock, 503 if the provider or mailer fails.

**Why insert the order before charging, if nobody can see it until the commit anyway?**
Because inserting it claims the idempotency key. A retry with the same key that arrives while the first is still
talking to the provider blocks on the key's unique index, and after the first commits it gets a unique violation and
replays the first order. If the insert came after the charge, both requests would charge the customer before either
reached the index.

**What is actually wrong with the monolith? Show me, don't tell me.**
Two tests in `tests/test_monolith.py` pin it. `..._a_failure_after_the_charge_keeps_the_charge_and_the_retry_charges_again`:
the mailer is down, the transaction rolls back after the provider approved the charge, so the customer is charged
for an order that doesn't exist; the client retries with the same key, but no order was committed to replay, so the
retry is a new order with a new id, and the provider charges again. `..._the_providers_latency_is_paid_with_the_rows_locked`:
two orders for one product take two provider latencies end to end, because the second waits for the first's row
lock; for different products they overlap. `scripts/measure_intake.py` puts numbers on the second one (README
"Results (day 2)").

**How does an Idempotency-Key work, exactly?**
`ordering.take_order`. Same key and same request (SHA-256 of its canonical JSON): the order the first request made, as
it is now, with `Idempotent-Replayed: true`. Same key, different request: 422. Two at once: one waits on the unique
index and replays the other. The case that's easy to miss: a retry that waited for the stock its own first attempt took
finds no stock. Without a second look at the key it would answer 409 to a request that succeeded; `take_order` checks
the key again after an out-of-stock refusal (`test_a_retry_that_waited_for_the_last_unit_gets_the_order_not_a_409`).

**How do you stop overselling, and why can't two orders deadlock?**
`ordering.reserve`: `UPDATE products SET stock = stock - q WHERE sku = $1 AND stock >= q`. The row lock serializes
orders for one product, and under READ COMMITTED the `stock >= q` is re-evaluated after the wait, so the loser gets no
row and a 409. Deadlock: every transaction locks its products in sku order, whatever order the request lists them in,
so no two can wait on each other in a cycle. A test sends 40 concurrent orders listing the same four products in random
orders; I changed the code to lock in request order and the test failed with `DeadlockDetected`.

**Why pin READ COMMITTED?**
The reservation depends on re-checking `stock >= q` after waiting for another transaction's lock. Under REPEATABLE READ
that re-check would fail the transaction with a serialization error instead. The pool's `configure` callback sets it on
every connection, so a database whose default is something else doesn't change behaviour (tested with a connection
defaulting to REPEATABLE READ).

**What does the order service do differently?**
`orders.py`: the same order-taking code, then instead of calling the provider it writes `payment.charge` to its outbox
in the same transaction and answers 201 with the order AWAITING_PAYMENT. Nothing in the request touches the broker:
a test closes the broker connection and orders are still taken, their commands waiting in the outbox.

## The outbox (day 2)

**Why an outbox? Why not publish after the commit?**
Publish after commit: a crash between the two loses the message, and the order waits forever. Publish before commit:
a rollback after it sends a command for an order that doesn't exist. Writing the message in the order's own
transaction makes "the order exists" and "the command will be sent" the same fact; the relay turns it into a publish
afterwards, at least once.

**Walk me through the relay.**
`outbox.Relay.publish_batch`: in one transaction, claim up to 100 unpublished rows in id order `FOR UPDATE SKIP
LOCKED`, skipping rows still waiting for a retry; publish all of them on a confirming channel, then wait for the
confirms (5 s at most); mark the confirmed ones published, and record each failed one's error and next attempt;
commit. `run` repeats it, sleeping between batches until a NOTIFY (sent by `outbox.write`, delivered
on commit), the 1 s poll, or the stop. Errors back off from 0.1 s to 5 s.

**Can a message be lost? Published twice?**
Lost: no; a row is marked only after its confirm, and only in the transaction that claimed it. Twice: yes, by design,
when the relay crashes (or its database connection dies) after publishing and before the mark commits. The test
`test_a_crash_before_the_mark_publishes_the_batch_again_with_the_same_ids` shows the batch arriving twice, byte for
byte, so with the same `message_id`: that's what the consumers (day 3) deduplicate on.

**What does SKIP LOCKED buy?**
Two relays (two instances of a service) take different rows instead of the second waiting for the first's batch or
both publishing the same rows. The test runs two relays over 600 rows and gets each exactly once; without `SKIP
LOCKED` it fails.

**What if one message can't be published?**
If the failure is the row's own (the broker returned it as unroutable, or it doesn't decode), it stays unpublished
with `attempts`, `last_error` and a `next_attempt_at` (0.5 s after the failure, doubling to a minute); the rest of its
batch is marked, and later claims skip it until its time, so it can't hold up the rows behind it. If the failure is the
broker's (no confirm, a nack, a closed channel), the rows are left alone and the relay backs off instead. That was the
review's best finding: before, failed rows were just unpublished rows at the head of the id order, so 100 of them filled
every batch and the outbox stopped for good; and the relay backed off for 5 s on any partial failure, ignoring
NOTIFY, so one stuck row slowed every message.

**Why publish a whole batch before waiting for any confirm?**
One round trip per batch instead of per message. `scripts/measure_relay.py` measures it (README "Results (day 2)").

**What does "a stop never cancels a mark" mean in code?**
The relay checks its stop event only between batches; the service skeleton's shutdown sets the event and waits up to
30 s for the workers before cancelling anything. A test stops the relay while a batch is being published: the batch
still gets marked, so the next start doesn't send it again. The 30 s is not arbitrary: a batch can wait 5 s for a
pooled connection and 5 s for its confirms. It was 10 s against a 10 s confirm wait until the review showed a stop
landing as a batch started cancelling it after one message was confirmed, so that message was sent again after the
restart. A test now checks the margin.

**How did the relay die, and what did you change?**
Two ways, both found running it inside the order service. aiormq raises `CancelledError` into whoever is awaiting an
RPC when the connection closes under it (the relay was opening its channel), though nothing cancelled the relay's
task, so the relay ended as if cancelled. And while a robust connection reconnects, opening a channel raises a bare
`RuntimeError`. The relay now treats a `CancelledError` as a broker error unless its own task is being cancelled
(`Task.cancelling()`, Python 3.11), and turns that one call's `RuntimeError` into a `ConnectionError`. A test drops the
connection under a running order service and checks the outbox drains after the reconnect; another checks that a real
cancel still ends the relay. And `/health` reports a worker that has died, so the next unknown way would be visible.

## The scripts

**What does `rabbitmq.sh` guard against?**
Another RabbitMQ on the machine (Homebrew's own config enables the management UI, MQTT, STOMP and streams, each on a
port; the script uses its own config and no plugins), listening beyond loopback (AMQP, distribution and epmd are bound
to 127.0.0.1), and pid reuse: a live pid in a pid file proves nothing after a crash or reboot. RabbitMQ 4 reads its
directories from the environment, not the command line, so the proof that a pid is this directory's node is that it
holds this directory's log file open (`lsof`).

## The review

**What did the review find?**
A separate agent read the day-1 code with a correctness brief and wrote each finding as a failing test
(`tests/review_d1/`). Seven defects. The two I'd lead with: the dead-letter queues, the last stop, had RabbitMQ 4's
default delivery limit and nothing behind them, so a message an inspector crashed on 21 times vanished (now `-1`, no
limit); and the migrator's advisory lock didn't protect a database whose default isolation is REPEATABLE READ,
because the snapshot was taken by the lock statement before the lock was granted, so a migrator that waited didn't
see the other's work (now the transaction pins READ COMMITTED first). The others: CI's last step could never pass, the
PostgreSQL script reported success after a failed statement (`set -e` ignores the left of `&&`), the empty routing key,
control characters in e-mails, a correlation id that wasn't the order.

**Did the fixes hold?**
A second round looked only at what the fixes and the new code broke, and found three things: the service's pool
handed out connections the server had ended, so `/health` said the database was down once per stale connection (the
pool checks connections now); the e-mail pattern's `\s` is a different set in Rust, Python and the JSON Schema's
ECMA-262, so the published schema refused an e-mail `decode` accepted (whitespace is spelled out now, and a test runs
the schema's own pattern in Node); and the PostgreSQL script still read a failed existence check as "missing". Round
1's fix to the same script had covered only a failing DROP: fixes are where the next bugs are.

**And the mutation pass?**
It changed one thing at a time in the three core modules and listed the changes no test noticed (22 mutants as I
re-applied them): boundaries
(`MAX_CENTS`, 254 characters, a zero refund), spellings (uppercase in a UUID's last group, five fractional digits),
the queue arguments, exchange types, whitespace-only edits to a migration. Each has a test now, and I re-applied each
mutant to see it fail.

## The review (day 2)

**What did the review find?**
One agent, in its own worktree, with a correctness brief and findings as failing tests (`tests/review_d2/`). Four
defects, all fixed: rows that kept failing blocked the whole outbox once there were a batch of them; one stuck row put
every message on a 5 s backoff; a stop could cancel a batch between publish and mark (the 10 s against 10 s above);
and a database whose DateStyle isn't ISO made every order fail, after the monolith had charged, because psycopg parses
timestamptz text only in ISO (the pool pins DateStyle now, as it pins the isolation level).

**And the mutants?**
Eight changes the suite didn't notice, all in the relay but one: never going straight to the next batch, not resetting
the backoff, `FOR UPDATE` without `SKIP LOCKED` (correct but serial), clearing `wake` before waiting (a NOTIFY during
a batch lost), no confirm timeout, the order service's relay not listening, `>=` at the 10^12-cent limit, and the
look after `LISTEN` (already caught by a test written after the review's snapshot). Each has a test now, and each
mutant re-applied fails it.

**Did the fixes hold?**
A second round looked only at the fixes and found two defects in the per-row retries. A broker-wide failure (a deleted
exchange, confirms timing out) was charged to every row, so after the broker came back every row still sat out its own
delay; and the wait was counted from the claim's `now()`, the start of its transaction, so a failure late in a slow
batch was due again before it was recorded. The fix splits failures by whose they are: a returned (unroutable)
message or an undecodable row is the row's own, and it waits; anything else is the broker's, the rows are left alone,
and the relay backs off. And the wait starts at `clock_timestamp()`. As on day 1, the fixes were where the next bugs
were.

**What else did it suspect?**
That the NOTIFY test was flaky: channels are per database and the tests share one, so another run's writes counted.
The payload names the schema now. Its other suspicions are limitations in the README: `/health` doesn't see a stuck
outbox (it reports it now), a short `idle_in_transaction_session_timeout` would cut a batch, and two decimals for every
currency (the e-mails use each currency's minor unit now).

## What went wrong on day 1

- aio-pika logs a returned message instead of raising, so my first "unroutable is an error" test failed; fixed with
  `on_return_raises`.
- I assumed `nack(requeue=True)` counted toward the delivery limit; the broker showed it doesn't. That changed the
  retry design before any retry code existed.
- RabbitMQ 4.3 refuses a topic binding key with more than two `#`, which my random-pattern test hit at once.
- The script tests' port picker asked the OS for a port and added 20,000 for the distribution port: macOS's ephemeral
  ports start at 49152, so there was never room.
- A commit message claimed 2,000+ pairs for the topic differential; it was 1,584 (corrected in the README's commit;
  1,665 now that the empty pattern and key are in it).
- I started the service once against the local broker before the dead-letter fix, so the real queues existed with the
  old arguments and the next start would have been refused as drift (which is the point of declaring at startup). I
  deleted the empty queues and redeclared them.
- My own reading of the service skeleton found a pool left retrying in the background when its first connections
  timed out at startup: `open()` sat before the `try` whose `finally` closes it.

## What went wrong on day 2

- The order view refused PostgreSQL's timestamps: psycopg returns `timestamptz` in the session's time zone (New York
  here) and the API's time type accepts only UTC. Converted at the read.
- Pydantic searches for a header's `pattern` rather than matching it whole, so `Idempotency-Key: has space` passed an
  unanchored pattern. Anchored now, and the same pattern is used with `fullmatch`.
- The relay died twice under aio-pika (above); the first time, `/health` reported the dead relay as "stopped" because a
  task ended by a `CancelledError` counts as cancelled.
- RabbitMQ's `list_queues` showed `payment.commands` empty right after the order service's smoke run had published to
  it: a quorum queue's counters lag by a few seconds. Fetching from the queue showed the command.

## What's next

Day 3: the payment and notification services (their consumers and the payment service's outbox), the saga's
transition function and its consumer in the order service, the deadline sweeper, idempotent consumers
(`processed_messages`), dead letters and retries with backoff through wait queues.
