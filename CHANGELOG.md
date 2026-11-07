# Changelog

## v1.0.0 (2026-11-06)

The first release: an order-processing monolith and the three services split from it, with the plumbing that keeps the
split correct and the measurements of what it costs. Built over five days; the README's "Review of day N" sections
say what each day's reviews found and fixed.

### Day 5: dead letters and the release

- `python -m orderly.dead_letters`: list the dead letters of each work queue, replay them into the queue they died in
  (a header-less copy through the default exchange, confirmed before the dead letter is acknowledged, so its attempts
  start over and only that consumer sees it), or discard them by id. Dead letters that can't be replayed (undecodable,
  or of a type not routed to that queue) are kept.
- Each service's `/health` counts its dead-letter queue (`dead_letters.waiting`), never failing on it.
- The case it exists for, end to end: a refund dead-lettered while the provider's refunds were down, replayed once they
  were back, the customer refunded.
- README "Results at v1.0.0", WALKTHROUGH "In two minutes" and "Dead letters and the release".

### Day 4: contracts, failure injection, the load test

- Consumer-driven contracts (`contracts/*.json`), checked against each consumer (it does what `then` says, and ignores
  every field it doesn't list) and against each producer (its own code, driven into `given`, sends what is listed).
- Failure injection through a TCP proxy in front of RabbitMQ: the broker cut mid-run, every message twice, both outboxes
  re-sent whole, a provider losing its answers.
- The 10,000-order load test (`scripts/load_test.py`), open and closed loop, monolith against services, end to end to
  the outcome event, the books checked after every run; a broker outage mode.
- Found by it and fixed: the services idled 2-3 s after a broker restart (now they wake on the reconnect); no
  backpressure (now an opt-in cap on orders awaiting payment, 503 with Retry-After).

### Day 3: the payment and notification services, the saga

- The payment service (charge and refund commute; a refund before any charge voids at the provider), the notification
  service (one e-mail per order and kind), the saga as a pure transition function with a deadline and compensations.
- The idempotent consumer: `processed_messages` in the handler's transaction, retries through wait queues (1, 5, 30 s),
  dead letters, a bounded stop that drains.

### Day 2: the monolith, the order service, the outbox

- The monolith baseline (the whole order in one request and one transaction), and its flaws pinned by tests.
- The order service, sharing the monolith's order-taking code with the charge replaced by an outbox write.
- The transactional outbox and its relay: `SKIP LOCKED` claims, batches published then confirmed, per-row retries,
  LISTEN/NOTIFY wake-up.

### Day 1: the design and the foundations

- DESIGN.md; the message contract (a strict decoder, a canonical encoder, a published JSON Schema); the RabbitMQ
  topology as code (quorum queues, dead-letter queues, a delivery limit); numbered SQL migrations under an advisory
  lock; the local RabbitMQ and PostgreSQL scripts; CI with service containers.
