# Orderly

An order-processing monolith split into event-driven services: an order service, a payment service and a
notification service talking through RabbitMQ, each with its own PostgreSQL database, held together by a
transactional outbox, idempotent consumers and a saga with compensations.

Status: day 1 of 5 (design and scaffold). See [DESIGN.md](DESIGN.md).

License: MIT.
