-- The order service's database (orderly_orders): products and their stock, orders, the outbox, the inbox.

CREATE TABLE products (
    sku         text PRIMARY KEY CHECK (sku ~ '^[A-Z0-9][A-Z0-9-]{0,31}$'),
    name        text NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
    price_cents bigint NOT NULL CHECK (price_cents BETWEEN 1 AND 1000000000000),
    currency    char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    -- Reserved by an order's creation, released by its cancellation; never below zero.
    stock       integer NOT NULL CHECK (stock >= 0)
);

CREATE TABLE orders (
    id               uuid PRIMARY KEY,
    customer_id      text NOT NULL CHECK (customer_id ~ '^[A-Za-z0-9._:-]{1,64}$'),
    email            text NOT NULL CHECK (length(email) <= 254 AND email ~ '^[^@[:space:]]+@[^@[:space:]]+$'),
    status           text NOT NULL CHECK (status IN ('AWAITING_PAYMENT', 'CONFIRMED', 'CANCELLED')),
    total_cents      bigint NOT NULL CHECK (total_cents BETWEEN 1 AND 1000000000000),
    currency         char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    -- The client's Idempotency-Key and a hash of its request, to tell a retry (same request) from a reused key.
    idempotency_key  text UNIQUE CHECK (length(idempotency_key) BETWEEN 1 AND 200),
    request_hash     text CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    payment_deadline timestamptz NOT NULL,
    cancel_reason    text CHECK (cancel_reason IN ('payment_declined', 'payment_timeout')),
    -- Set once payment.refund has been written, so a late success is refunded once.
    refund_requested boolean NOT NULL DEFAULT false,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    CHECK ((status = 'CANCELLED') = (cancel_reason IS NOT NULL)),
    CHECK ((idempotency_key IS NULL) = (request_hash IS NULL)),
    CHECK (NOT refund_requested OR status = 'CANCELLED')
);

-- The deadline sweeper's scan: only orders still waiting, oldest deadline first.
CREATE INDEX orders_awaiting_payment ON orders (payment_deadline) WHERE status = 'AWAITING_PAYMENT';

CREATE TABLE order_lines (
    order_id         uuid NOT NULL REFERENCES orders (id),
    line_no          smallint NOT NULL CHECK (line_no >= 1),
    sku              text NOT NULL REFERENCES products (sku),
    quantity         integer NOT NULL CHECK (quantity BETWEEN 1 AND 1000),
    unit_price_cents bigint NOT NULL CHECK (unit_price_cents BETWEEN 1 AND 1000000000000),
    PRIMARY KEY (order_id, line_no),
    UNIQUE (order_id, sku)
);

-- Messages written in the same transaction as the change they describe; the relay publishes them (day 2).
CREATE TABLE outbox (
    id           bigserial PRIMARY KEY,
    message_id   uuid NOT NULL UNIQUE,
    message_type text NOT NULL,
    body         bytea NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);
CREATE INDEX outbox_unpublished ON outbox (id) WHERE published_at IS NULL;

-- Messages already handled, per consumer: the idempotent consumer's check (day 3).
CREATE TABLE processed_messages (
    consumer     text NOT NULL,
    message_id   uuid NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, message_id)
);
