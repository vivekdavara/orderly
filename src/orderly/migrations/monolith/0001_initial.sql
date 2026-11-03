-- The monolith's one database (orderly_monolith): what the three services split between them, in one schema. It is
-- the baseline (DESIGN.md "The monolith"): an order, its payment and its notification are written in one transaction,
-- with the payment provider called while that transaction is open. It publishes nothing, so it has no outbox.

-- The same catalogue as the order service's: the code that reserves stock is shared (orderly.ordering).
CREATE TABLE products (
    sku         text PRIMARY KEY CHECK (sku ~ '^[A-Z0-9][A-Z0-9-]{0,31}$'),
    name        text NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
    price_cents bigint NOT NULL CHECK (price_cents BETWEEN 1 AND 1000000000000),
    currency    char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    stock       integer NOT NULL CHECK (stock >= 0)
);

-- An order is inserted as AWAITING_PAYMENT before the provider is called (so its idempotency key is claimed first and
-- a concurrent retry waits instead of charging again) and decided before the transaction commits: no other
-- transaction ever sees it awaiting payment. There is no deadline and no refund: the charge happens in the request.
CREATE TABLE orders (
    id              uuid PRIMARY KEY,
    customer_id     text NOT NULL CHECK (customer_id ~ '^[A-Za-z0-9._:-]{1,64}$'),
    email           text NOT NULL CHECK (length(email) <= 254 AND email ~ '^[^@[:space:]]+@[^@[:space:]]+$'),
    status          text NOT NULL CHECK (status IN ('AWAITING_PAYMENT', 'CONFIRMED', 'CANCELLED')),
    total_cents     bigint NOT NULL CHECK (total_cents BETWEEN 1 AND 1000000000000),
    currency        char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    idempotency_key text UNIQUE CHECK (length(idempotency_key) BETWEEN 1 AND 200),
    request_hash    text CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    cancel_reason   text CHECK (cancel_reason IN ('payment_declined')),
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    CHECK ((status = 'CANCELLED') = (cancel_reason IS NOT NULL)),
    CHECK ((idempotency_key IS NULL) = (request_hash IS NULL))
);

CREATE TABLE order_lines (
    order_id         uuid NOT NULL REFERENCES orders (id),
    line_no          smallint NOT NULL CHECK (line_no >= 1),
    sku              text NOT NULL REFERENCES products (sku),
    quantity         integer NOT NULL CHECK (quantity BETWEEN 1 AND 1000),
    unit_price_cents bigint NOT NULL CHECK (unit_price_cents BETWEEN 1 AND 1000000000000),
    PRIMARY KEY (order_id, line_no),
    UNIQUE (order_id, sku)
);

-- One payment per order, written in the order's transaction: CHARGED or DECLINED by the provider.
CREATE TABLE payments (
    id           uuid PRIMARY KEY,
    order_id     uuid NOT NULL UNIQUE REFERENCES orders (id),
    status       text NOT NULL CHECK (status IN ('CHARGED', 'DECLINED')),
    amount_cents bigint NOT NULL CHECK (amount_cents BETWEEN 1 AND 1000000000000),
    currency     char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    charge_ref   text,
    created_at   timestamptz NOT NULL DEFAULT now(),
    CHECK ((status = 'CHARGED') = (charge_ref IS NOT NULL))
);

-- What was sent, in the same transaction too: a failed e-mail fails the order (DESIGN.md "The monolith").
CREATE TABLE notifications (
    id        bigserial PRIMARY KEY,
    order_id  uuid NOT NULL REFERENCES orders (id),
    kind      text NOT NULL CHECK (kind IN ('order_confirmed', 'order_cancelled')),
    recipient text NOT NULL CHECK (length(recipient) <= 254),
    subject   text NOT NULL,
    body      text NOT NULL,
    sent_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (order_id, kind)
);
