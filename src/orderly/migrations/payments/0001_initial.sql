-- The payment service's database (orderly_payments): one payment per order, the outbox, the inbox.

CREATE TABLE payments (
    id           uuid PRIMARY KEY,
    order_id     uuid NOT NULL UNIQUE,
    -- CHARGED: the provider took the money. DECLINED: it refused. REFUNDED: charged, then given back.
    -- VOIDED: a refund came before any charge, so this order is never charged (DESIGN.md "The saga").
    status       text NOT NULL CHECK (status IN ('CHARGED', 'DECLINED', 'REFUNDED', 'VOIDED')),
    amount_cents bigint NOT NULL CHECK (amount_cents BETWEEN 1 AND 1000000000000),
    currency     char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    -- The provider's ids for the charge and the refund.
    charge_ref   text,
    refund_ref   text,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    CHECK ((status IN ('CHARGED', 'REFUNDED')) = (charge_ref IS NOT NULL)),
    CHECK ((status = 'REFUNDED') = (refund_ref IS NOT NULL))
);

CREATE TABLE outbox (
    id           bigserial PRIMARY KEY,
    message_id   uuid NOT NULL UNIQUE,
    message_type text NOT NULL,
    body         bytea NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);
CREATE INDEX outbox_unpublished ON outbox (id) WHERE published_at IS NULL;

CREATE TABLE processed_messages (
    consumer     text NOT NULL,
    message_id   uuid NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, message_id)
);
