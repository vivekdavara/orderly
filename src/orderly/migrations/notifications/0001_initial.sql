-- The notification service's database (orderly_notifications): what was sent, the inbox. It publishes nothing, so
-- it has no outbox.

CREATE TABLE notifications (
    id         bigserial PRIMARY KEY,
    order_id   uuid NOT NULL,
    kind       text NOT NULL CHECK (kind IN ('order_confirmed', 'order_cancelled')),
    recipient  text NOT NULL CHECK (length(recipient) <= 254),
    subject    text NOT NULL,
    body       text NOT NULL,
    -- The event that caused it.
    message_id uuid NOT NULL,
    sent_at    timestamptz NOT NULL DEFAULT now(),
    -- Even an event delivered twice under two ids can't send the same notification twice.
    UNIQUE (order_id, kind)
);

CREATE TABLE processed_messages (
    consumer     text NOT NULL,
    message_id   uuid NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, message_id)
);
