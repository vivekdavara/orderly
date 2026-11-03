-- Each outbox row's own retries (day-2 review): a row whose publish failed waits next_attempt_at before it is claimed
-- again, so rows that keep failing (an unroutable type, say) no longer hold every row behind them, and attempts and
-- last_error show an operator what is stuck and why.
ALTER TABLE outbox
    ADD COLUMN attempts        integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    ADD COLUMN next_attempt_at timestamptz,
    ADD COLUMN last_error      text CHECK (length(last_error) <= 1000),
    ADD CHECK ((attempts = 0) = (last_error IS NULL));
