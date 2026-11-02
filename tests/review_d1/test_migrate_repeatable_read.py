"""Defect: the advisory lock doesn't protect a migrator whose transactions are REPEATABLE READ (a database or role
with default_transaction_isolation set). Its snapshot is taken by the lock statement itself, before the lock is
granted, so after waiting for another migrator it doesn't see that migrator's work: it re-applies migration 1 and
fails (DuplicateTable) instead of finding the database up to date. Fix: pin READ COMMITTED for the transaction, or
lock before the snapshot."""

import threading
import time

import psycopg

from orderly.migrate import _LOCK_KEY, Migration, migrate

FILES = [Migration(1, "a", "CREATE TABLE a (x int)")]


def test_a_repeatable_read_migrator_that_waited_for_another_finds_the_database_up_to_date(
    fresh_database: str,
) -> None:
    outcome: list[object] = []
    backend: list[int] = []

    def second_migrator() -> None:
        try:
            with psycopg.connect(fresh_database, autocommit=True) as conn:
                conn.execute("SET default_transaction_isolation = 'repeatable read'")
                backend.append(conn.info.backend_pid)
                outcome.append(migrate(conn, "orders", FILES))
        except Exception as error:
            outcome.append(error)

    with psycopg.connect(fresh_database, autocommit=True) as first:
        first.execute("SELECT pg_advisory_lock(%s)", (_LOCK_KEY,))  # the first migrator is "running"
        waiter = threading.Thread(target=second_migrator)
        waiter.start()
        waiting = "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted AND pid = %s"
        for _ in range(250):  # until the second migrator is blocked on the lock
            if backend and first.execute(waiting, (backend[0],)).fetchone() == (1,):
                break
            time.sleep(0.02)
        else:
            raise AssertionError("the second migrator never waited for the lock")
        assert migrate(first, "orders", FILES) == [1]
        first.execute("SELECT pg_advisory_unlock(%s)", (_LOCK_KEY,))
        waiter.join(timeout=10)
    assert outcome == [[]]
