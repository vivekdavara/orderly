import threading
from pathlib import Path

import psycopg
import pytest

from orderly.config import SERVICES
from orderly.migrate import Migration, MigrationError, load, main, migrate, migrations


def tables(url: str) -> set[str]:
    with psycopg.connect(url) as conn:
        rows = conn.execute("SELECT tablename FROM pg_tables WHERE schemaname = current_schema()").fetchall()
    return {row[0] for row in rows}


def recorded(url: str) -> list[tuple[int, str]]:
    with psycopg.connect(url) as conn:
        return [(row[0], row[1]) for row in conn.execute("SELECT version, name FROM schema_migrations ORDER BY 1")]


def test_every_service_has_its_initial_migration() -> None:
    for service in SERVICES:
        [first, *_] = migrations(service)
        assert (first.version, first.name) == (1, "initial")


def test_an_unknown_service_is_an_error() -> None:
    with pytest.raises(MigrationError, match="unknown service 'inventory'"):
        migrations("inventory")


def write(folder: Path, files: dict[str, str]) -> Path:
    for name, text in files.items():
        (folder / name).write_text(text)
    return folder


def test_load_orders_by_number_and_skips_dotfiles(tmp_path: Path) -> None:
    folder = write(tmp_path, {"0002_b.sql": "B", "0001_a.sql": "A", ".DS_Store": "x"})
    assert load(folder) == [Migration(1, "a", "A"), Migration(2, "b", "B")]


@pytest.mark.parametrize(
    ("files", "message"),
    [
        ({"0001_a.sql": "", "0003_c.sql": ""}, r"without gaps or repeats; found \[1, 3\]"),
        ({"0002_b.sql": ""}, r"found \[2\]"),
        ({"0001_a.sql": "", "0001_b.sql": ""}, r"found \[1, 1\]"),
        ({"0001_a.sql": "", "0002_add_index.sq": ""}, "0002_add_index.sq: not a migration"),
        ({"1_a.sql": ""}, "1_a.sql: not a migration"),
        ({"0001_Add.sql": ""}, "0001_Add.sql: not a migration"),
    ],
)
def test_load_refuses_a_folder_that_isnt_a_clean_sequence(tmp_path: Path, files: dict[str, str], message: str) -> None:
    with pytest.raises(MigrationError, match=message):
        load(write(tmp_path, files))


def test_migrate_applies_everything_once(fresh_database: str) -> None:
    with psycopg.connect(fresh_database, autocommit=True) as conn:
        assert migrate(conn, "orders") == [1, 2]
        assert migrate(conn, "orders") == []
    assert {"products", "orders", "order_lines", "outbox", "processed_messages"} <= tables(fresh_database)
    assert recorded(fresh_database) == [(1, "initial"), (2, "outbox_retries")]


@pytest.mark.parametrize("service", SERVICES)
def test_each_services_migrations_apply_to_an_empty_database(fresh_database: str, service: str) -> None:
    with psycopg.connect(fresh_database, autocommit=True) as conn:
        assert migrate(conn, service) == [m.version for m in migrations(service)]


def test_new_migrations_are_applied_after_the_old_ones(fresh_database: str) -> None:
    first = [Migration(1, "a", "CREATE TABLE a (x int)")]
    second = [*first, Migration(2, "b", "CREATE TABLE b (y int); INSERT INTO a VALUES (1)")]
    with psycopg.connect(fresh_database, autocommit=True) as conn:
        assert migrate(conn, "orders", first) == [1]
        assert migrate(conn, "orders", second) == [2]
        assert conn.execute("SELECT x FROM a").fetchall() == [(1,)]
    assert recorded(fresh_database) == [(1, "a"), (2, "b")]


def test_a_failing_migration_changes_nothing(fresh_database: str) -> None:
    broken = [
        Migration(1, "a", "CREATE TABLE a (x int)"),
        Migration(2, "b", "CREATE TABLE b (y int); SELECT * FROM no_such_table"),
    ]
    with psycopg.connect(fresh_database, autocommit=True) as conn, pytest.raises(psycopg.errors.UndefinedTable):
        migrate(conn, "orders", broken)
    # Not even migration 1, nor the bookkeeping table: one transaction.
    assert tables(fresh_database) == set()


@pytest.mark.parametrize(
    ("later", "message"),
    [
        ([Migration(1, "a", "CREATE TABLE a (x bigint)")], "was applied as a with SHA-256"),
        ([Migration(1, "renamed", "CREATE TABLE a (x int)")], "was applied as a with"),
        ([], "the database has migration 1 \\(a\\); this code has only 0"),
    ],
)
def test_a_database_whose_history_differs_from_the_files_is_refused(
    fresh_database: str, later: list[Migration], message: str
) -> None:
    with psycopg.connect(fresh_database, autocommit=True) as conn:
        migrate(conn, "orders", [Migration(1, "a", "CREATE TABLE a (x int)")])
        with pytest.raises(MigrationError, match=message):
            migrate(conn, "orders", later)


def test_a_gap_in_the_recorded_history_is_refused(fresh_database: str) -> None:
    files = [Migration(1, "a", "SELECT 1"), Migration(2, "b", "SELECT 2"), Migration(3, "c", "SELECT 3")]
    with psycopg.connect(fresh_database, autocommit=True) as conn:
        migrate(conn, "orders", files)
        conn.execute("DELETE FROM schema_migrations WHERE version = 2")
        with pytest.raises(MigrationError, match=r"records migrations \[1, 3\]"):
            migrate(conn, "orders", files)


def test_migrators_started_together_apply_each_migration_once(fresh_database: str) -> None:
    # Slow enough that the others arrive while the first holds the lock.
    files = [Migration(1, "slow", "SELECT pg_sleep(0.3); CREATE TABLE a (x int)")]
    results: list[list[int]] = []
    errors: list[BaseException] = []
    start = threading.Barrier(4)

    def run() -> None:
        try:
            with psycopg.connect(fresh_database, autocommit=True) as conn:
                start.wait()
                results.append(migrate(conn, "orders", files))
        except BaseException as error:
            errors.append(error)

    threads = [threading.Thread(target=run) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
    assert errors == []
    assert sorted(results) == [[], [], [], [1]]


def test_the_command_line(fresh_database: str, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["payments", "--url", fresh_database]) == 0
    assert main(["payments", "--url", fresh_database]) == 0
    with psycopg.connect(fresh_database, autocommit=True) as conn:
        conn.execute("UPDATE schema_migrations SET sha256 = 'edited'")
    assert main(["payments", "--url", fresh_database]) == 1
    out, err = capsys.readouterr()
    assert out.splitlines() == ["payments: applied [1, 2], now at 2", "payments: up to date at 2"]
    assert "payments: migration 1 was applied as initial with SHA-256 edited..." in err


def test_a_connection_inside_a_transaction_is_refused(fresh_database: str) -> None:
    with psycopg.connect(fresh_database) as conn:  # not autocommit: the SELECT opens a transaction
        conn.execute("SELECT 1")
        with pytest.raises(MigrationError, match="outside any transaction"):
            migrate(conn, "orders")
        conn.rollback()
        assert migrate(conn, "orders") == [1, 2]  # idle again: fine, even without autocommit


# The rest kill mutants of migrate.py that the day-1 review found no test noticed.


def test_load_skips_pycache_but_refuses_a_directory_named_like_a_migration(tmp_path: Path) -> None:
    write(tmp_path, {"0001_a.sql": "A"})
    (tmp_path / "__pycache__").mkdir()
    assert load(tmp_path) == [Migration(1, "a", "A")]
    (tmp_path / "0002_b.sql").mkdir()
    with pytest.raises(MigrationError, match=r"0002_b\.sql: not a migration"):
        load(tmp_path)


def test_a_whitespace_only_edit_is_still_an_edit(fresh_database: str) -> None:
    with psycopg.connect(fresh_database, autocommit=True) as conn:
        migrate(conn, "orders", [Migration(1, "a", "CREATE TABLE a (x int)")])
        with pytest.raises(MigrationError, match="was applied as a with SHA-256"):
            migrate(conn, "orders", [Migration(1, "a", "CREATE TABLE a (x int)\n")])


def test_the_history_is_read_in_version_order_not_storage_order(fresh_database: str) -> None:
    files = [Migration(1, "a", "SELECT 1"), Migration(2, "b", "SELECT 2")]
    with psycopg.connect(fresh_database, autocommit=True) as conn:
        migrate(conn, "orders", files)
        # Rewrite row 1 so it is stored after row 2: a scan without ORDER BY now meets 2 first.
        conn.execute("UPDATE schema_migrations SET applied_at = applied_at WHERE version = 1")
        stored = [row[0] for row in conn.execute("SELECT version FROM schema_migrations")]
        assert stored == [2, 1], "the setup didn't reorder the rows"
        assert migrate(conn, "orders", files) == []
