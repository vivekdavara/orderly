"""The synthetic catalogue: fixed prices, restocking on a second run, the CLI."""

from __future__ import annotations

import psycopg
import pytest

from orderly.migrate import migrate
from orderly.seed import main, products, seed


def test_products_are_numbered_and_priced_by_their_number() -> None:
    catalogue = products(100)
    assert [p.sku for p in catalogue[:3]] == ["SKU-0001", "SKU-0002", "SKU-0003"]
    assert catalogue == products(100)
    assert all(100 <= p.price_cents <= 10_000 and p.currency == "USD" for p in catalogue)
    assert len({p.price_cents for p in catalogue}) > 90  # spread, not one price


@pytest.mark.parametrize("count", [0, 10_000])
def test_the_count_is_bounded(count: int) -> None:
    with pytest.raises(ValueError, match="between 1 and 9999"):
        products(count)


@pytest.mark.parametrize("service", ["orders", "monolith"])
def test_seeding_again_restocks_instead_of_adding(fresh_database: str, service: str) -> None:
    with psycopg.connect(fresh_database, autocommit=True) as conn:
        migrate(conn, service)
        assert seed(conn, 20, 5) == 20
        conn.execute("UPDATE products SET stock = 0 WHERE sku = 'SKU-0001'")
        assert seed(conn, 20, 7) == 20
        assert conn.execute("SELECT count(*), min(stock), max(stock) FROM products").fetchone() == (20, 7, 7)


def test_the_cli(fresh_database: str, capsys: pytest.CaptureFixture[str]) -> None:
    with psycopg.connect(fresh_database, autocommit=True) as conn:
        migrate(conn, "orders")
    assert main(["orders", "--products", "3", "--stock", "9", "--url", fresh_database]) == 0
    assert capsys.readouterr().out == "orders: 3 products with 9 each\n"
    assert main(["orders", "--stock", "-1", "--url", fresh_database]) == 1
    assert "stock can't be negative (-1)" in capsys.readouterr().err
