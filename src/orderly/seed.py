"""A synthetic catalogue, for running the monolith or the order service by hand and for the load test (day 4).

    python -m orderly.seed orders   [--products 100] [--stock 10000] [--url postgresql://...]
    python -m orderly.seed monolith

Products SKU-0001 ... SKU-NNNN, priced between 1.00 and 100.00 USD by their number (the same every run), each with
`--stock` units. Seeding again restocks: it sets the stock, it doesn't add to it. All of it is synthetic.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import psycopg

from orderly.config import database_url

# The databases with a catalogue.
CATALOGUES = ("orders", "monolith")


@dataclass(frozen=True)
class Product:
    sku: str
    name: str
    price_cents: int
    currency: str


def products(count: int) -> list[Product]:
    if not 1 <= count <= 9999:
        raise ValueError(f"between 1 and 9999 products, not {count}")
    # A fixed spread of prices: 100 to 10,000 cents, decided by the number alone.
    return [
        Product(f"SKU-{n:04d}", f"Synthetic product {n}", 100 + (n * 7919) % 9901, "USD") for n in range(1, count + 1)
    ]


def seed(conn: psycopg.Connection[Any], count: int, stock: int) -> int:
    """Insert or restock `count` products in one transaction; return how many."""
    if stock < 0:
        raise ValueError(f"stock can't be negative ({stock})")
    catalogue = products(count)
    with conn.transaction(), conn.cursor() as cursor:
        cursor.executemany(
            "INSERT INTO products (sku, name, price_cents, currency, stock) VALUES (%s, %s, %s, %s, %s)"
            " ON CONFLICT (sku) DO UPDATE SET name = excluded.name, price_cents = excluded.price_cents,"
            " currency = excluded.currency, stock = excluded.stock",
            [(p.sku, p.name, p.price_cents, p.currency, stock) for p in catalogue],
        )
    return len(catalogue)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m orderly.seed", description=__doc__.split("\n\n")[0])
    parser.add_argument("service", choices=CATALOGUES)
    parser.add_argument("--products", type=int, default=100)
    parser.add_argument("--stock", type=int, default=10_000)
    parser.add_argument("--url", help="the database (default: ORDERLY_<SERVICE>_DATABASE_URL or the local one)")
    args = parser.parse_args(argv)
    try:
        with psycopg.connect(args.url or database_url(args.service)) as conn:
            seeded = seed(conn, args.products, args.stock)
    except (ValueError, psycopg.Error) as error:
        print(f"{args.service}: {error}", file=sys.stderr)
        return 1
    print(f"{args.service}: {seeded} products with {args.stock} each")
    return 0


if __name__ == "__main__":
    sys.exit(main())
