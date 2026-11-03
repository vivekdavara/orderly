"""Defect: under a database whose DateStyle isn't ISO, every POST /orders fails, after the provider has charged.

`read_order` (day 2) reads `orders.created_at`, a timestamptz, in psycopg's default text format, and psycopg can only
parse timestamptz text in DateStyle ISO ("can't parse timestamptz with DateStyle 'SQL, DMY'"). The pool pins the one
server setting the services depend on, the isolation level (`service._read_committed`), but not this one, so a database
or role configured with another DateStyle (`ALTER DATABASE ... SET DateStyle = 'SQL, DMY'`, here through the
connection's options) makes `take_order`'s read of the order it just wrote raise NotImplementedError inside the
transaction. Consequence: the order is rolled back and the client gets a 500 for every order; in the monolith the
provider was already called, so each attempt (and each retry) is charged with no order to show for it. GET /orders/{id}
fails the same way.
"""

from __future__ import annotations

import psycopg.conninfo
from helpers import count, running, stock

from orderly import monolith
from orderly.fakes import FakeMailer, FakeProvider
from orderly.service import Settings


async def test_orders_work_whatever_the_databases_datestyle(fresh_database: str) -> None:
    options = str(psycopg.conninfo.conninfo_to_dict(fresh_database)["options"])
    url = psycopg.conninfo.make_conninfo(fresh_database, options=f"{options} -c DateStyle=SQL,DMY")
    provider = FakeProvider()
    app = monolith.create_app(Settings("monolith", url, "amqp://unused/"), provider=provider, mailer=FakeMailer())
    async with running(app) as client:
        stock(fresh_database, [("WIDGET-1", 2599, 10)])
        body = {"customer_id": "c-42", "email": "ada@example.com", "lines": [{"sku": "WIDGET-1", "quantity": 1}]}
        try:
            response = await client.post("/orders", json=body)
            status = response.status_code
        except NotImplementedError as error:  # the ASGI transport re-raises the app's unhandled error (a 500)
            status = 500
            assert "DateStyle" in str(error)
    assert status == 201, "the order failed under DateStyle 'SQL, DMY'"
    assert count(fresh_database, "orders") == 1
    assert provider.charged_cents() == 2599
