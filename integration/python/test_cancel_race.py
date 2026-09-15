#!/usr/bin/env python3
import threading
import time
from contextlib import suppress

import psycopg

RUNS = 5

DIRECT_DSN = "user=pgdog password=pgdog dbname=pgdog host=127.0.0.1 port=5432"
PGDOG_DSN = "user=pgdog password=pgdog dbname=pgdog host=127.0.0.1 port=6432"


def suppress_query(conn, sql: str) -> None:
    with suppress(BaseException):
        conn.execute(sql)


def one(dsn: str) -> bool:
    a, *holders, b = [psycopg.connect(dsn, autocommit=True) for _ in range(9)]
    workers = []
    for conn in holders:
        thread = threading.Thread(
            target=suppress_query, args=(conn, "select pg_sleep(1)"), daemon=True
        )
        thread.start()
        workers.append(thread)
    thread = threading.Thread(
        target=suppress_query, args=(a, "select pg_sleep(.5)"), daemon=True
    )
    thread.start()
    workers.append(thread)
    time.sleep(0.03)

    error = []

    def wait() -> None:
        try:
            b.execute("select pg_sleep(.5)")
        except BaseException as exc:
            error.append(exc)

    thread = threading.Thread(target=wait, daemon=True)
    thread.start()
    workers.append(thread)
    cancels = [
        threading.Thread(target=a.cancel_safe, kwargs={"timeout": 1}, daemon=True)
        for _ in range(2)
    ]
    for thread in cancels:
        thread.start()
    for thread in cancels:
        thread.join(1)
    for thread in workers:
        thread.join(2)
    for conn in [a, b, *holders]:
        conn.close()
    return bool(error and getattr(error[0], "sqlstate", None) == "57014")


def test_cross_client_cancel_race():
    for label, dsn in (("direct", DIRECT_DSN), ("pgdog", PGDOG_DSN)):
        hits = sum(one(dsn) for _ in range(RUNS))
        print(f"{label}: cross_client_57014={hits}/{RUNS}")
        assert hits == 0, (
            f"{label}: B was canceled by A's CancelRequest {hits}/{RUNS} runs"
        )
