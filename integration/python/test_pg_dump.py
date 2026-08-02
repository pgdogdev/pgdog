"""pg_dump must work against a pooled connection that already served one.

pg_dump creates a SQL-level prepared statement ("dumpfunc"). In transaction
pooling the server connection goes back into the pool at the end of the
transaction, so the next dump that lands on it fails with "prepared statement
already exists" unless the pooler cleans that state up.

Runs against a database with a single server connection, so every dump lands
on the one another dump just used.
"""

import os
import subprocess

import psycopg

DUMPS = 3


def connect():
    conn = psycopg.connect(
        user="pgdog",
        password="pgdog",
        dbname="pgdog_leak",
        host="127.0.0.1",
        port=6432,
    )
    conn.autocommit = True
    return conn


def pg_dump():
    return subprocess.run(
        ["pg_dump", "-h", "127.0.0.1", "-p", "6432", "-U", "pgdog", "-d", "pgdog_leak"],
        env=dict(os.environ, PGPASSWORD="pgdog"),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )


def test_repeated_dumps():
    # pg_dump only prepares its statement when there are functions to dump,
    # so don't rely on whatever else happens to live in the database.
    conn = connect()
    conn.execute("CREATE OR REPLACE FUNCTION public.pg_dump_probe() RETURNS int AS $$ SELECT 1 $$ LANGUAGE SQL")
    conn.close()

    for attempt in range(DUMPS):
        result = pg_dump()
        assert result.returncode == 0, (
            f"dump {attempt + 1} of {DUMPS} failed: {result.stderr.strip()}"
        )


def test_prepared_statement_with_dirty_connection():
    """A connection can need both a parameter reset and a deallocate."""
    conn = connect()
    conn.execute("SET pgdog.pin TO true")
    conn.execute("PREPARE pg_dump_probe_stmt AS SELECT 1")
    conn.close()

    conn = connect()
    left = conn.execute(
        "SELECT count(*) FROM pg_catalog.pg_prepared_statements "
        "WHERE name = 'pg_dump_probe_stmt'"
    ).fetchone()[0]
    conn.close()

    assert left == 0, "prepared statement outlived its client's checkin"
