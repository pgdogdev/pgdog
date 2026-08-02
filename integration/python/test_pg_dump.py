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

DUMPS = 3


def pg_dump():
    return subprocess.run(
        ["pg_dump", "-h", "127.0.0.1", "-p", "6432", "-U", "pgdog", "-d", "pgdog_leak"],
        env=dict(os.environ, PGPASSWORD="pgdog"),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )


def test_repeated_dumps():
    for attempt in range(DUMPS):
        result = pg_dump()
        assert result.returncode == 0, (
            f"dump {attempt + 1} of {DUMPS} failed: {result.stderr.strip()}"
        )
