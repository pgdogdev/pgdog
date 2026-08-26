"""Session parameters must not survive the client that set them.

Runs against a database with a single server connection, so the next client
always gets the connection the previous one used. current_setting() is used
instead of SHOW because SHOW can be answered by PgDog itself.
"""

import psycopg


def connect():
    conn = psycopg.connect(
        user="pgdog",
        password="pgdog",
        dbname="pgdog_leak",
        host="127.0.0.1",
        port=6432,
    )
    # Otherwise psycopg wraps each statement in a transaction and rolls it
    # back on close, undoing the state we're testing for.
    conn.autocommit = True
    return conn


def read(setting):
    conn = connect()
    value = conn.execute(f"SELECT current_setting('{setting}')").fetchone()[0]
    conn.close()

    return value


def test_reset_rolled_back():
    """A ROLLBACK brings back the value the RESET cleared.

    This is the sequence pg_dump -t <table> emits. SET search_path TO '' leaves
    an empty quoted identifier, hence the two spellings of "empty".
    """
    conn = connect()
    conn.execute("SET search_path TO ''")
    with conn.transaction():
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        conn.execute("RESET search_path")
        conn.execute("SELECT 1")
        raise psycopg.Rollback()
    conn.close()

    assert read("search_path") not in ("", '""')


def test_set_committed_after_connecting():
    """A SET that lands once the connection is already ours."""
    conn = connect()
    with conn.transaction():
        conn.execute("SELECT 1")
        conn.execute("SET statement_timeout TO '5s'")
    conn.close()

    assert read("statement_timeout") == "0"


def test_reset_committed():
    """A committed RESET is permanent and needs no undoing."""
    conn = connect()
    conn.execute("SET search_path TO public")
    with conn.transaction():
        conn.execute("SELECT 1")
        conn.execute("RESET search_path")
    conn.close()

    assert read("search_path") == '"$user", public'
