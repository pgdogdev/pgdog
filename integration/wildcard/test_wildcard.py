"""
Integration test for wildcard database routing with passthrough auth.

Tests that pgdog dynamically creates pools when a client connects with
a (user, database) pair not explicitly listed in the config — using
the wildcard "*" template — and forwards the client's actual
credentials to Postgres for verification.

Setup (run before this test):
  CREATE USER wildcard_tester WITH PASSWORD 'Xk9mP2vLq7w';
  CREATE DATABASE wildcard_test_db OWNER wildcard_tester;
  -- plus a table:
  CREATE TABLE items (id serial PRIMARY KEY, name text NOT NULL);
  INSERT INTO items (name) VALUES ('alpha'), ('beta'), ('gamma');
"""

import psycopg
import sys

PGDOG_HOST = "127.0.0.1"
PGDOG_PORT = 6432

# Existing user configured explicitly in users.toml.
EXPLICIT_USER = "pgdog"
EXPLICIT_PASS = "pgdog"

# New user unknown to pgdog — only exists in Postgres.
WILDCARD_USER = "wildcard_tester"
WILDCARD_PASS = "Xk9mP2vLq7w"
WILDCARD_DB = "wildcard_test_db"


def connect(dbname, user, password):
    return psycopg.connect(
        host=PGDOG_HOST,
        port=PGDOG_PORT,
        dbname=dbname,
        user=user,
        password=password,
        autocommit=True,
    )


# ------------------------------------------------------------------ #
# 1. Baseline: explicit pool still works
# ------------------------------------------------------------------ #

def test_explicit_pool():
    """The explicit (pgdog, pgdog) pool works as before."""
    conn = connect("pgdog", EXPLICIT_USER, EXPLICIT_PASS)
    cur = conn.cursor()
    cur.execute("SELECT current_user, current_database()")
    row = cur.fetchone()
    assert row[0] == "pgdog", f"expected user pgdog, got {row[0]}"
    assert row[1] == "pgdog", f"expected db pgdog, got {row[1]}"
    conn.close()
    print("  PASS  explicit pool (pgdog/pgdog)")


# ------------------------------------------------------------------ #
# 2. Wildcard: known user (pgdog) → unknown database
# ------------------------------------------------------------------ #

def test_known_user_wildcard_db():
    """User 'pgdog' connects to 'wildcard_test_db' — a database
    pgdog doesn't know about. Passthrough auth forwards pgdog's
    credentials to Postgres."""
    conn = connect(WILDCARD_DB, EXPLICIT_USER, EXPLICIT_PASS)
    cur = conn.cursor()
    cur.execute("SELECT current_database()")
    db = cur.fetchone()[0]
    assert db == WILDCARD_DB, f"expected {WILDCARD_DB}, got {db}"
    conn.close()
    print(f"  PASS  known user → wildcard db ({WILDCARD_DB})")


# ------------------------------------------------------------------ #
# 3. Wildcard: unknown user + unknown database (the main scenario)
# ------------------------------------------------------------------ #

def test_unknown_user_wildcard_db():
    """User 'wildcard_tester' (unknown to pgdog) connects to
    'wildcard_test_db' (also unknown). Both user and database are
    resolved via the wildcard template, and passthrough auth
    forwards the real credentials to Postgres."""
    conn = connect(WILDCARD_DB, WILDCARD_USER, WILDCARD_PASS)
    cur = conn.cursor()
    cur.execute("SELECT current_user, current_database()")
    row = cur.fetchone()
    assert row[0] == WILDCARD_USER, f"expected user {WILDCARD_USER}, got {row[0]}"
    assert row[1] == WILDCARD_DB, f"expected db {WILDCARD_DB}, got {row[1]}"
    conn.close()
    print(f"  PASS  unknown user ({WILDCARD_USER}) → wildcard db ({WILDCARD_DB})")


def test_unknown_user_read_existing_data():
    """wildcard_tester reads the pre-seeded 'items' table through
    the wildcard pool."""
    conn = connect(WILDCARD_DB, WILDCARD_USER, WILDCARD_PASS)
    cur = conn.cursor()
    cur.execute("SELECT name FROM items ORDER BY id")
    rows = [r[0] for r in cur.fetchall()]
    assert rows == ["alpha", "beta", "gamma"], f"unexpected: {rows}"
    conn.close()
    print("  PASS  unknown user → read existing data")


def test_unknown_user_write_and_read():
    """wildcard_tester creates a table, writes, reads, and drops it
    — full lifecycle through the wildcard pool."""
    conn = connect(WILDCARD_DB, WILDCARD_USER, WILDCARD_PASS)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS wc_lifecycle")
    cur.execute("CREATE TABLE wc_lifecycle (id int, val text)")
    cur.execute("INSERT INTO wc_lifecycle VALUES (1, 'x'), (2, 'y')")
    cur.execute("SELECT val FROM wc_lifecycle ORDER BY id")
    rows = [r[0] for r in cur.fetchall()]
    assert rows == ["x", "y"], f"unexpected: {rows}"
    cur.execute("DROP TABLE wc_lifecycle")
    conn.close()
    print("  PASS  unknown user → full DDL+DML lifecycle")


# ------------------------------------------------------------------ #
# 4. Wrong password — pgdog should relay the Postgres auth error
# ------------------------------------------------------------------ #

def test_wrong_password_rejected():
    """wildcard_tester with a wrong password is rejected.
    Passthrough auth should forward the bad password to Postgres
    and relay the auth failure back."""
    try:
        conn = connect(WILDCARD_DB, WILDCARD_USER, "WRONG_PASSWORD")
        cur = conn.cursor()
        cur.execute("SELECT 1")
        conn.close()
        raise AssertionError("expected auth failure, but connection succeeded")
    except psycopg.OperationalError as e:
        err = str(e).lower()
        ok = ("password" in err or "authentication" in err
              or "auth" in err or "fatal" in err)
        assert ok, f"unexpected error: {e}"
    print("  PASS  wrong password → rejected")


# ------------------------------------------------------------------ #
# 5. Unknown user + unknown db — nonexistent database
# ------------------------------------------------------------------ #

def test_nonexistent_database():
    """wildcard_tester tries to connect to a database that doesn't
    exist in Postgres. The error should come from Postgres."""
    try:
        conn = connect("nope_db_xyz", WILDCARD_USER, WILDCARD_PASS)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        conn.close()
        raise AssertionError("expected error for nonexistent db")
    except psycopg.OperationalError as e:
        err = str(e).lower()
        ok = "does not exist" in err or "fatal" in err or "down" in err
        assert ok, f"unexpected error: {e}"
    print("  PASS  nonexistent db → correct error")


# ------------------------------------------------------------------ #
# 6. Multiple wildcard users concurrently
# ------------------------------------------------------------------ #

def test_concurrent_wildcard_users():
    """Both pgdog and wildcard_tester connect to wildcard_test_db
    at the same time — each gets their own pool."""
    conn1 = connect(WILDCARD_DB, EXPLICIT_USER, EXPLICIT_PASS)
    conn2 = connect(WILDCARD_DB, WILDCARD_USER, WILDCARD_PASS)

    cur1 = conn1.cursor()
    cur2 = conn2.cursor()

    cur1.execute("SELECT current_user")
    cur2.execute("SELECT current_user")

    assert cur1.fetchone()[0] == EXPLICIT_USER
    assert cur2.fetchone()[0] == WILDCARD_USER

    conn1.close()
    conn2.close()
    print("  PASS  concurrent wildcard users (pgdog + wildcard_tester)")


# ------------------------------------------------------------------ #
# 7. Unknown user connects to multiple databases
# ------------------------------------------------------------------ #

def test_wildcard_user_multiple_dbs():
    """wildcard_tester connects to wildcard_test_db and also to pgdog
    (the pgdog database grants connect to all users by default)."""
    for dbname in [WILDCARD_DB, "pgdog"]:
        conn = connect(dbname, WILDCARD_USER, WILDCARD_PASS)
        cur = conn.cursor()
        cur.execute("SELECT current_database()")
        db = cur.fetchone()[0]
        assert db == dbname, f"expected {dbname}, got {db}"
        conn.close()
    print("  PASS  wildcard user → multiple databases")


# ------------------------------------------------------------------ #

def main():
    print("=== Wildcard Passthrough Auth Integration Tests ===")
    print(f"    user: {WILDCARD_USER}, db: {WILDCARD_DB}")
    print()
    failures = 0
    total = 0

    tests = [
        ("explicit pool",          test_explicit_pool),
        ("known user → wc db",     test_known_user_wildcard_db),
        ("unknown user → wc db",   test_unknown_user_wildcard_db),
        ("read existing data",     test_unknown_user_read_existing_data),
        ("write+read lifecycle",   test_unknown_user_write_and_read),
        ("wrong password",         test_wrong_password_rejected),
        ("nonexistent db",         test_nonexistent_database),
        ("concurrent users",       test_concurrent_wildcard_users),
        ("user → multiple dbs",    test_wildcard_user_multiple_dbs),
    ]

    for name, test_fn in tests:
        total += 1
        try:
            test_fn()
        except Exception as e:
            print(f"  FAIL  {name}: {e}")
            failures += 1

    passed = total - failures
    print(f"\n=== Results: {passed}/{total} passed, {failures} failed ===")
    if failures > 0:
        sys.exit(1)
    print("All wildcard passthrough auth tests passed!")


if __name__ == "__main__":
    main()
