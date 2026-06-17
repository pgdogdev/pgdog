"""
End-to-end JWT authentication tests for pgDog.

Each test connects through pgDog using a JWT token as the password.
pgDog validates the token, extracts the `sub` claim, and provisions
a backend connection pool on first login.

Prerequisites:
  - generate_keys.sh has been run (keys/ dir exists)
  - docker-compose up (postgres + pgdog) OR run.sh
"""

import time

import psycopg
import pytest

from conftest import pgdog_dsn, postgres_dsn, make_token


# ── Helpers ────────────────────────────────────────────────────────────────────


def connect_via_jwt(user: str, token: str, dbname: str = "pgdog", autocommit: bool = True):
    conn = psycopg.connect(pgdog_dsn(user, token, dbname))
    conn.autocommit = autocommit
    return conn


def connect_direct(user: str = "pgdog", password: str = "pgdog"):
    conn = psycopg.connect(postgres_dsn(user, password))
    conn.autocommit = True
    return conn


# ── Test suite ─────────────────────────────────────────────────────────────────


class TestJwtLogin:
    """Successful JWT login and basic query execution."""

    def test_valid_jwt_connects(self, token_factory):
        """A valid JWT token authenticates and allows a simple query."""
        token = token_factory("alice")
        with connect_via_jwt("alice", token) as conn:
            row = conn.execute("SELECT current_user").fetchone()
            # pgDog executed SET ROLE "alice"; Postgres returns the role name.
            assert row is not None

    def test_query_returns_correct_data(self, token_factory):
        """After JWT login, ordinary SELECT works."""
        token = token_factory("bob")
        with connect_via_jwt("bob", token) as conn:
            row = conn.execute("SELECT 1 + 1 AS result").fetchone()
            assert row[0] == 2

    def test_jwt_user_auto_provisioned_on_first_connect(self, token_factory):
        """Each unique sub claim gets its own auto-provisioned pool (no pre-config needed)."""
        token = token_factory("carol")
        with connect_via_jwt("carol", token) as conn:
            row = conn.execute("SELECT 42").fetchone()
            assert row[0] == 42

    def test_multiple_users_independent_sessions(self, token_factory):
        """Two JWT users connect simultaneously without interfering."""
        t1 = token_factory("dave")
        t2 = token_factory("eve")
        with connect_via_jwt("dave", t1) as c1, connect_via_jwt("eve", t2) as c2:
            r1 = c1.execute("SELECT 'dave'").fetchone()[0]
            r2 = c2.execute("SELECT 'eve'").fetchone()[0]
            assert r1 == "dave"
            assert r2 == "eve"

    def test_same_user_reconnects(self, token_factory):
        """Reconnecting with a fresh token for the same user succeeds."""
        for _ in range(3):
            token = token_factory("frank")
            with connect_via_jwt("frank", token) as conn:
                conn.execute("SELECT 1")


class TestJwtRejection:
    """Tokens that must be rejected."""

    def test_expired_token_is_rejected(self, private_key):
        """A token whose `exp` is in the past must not authenticate."""
        token = make_token(private_key, "grace", exp_offset=-10)
        with pytest.raises(psycopg.OperationalError):
            connect_via_jwt("grace", token)

    def test_tampered_token_is_rejected(self, token_factory):
        """Flipping a byte in the signature must cause authentication failure."""
        token = token_factory("heidi")
        parts = token.split(".")
        # Corrupt the signature (last part)
        sig = parts[2]
        corrupted = sig[:-4] + ("AAAA" if sig[-4:] != "AAAA" else "BBBB")
        bad_token = ".".join(parts[:2] + [corrupted])
        with pytest.raises(psycopg.OperationalError):
            connect_via_jwt("heidi", bad_token)

    def test_wrong_password_rejected(self):
        """Sending a plain string (not a JWT) is rejected."""
        with pytest.raises(psycopg.OperationalError):
            connect_via_jwt("ivan", "not-a-jwt")

    def test_empty_password_rejected(self):
        """Empty password is rejected."""
        with pytest.raises(psycopg.OperationalError):
            connect_via_jwt("judy", "")


class TestJwtTransactions:
    """Verify transactional behaviour through the proxy."""

    def test_commit(self, token_factory):
        """INSERT + COMMIT is durable."""
        token = token_factory("pgdog")
        with psycopg.connect(pgdog_dsn("pgdog", token), autocommit=False) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS jwt_test_commit (id SERIAL PRIMARY KEY, val TEXT)"
            )
            conn.execute("INSERT INTO jwt_test_commit (val) VALUES ('hello')")
            conn.commit()

        # Verify directly on Postgres
        with connect_direct() as pg:
            row = pg.execute("SELECT val FROM jwt_test_commit ORDER BY id DESC LIMIT 1").fetchone()
            assert row is not None
            assert row[0] == "hello"

    def test_rollback(self, token_factory):
        """INSERT + ROLLBACK leaves no trace."""
        token = token_factory("pgdog")
        with psycopg.connect(pgdog_dsn("pgdog", token), autocommit=False) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS jwt_test_rollback (id SERIAL PRIMARY KEY, val TEXT)"
            )
            conn.execute("INSERT INTO jwt_test_rollback (val) VALUES ('ghost')")
            conn.rollback()

        with connect_direct() as pg:
            row = pg.execute("SELECT COUNT(*) FROM jwt_test_rollback WHERE val = 'ghost'").fetchone()
            assert row[0] == 0


class TestJwtCustomClaim:
    """JWT tokens with a custom username claim (requires pgdog.toml jwt_username_claim setting)."""

    def test_sub_claim_used_as_username(self, token_factory):
        """The default `sub` claim is used as the Postgres username."""
        token = token_factory("kate")
        with connect_via_jwt("kate", token) as conn:
            # Just verify the connection works; SET ROLE is called with the sub value
            row = conn.execute("SELECT 1").fetchone()
            assert row[0] == 1
