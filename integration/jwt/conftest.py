"""
Shared fixtures for JWT integration tests.

Keys are read from /keys/ (mounted in Docker) or from the local ./keys/ directory
when running directly. The generate_keys.sh script creates them.
"""

import os
import time
from pathlib import Path

import jwt as pyjwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend


# ── Key loading ───────────────────────────────────────────────────────────────

def _find_keys_dir() -> Path:
    candidates = [
        Path("/keys"),
        Path(__file__).parent / "keys",
    ]
    for p in candidates:
        if (p / "private.pem").exists():
            return p
    raise FileNotFoundError(
        "RSA key pair not found. Run integration/jwt/generate_keys.sh first."
    )


@pytest.fixture(scope="session")
def keys_dir() -> Path:
    return _find_keys_dir()


@pytest.fixture(scope="session")
def private_key(keys_dir):
    return (keys_dir / "private.pem").read_bytes()


@pytest.fixture(scope="session")
def public_key(keys_dir):
    return (keys_dir / "public.pem").read_bytes()


# ── JWT helpers ────────────────────────────────────────────────────────────────

def make_token(private_key: bytes, sub: str, exp_offset: int = 300, **extra) -> str:
    """Create a signed RS256 JWT token."""
    payload = {
        "sub": sub,
        "iat": int(time.time()),
        "exp": int(time.time()) + exp_offset,
        **extra,
    }
    return pyjwt.encode(payload, private_key, algorithm="RS256")


@pytest.fixture(scope="session")
def token_factory(private_key):
    """Return a callable that creates JWT tokens signed with the test private key."""
    def _factory(sub: str, exp_offset: int = 300, **extra) -> str:
        return make_token(private_key, sub, exp_offset, **extra)
    return _factory


# ── Connection helpers ─────────────────────────────────────────────────────────

PGDOG_HOST = os.environ.get("PGDOG_HOST", "127.0.0.1")
PGDOG_PORT = int(os.environ.get("PGDOG_PORT", "6432"))
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "127.0.0.1")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))


def pgdog_dsn(user: str, token: str, dbname: str = "pgdog") -> str:
    return (
        f"host={PGDOG_HOST} port={PGDOG_PORT} "
        f"dbname={dbname} user={user} password={token}"
    )


def postgres_dsn(user: str = "pgdog", password: str = "pgdog", dbname: str = "pgdog") -> str:
    return (
        f"host={POSTGRES_HOST} port={POSTGRES_PORT} "
        f"dbname={dbname} user={user} password={password}"
    )
