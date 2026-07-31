from pathlib import Path
import subprocess
import sys

import psycopg
import pytest


ALEMBIC_CONFIG = Path(__file__).with_name("alembic.ini")
ALEMBIC_REVISION = "0001_pgdog_alembic"
SCHEMAS = ("shard_0", "shard_1")


def set_read_write_strategy(strategy):
    with psycopg.connect(
        user="admin",
        password="pgdog",
        dbname="admin",
        host="127.0.0.1",
        port=6432,
        autocommit=True,
    ) as admin:
        admin.execute(f"SET read_write_strategy TO '{strategy}'")


@pytest.fixture(scope="module", autouse=True)
def conservative_read_write_strategy():
    set_read_write_strategy("conservative")
    try:
        yield
    finally:
        set_read_write_strategy("aggressive")


def direct_connection(database):
    connection = psycopg.connect(
        user="pgdog",
        password="pgdog",
        dbname=database,
        host="127.0.0.1",
        port=5432,
    )
    connection.autocommit = True
    return connection


def run_alembic(schema, revision):
    subprocess.run(
        [
            sys.executable,
            "-m",
            "alembic",
            "-c",
            str(ALEMBIC_CONFIG),
            "-x",
            f"schema={schema}",
            "upgrade" if revision == "head" else "downgrade",
            revision,
        ],
        check=True,
    )


@pytest.mark.parametrize("schema", SCHEMAS)
def test_alembic_migration_on_schema_shard(schema):
    with direct_connection(schema) as direct:
        direct.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        direct.execute(f"DROP TABLE IF EXISTS {schema}.alembic_pgdog_test")
        direct.execute(f"DROP TABLE IF EXISTS {schema}.alembic_version")

    try:
        run_alembic(schema, "head")

        with direct_connection(schema) as direct:
            revision = direct.execute(
                f"SELECT version_num FROM {schema}.alembic_version"
            ).fetchone()
            assert revision == (ALEMBIC_REVISION,)

            direct.execute(
                f"INSERT INTO {schema}.alembic_pgdog_test (id, value) "
                "VALUES (1, 'migrated')"
            )
            row = direct.execute(
                f"SELECT id, value FROM {schema}.alembic_pgdog_test"
            ).fetchone()
            assert row == (1, "migrated")

        run_alembic(schema, "base")

        with direct_connection(schema) as direct:
            table = direct.execute(
                "SELECT to_regclass(%s)",
                (f"{schema}.alembic_pgdog_test",),
            ).fetchone()
            assert table == (None,)
            version_count = direct.execute(
                f"SELECT count(*) FROM {schema}.alembic_version"
            ).fetchone()
            assert version_count == (0,)
    finally:
        with direct_connection(schema) as direct:
            direct.execute(f"DROP TABLE IF EXISTS {schema}.alembic_pgdog_test")
            direct.execute(f"DROP TABLE IF EXISTS {schema}.alembic_version")
