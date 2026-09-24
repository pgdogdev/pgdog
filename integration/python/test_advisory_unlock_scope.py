import uuid

import psycopg
from psycopg.rows import dict_row
import pytest


@pytest.mark.parametrize("case", ["null_literal", "null_parameter", "expression", "null_values"])
@pytest.mark.parametrize("binary", [False, True])
def test_individual_unlock_keeps_other_session_locks(case, binary):
    application = f"unlock_scope_{uuid.uuid4().hex}"
    key = 2026092109
    with psycopg.connect(
        host="127.0.0.1", port=6432, user="pgdog", password="pgdog",
        dbname="pgdog_sharded", application_name=application, autocommit=True,
    ) as owner, psycopg.connect(
        host="127.0.0.1", port=6432, user="admin", password="pgdog",
        dbname="admin", autocommit=True, row_factory=dict_row,
    ) as admin:
        def locked():
            # Complete another request so SHOW CLIENTS observes the prior one.
            owner.execute("SELECT 1")
            rows = admin.execute("SHOW CLIENTS").fetchall()
            return next(row["locked"] for row in rows if row["application_name"] == application)

        try:
            owner.execute(f"SELECT pg_advisory_lock({key})")
            assert locked() is True
            query, params, expected = {
                "null_literal": ("SELECT pg_advisory_unlock(NULL::bigint)", None, None),
                "null_parameter": ("SELECT pg_advisory_unlock(%s::bigint)", (None,), None),
                "expression": (f"SELECT pg_advisory_unlock((SELECT {key} + 1))", None, False),
                "null_values": ("SELECT pg_advisory_unlock(value) FROM (VALUES (NULL::bigint)) AS t(value)", None, None),
            }[case]
            with owner.cursor(binary=binary) as cursor:
                cursor.execute(query, params)
                assert cursor.fetchone()[0] is expected
            assert locked() is True
            owner.execute("SELECT pg_advisory_unlock_all()")
            assert locked() is False
        finally:
            owner.execute("SELECT pg_advisory_unlock_all()")
