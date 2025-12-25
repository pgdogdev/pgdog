import psycopg2
import pytest
from globals import admin

queries = [
    "SELECT 1",
    "CREATE TABLE IF NOT EXISTS test_conn_reads(id BIGINT)",
    "INSERT INTO test_conn_reads (id) VALUES (1)",
    "INSERT INTO test_conn_reads (id) VALUES (2)",
    "INSERT INTO test_conn_reads (id) VALUES (3)",
    "SELECT * FROM test_conn_reads WHERE id = 1",
    "SELECT * FROM test_conn_reads WHERE id = 2",
    "SELECT * FROM test_conn_reads WHERE id = 3",
    "SET work_mem TO '4MB'",
    "SET work_mem TO '6MB'",
    "SET work_mem TO '8MB'",
]


@pytest.fixture
def conn_reads():
    return psycopg2.connect(
        "host=127.0.0.1 port=6432 user=pgdog password=pgdog "
        "options='-c pgdog.role=replica'"
    )


@pytest.fixture
def conn_writes():
    return psycopg2.connect(
        "host=127.0.0.1 port=6432 user=pgdog password=pgdog "
        "options='-c pgdog.role=primary'"
    )


@pytest.fixture
def conn_default():
    return psycopg2.connect("host=127.0.0.1 port=6432 user=pgdog password=pgdog")


def test_conn_writes(conn_writes):
    admin().execute("SET query_parser TO 'off'")
    for query in queries:
        conn_writes.autocommit = True
        cursor = conn_writes.cursor()
        cursor.execute(query)
    admin().execute("SET query_parser TO 'auto'")


def test_conn_reads(conn_reads, conn_writes):
    admin().execute("SET query_parser TO 'off'")

    conn_writes.autocommit = True
    conn_reads.autocommit = True

    conn_writes.cursor().execute(
        "CREATE TABLE IF NOT EXISTS test_conn_reads(id BIGINT)"
    )

    read = False
    for query in queries:
        cursor = conn_reads.cursor()
        try:
            cursor.execute(query)
        except psycopg2.errors.ReadOnlySqlTransaction:
            # Some will succeed because we allow reads
            # on the primary.
            read = True
    admin().execute("SET query_parser TO 'auto'")

    conn_writes.cursor().execute("DROP TABLE IF EXISTS test_conn_reads")
    assert read, "expected some queries to hit replicas and fail"


def test_transactions_writes(conn_writes):
    admin().execute("SET query_parser TO 'off'")

    for query in queries:
        conn_writes.cursor().execute(query)
        conn_writes.commit()

    admin().execute("SET query_parser TO 'auto'")


def test_transactions_reads(conn_reads):
    admin().execute("SET query_parser TO 'off'")
    read = False

    for query in queries:
        try:
            conn_reads.cursor().execute(query)
        except psycopg2.errors.ReadOnlySqlTransaction:
            # Some will succeed because we allow reads
            # on the primary.
            read = True
        conn_reads.commit()

    assert read, "expected some queries to hit replicas and fail"
    admin().execute("SET query_parser TO 'auto'")


def test_transaction_reads_explicit(conn_reads, conn_writes):
    conn_reads.autocommit = True
    admin().execute("SET query_parser TO 'off'")

    conn_writes.cursor().execute(
        "CREATE TABLE IF NOT EXISTS test_conn_reads(id BIGINT)"
    )
    conn_writes.commit()

    cursor = conn_reads.cursor()

    read = False

    for _ in range(15):
        cursor.execute("BEGIN")
        try:
            cursor.execute("INSERT INTO test_conn_reads (id) VALUES (1)")
            cursor.execute("COMMIT")
        except psycopg2.errors.ReadOnlySqlTransaction:
            read = True
            cursor.execute("ROLLBACK")

    assert read, "expected some queries to hit replicas and fail"

    for _ in range(15):
        cursor.execute("BEGIN READ ONLY")  # Won't be parsed, doesn't matter to PgDog
        cursor.execute("SELECT 1")
        cursor.execute("ROLLBACK")

    admin().execute("SET query_parser TO 'on'")
