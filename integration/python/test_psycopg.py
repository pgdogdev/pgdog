import psycopg
import pytest
import random

from globals import admin, no_out_of_sync

def sharded():
    return psycopg.connect(
		user='pgdog',
		password='pgdog',
		dbname='pgdog_sharded',
		host='127.0.0.1',
		port=6432)

def normal():
    return psycopg.connect(
		user='pgdog',
		password='pgdog',
		dbname='pgdog',
		host='127.0.0.1',
		port=6432)

def setup(conn):
    try:
        conn.cursor().execute("DROP TABLE sharded")
    except psycopg.errors.UndefinedTable:
        conn.rollback()
        pass
    conn.cursor().execute("""CREATE TABLE sharded (
        id BIGINT,
        value TEXT,
        created_at TIMESTAMPTZ
    )""")
    conn.cursor().execute("TRUNCATE TABLE sharded")
    conn.commit()

def test_connect():
    for conn in [normal(), sharded()]:
        cur = conn.cursor()
        cur.execute("SELECT 1::bigint")
        one = cur.fetchall()
        assert len(one) == 1
        assert one[0][0] == 1
    no_out_of_sync()

def test_insert():
    for conn in [sharded()]:
        setup(conn)

        for start in [1, 10_000, 100_000, 1_000_000_000, 10_000_000_000, 10_000_000_000_000]:
            for _ in range(250):
                id = random.randint(start, start + 100)
                cur = conn.cursor()
                cur.execute("INSERT INTO sharded (id, value) VALUES (%s, %s) RETURNING *", (id, 'test'))
                results = cur.fetchall()

                assert len(results) == 1
                assert results[0][0] == id
                conn.commit()
    no_out_of_sync()
