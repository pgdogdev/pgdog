import psycopg
import asyncpg


def admin():
    conn = psycopg.connect(
        "dbname=admin user=admin password=pgdog host=127.0.0.1 port=6432"
    )
    conn.autocommit = True
    return conn


def no_out_of_sync():
    conn = admin()
    cur = conn.cursor()
    cur.execute("SHOW POOLS;")
    column_names = [desc[0] for desc in cur.description]
    out_of_sync_idx = column_names.index("out_of_sync")
    pools = cur.fetchall()
    for pool in pools:
        assert pool[out_of_sync_idx] == 0


def sharded_sync():
    return psycopg.connect(
        user="pgdog",
        password="pgdog",
        dbname="pgdog_sharded",
        host="127.0.0.1",
        port=6432,
    )


def normal_sync():
    return psycopg.connect(
        user="pgdog", password="pgdog", dbname="pgdog", host="127.0.0.1", port=6432
    )


def direct_sync():
    return psycopg.connect(
        user="pgdog", password="pgdog", dbname="pgdog", host="127.0.0.1", port=5432
    )


async def sharded_async():
    return await asyncpg.connect(
        user="pgdog",
        password="pgdog",
        database="pgdog_sharded",
        host="127.0.0.1",
        port=6432,
        statement_cache_size=250,
    )


async def normal_async():
    return await asyncpg.connect(
        user="pgdog",
        password="pgdog",
        database="pgdog",
        host="127.0.0.1",
        port=6432,
        statement_cache_size=250,
    )
