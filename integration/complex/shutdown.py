import asyncpg
import asyncio
import psycopg
import sys


async def run(db):
    conns = []
    for i in range(5):
        conn = await asyncpg.connect(
            host="127.0.0.1",
            port=6432,
            database=db,
            user="pgdog",
            password="pgdog")

        await conn.execute("BEGIN")
        await conn.execute("SELECT 1") # sharded dbs need this because they don't checkout
                                       # conns from pool until first query
        conns.append(conn)

    admin = psycopg.connect("dbname=admin user=admin host=127.0.0.1 port=6432 password=pgdog")
    admin.autocommit = True # No transactions supported in admin DB.
    admin.execute("SHUTDOWN")

    for conn in conns:
        for i in range(25):
            await conn.execute("SELECT 1, 2, 3")
        await conn.execute("COMMIT")

if __name__ == "__main__":
    asyncio.run(run(sys.argv[1]))
