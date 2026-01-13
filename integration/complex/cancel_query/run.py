import asyncio
import sys

import asyncpg
import psycopg


SHUTDOWN_TIMEOUT = 1
SLEEP_SECONDS = 10000
APPLICATION_NAME = "pgdog_cancel_query"


async def trigger_shutdown() -> None:
    conn = await asyncpg.connect(
        host="127.0.0.1",
        port=6432,
        database="pgdog",
        user="pgdog",
        password="pgdog",
    )

    try:
        await conn.execute(f"SET application_name = '{APPLICATION_NAME}'")
        sleep_task = asyncio.create_task(conn.execute(f"SELECT pg_sleep({SLEEP_SECONDS})"))

        # Give the backend time to register the long running query.
        await asyncio.sleep(1)

        admin = psycopg.connect(
            "dbname=admin user=admin host=127.0.0.1 port=6432 password=pgdog"
        )
        admin.autocommit = True
        try:
            admin.execute("SHUTDOWN")
        finally:
            admin.close()

        try:
            await asyncio.wait_for(sleep_task, timeout=SHUTDOWN_TIMEOUT)
        except asyncio.TimeoutError:
            print("pg_sleep query did not terminate after PgDog shutdown", file=sys.stderr)
            raise SystemExit(1)
        except (asyncpg.exceptions.PostgresError, asyncpg.exceptions.InterfaceError):
            # Expected: connection terminates as PgDog shuts down.
            return
        except (ConnectionError, psycopg.Error):
            # psycopg errors propagate through asyncpg when connection drops.
            return
        else:
            print("pg_sleep query completed without interruption", file=sys.stderr)
            raise SystemExit(1)
    finally:
        try:
            await conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(trigger_shutdown())
