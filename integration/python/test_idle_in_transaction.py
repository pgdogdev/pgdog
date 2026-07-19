import asyncio
import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from globals import admin, direct_sync

WORKERS = 10


@pytest.fixture(autouse=True)
def set_idle_timeout_on_user():
    """ALTER USER to set idle_in_transaction_session_timeout, then RECONNECT."""
    direct = direct_sync()
    direct.autocommit = True
    direct.execute(
        "ALTER USER pgdog SET idle_in_transaction_session_timeout = '500ms'"
    )
    direct.close()

    # Tell pgdog to reconnect so the new setting takes effect
    adm = admin()
    adm.cursor().execute("RECONNECT")
    adm.close()

    yield

    # Reset the user setting
    direct = direct_sync()
    direct.autocommit = True
    direct.execute("ALTER USER pgdog RESET idle_in_transaction_session_timeout")
    direct.close()

    adm = admin()
    adm.cursor().execute("RECONNECT")
    adm.close()


async def _run_idle_in_transaction():
    """Single worker: open a transaction, idle past the timeout, verify the error."""
    engine = create_async_engine(
        "postgresql+asyncpg://pgdog:pgdog@127.0.0.1:6432/pgdog",
        pool_size=1,
        max_overflow=0,
    )
    session_factory = async_sessionmaker(engine)

    async with session_factory() as session:
        async with session.begin():
            # Confirm the timeout is set on this connection
            row = (
                await session.execute(
                    text("SHOW idle_in_transaction_session_timeout")
                )
            ).scalar()
            assert row == "500ms", f"expected timeout to be 500ms, got: {row}"

            pid = (
                await session.execute(text("SELECT pg_backend_pid()"))
            ).scalar()

            # Run a query so the connection is now idle *in* the transaction
            await session.execute(text("SELECT 1"))

            # Sleep longer than the 500ms timeout
            await asyncio.sleep(0.75)

            # asyncpg reads the FATAL during sleep and closes the connection
            try:
                await session.execute(text("SELECT 2"))
                raise AssertionError("expected connection to be closed")
            except Exception as e:
                assert "connection is closed" in str(
                    e
                ) or "connection was closed" in str(e), f"unexpected error: {e}"
                await session.rollback()

    await engine.dispose()

    # Give pgdog a moment to process the disconnect
    await asyncio.sleep(0.5)

    # Verify the backend connection was closed
    direct = direct_sync()
    direct.autocommit = True
    cur = direct.cursor()
    cur.execute(
        "SELECT pid FROM pg_stat_activity WHERE pid = %s",
        (pid,),
    )
    row = cur.fetchone()
    direct.close()

    assert (
        row is None
    ), f"expected postgres connection {pid} to be closed, but it still exists"


@pytest.mark.asyncio
async def test_idle_in_transaction_timeout():
    """Verify that pgdog forwards PostgreSQL's idle_in_transaction_session_timeout error
    under concurrent load."""
    tasks = [_run_idle_in_transaction() for _ in range(WORKERS)]
    await asyncio.gather(*tasks)


async def _disconnect_mid_transaction():
    """Single worker: begin a transaction, disconnect, verify rollback."""
    conn = await asyncpg.connect(
        user="pgdog",
        password="pgdog",
        database="pgdog",
        host="127.0.0.1",
        port=6432,
    )

    # Start a transaction and grab the backend pid
    await conn.execute("BEGIN")
    pid = await conn.fetchval("SELECT pg_backend_pid()")
    await conn.execute("SELECT 1")

    # Disconnect abruptly (without COMMIT/ROLLBACK)
    await conn.close()

    # Give pgdog a moment to clean up
    await asyncio.sleep(0.5)

    # Check via direct connection that the backend is idle (transaction was rolled back)
    direct = direct_sync()
    direct.autocommit = True
    cur = direct.cursor()
    cur.execute(
        "SELECT state FROM pg_stat_activity WHERE pid = %s",
        (pid,),
    )
    row = cur.fetchone()
    direct.close()

    assert row is not None, f"expected backend {pid} to still exist in the pool"
    assert row[0] == "idle", f"expected backend {pid} to be idle (rolled back), got: {row[0]}"


@pytest.mark.asyncio
async def test_disconnect_mid_transaction_rolls_back():
    """Disconnect in the middle of a transaction and verify pgdog rolls it back."""
    tasks = [_disconnect_mid_transaction() for _ in range(WORKERS)]
    await asyncio.gather(*tasks)
