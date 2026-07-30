#!/usr/bin/env python3
"""Verify WAL recovery after PgDog is killed during two-phase commit."""

from __future__ import annotations

import asyncio
import os
import signal
import sys
import time
from pathlib import Path
from typing import BinaryIO

import asyncpg

SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG = SCRIPT_DIR / "pgdog.toml"
USERS = SCRIPT_DIR / "users.toml"

POSTGRES_HOST = "127.0.0.1"
POSTGRES_PORT = 5432
PGDOG_PORT = int(os.environ.get("PGDOG_PORT", "6432"))
SHARDS = ("shard_0", "shard_1")
CLIENTS = int(os.environ.get("PGDOG_CRASH_RECOVERY_CLIENTS", "100"))
ITERATIONS = int(os.environ.get("PGDOG_CRASH_RECOVERY_ITERATIONS", "10"))
SEGMENT_SIZES = (
    ("10kb", 10 * 1024),
    ("50kb", 50 * 1024),
    ("150kb", 150 * 1024),
    ("1mb", 1024 * 1024),
    ("16mb", 16 * 1024 * 1024),
)
PREPARED_QUERY = "SELECT count(*) FROM pg_prepared_xacts WHERE gid LIKE '__pgdog_2pc_%'"


async def connect(database: str, port: int) -> asyncpg.Connection:
    return await asyncpg.connect(
        host=POSTGRES_HOST,
        port=port,
        user="pgdog",
        password="pgdog",
        database=database,
        statement_cache_size=0,
        timeout=5,
    )


async def shard_prepared_transactions() -> dict[str, int]:
    query = (
        "SELECT count(*) FROM pg_prepared_xacts "
        "WHERE database = current_database() "
        "AND gid LIKE '__pgdog_2pc_%'"
    )

    async def count(shard: str) -> tuple[str, int]:
        connection = await connect(shard, POSTGRES_PORT)
        try:
            return shard, await connection.fetchval(query)
        finally:
            await connection.close()

    return dict(await asyncio.gather(*(count(shard) for shard in SHARDS)))


async def prepare_database() -> None:
    create_table = """
        CREATE TABLE IF NOT EXISTS omnisharded_two_pc (
            id BIGINT PRIMARY KEY,
            value VARCHAR
        )
    """
    for shard in SHARDS:
        connection = await connect(shard, POSTGRES_PORT)
        try:
            await connection.execute(create_table)
        finally:
            await connection.close()

    dangling = await shard_prepared_transactions()
    if any(dangling.values()):
        raise RuntimeError(f"test started with prepared transactions: {dangling}")


async def wait_for_pgdog(
    process: asyncio.subprocess.Process,
    timeout: float = 10.0,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.returncode is not None:
            raise RuntimeError(f"PgDog exited during startup with {process.returncode}")
        try:
            connection = await connect("pgdog", PGDOG_PORT)
            await connection.close()
            await asyncio.sleep(0.05)
            if process.returncode is not None:
                raise RuntimeError(
                    f"PgDog exited during startup with {process.returncode}"
                )
            return
        except (OSError, asyncpg.PostgresError, asyncio.TimeoutError):
            await asyncio.sleep(0.05)
    raise TimeoutError("PgDog did not become ready")


async def start_pgdog(
    binary: Path,
    log: BinaryIO,
    segment_size: int,
) -> asyncio.subprocess.Process:
    environment = os.environ.copy()
    environment.setdefault("NODE_ID", "pgdog-two-pc-1")
    environment["PGDOG_TWO_PHASE_COMMIT_WAL_SEGMENT_SIZE"] = str(segment_size)
    process = await asyncio.create_subprocess_exec(
        str(binary),
        "--config",
        str(CONFIG),
        "--users",
        str(USERS),
        cwd=SCRIPT_DIR,
        env=environment,
        stdout=log,
        stderr=asyncio.subprocess.STDOUT,
    )
    await wait_for_pgdog(process)
    return process


async def stop_process(process: asyncio.subprocess.Process | None) -> None:
    if process is None or process.returncode is not None:
        return
    try:
        process.terminate()
    except ProcessLookupError:
        return
    try:
        await asyncio.wait_for(process.wait(), timeout=5)
    except asyncio.TimeoutError:
        process.kill()
        await process.wait()


async def run_client(
    client_number: int,
    ready: asyncio.Queue[None],
    start: asyncio.Event,
) -> None:
    connection = await connect("pgdog", PGDOG_PORT)
    sequence = 0
    base = time.time_ns() + client_number * 10_000_000

    try:
        ready.put_nowait(None)
        await start.wait()
        while True:
            async with connection.transaction():
                await connection.execute(
                    """
                    INSERT INTO omnisharded_two_pc (id, value)
                    VALUES ($1, 'crash recovery')
                    """,
                    base + sequence,
                )
            sequence += 1
    finally:
        await connection.close()


async def wait_for_prepared_transaction(timeout: float = 10.0) -> None:
    connection = await connect(SHARDS[0], POSTGRES_PORT)
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            if await connection.fetchval(PREPARED_QUERY) > 0:
                return
        raise TimeoutError("did not observe an in-flight prepared transaction")
    finally:
        await connection.close()


async def crash_during_two_pc(process: asyncio.subprocess.Process) -> None:
    ready: asyncio.Queue[None] = asyncio.Queue()
    start = asyncio.Event()
    clients = [
        asyncio.create_task(
            run_client(number, ready, start),
            name=f"two-pc-client-{number}",
        )
        for number in range(CLIENTS)
    ]
    monitor: asyncio.Task[None] | None = None

    try:
        await asyncio.wait_for(
            asyncio.gather(*(ready.get() for _ in range(CLIENTS))),
            timeout=30,
        )
        monitor = asyncio.create_task(
            wait_for_prepared_transaction(),
            name="prepared-transaction-monitor",
        )
        start.set()

        done, _ = await asyncio.wait(
            [monitor, *clients],
            return_when=asyncio.FIRST_COMPLETED,
        )
        if monitor not in done:
            failed_client = next(iter(done))
            await failed_client
            raise RuntimeError("workload client exited before PgDog was killed")
        await monitor

        # Freeze PgDog at the observed phase-one boundary so it cannot send
        # COMMIT PREPARED before the fatal signal is delivered.
        process.send_signal(signal.SIGSTOP)
        await asyncio.sleep(0.01)
        process.send_signal(signal.SIGKILL)
        await asyncio.wait_for(process.wait(), timeout=5)
    finally:
        if monitor is not None:
            monitor.cancel()
        for client in clients:
            client.cancel()
        tasks = clients if monitor is None else [monitor, *clients]
        await asyncio.gather(*tasks, return_exceptions=True)


async def wait_for_recovery(timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    last = {}
    while time.monotonic() < deadline:
        last = await shard_prepared_transactions()
        if not any(last.values()):
            return
        await asyncio.sleep(0.1)
    raise TimeoutError(f"recovery left prepared transactions on shards: {last}")


async def run_test() -> None:
    if CLIENTS <= 0:
        raise ValueError("PGDOG_CRASH_RECOVERY_CLIENTS must be greater than zero")
    if ITERATIONS <= 0:
        raise ValueError("PGDOG_CRASH_RECOVERY_ITERATIONS must be greater than zero")

    binary_value = os.environ.get("PGDOG_BIN")
    if not binary_value:
        raise RuntimeError("PGDOG_BIN must point to the PgDog executable")

    binary = Path(binary_value).expanduser().resolve()
    if not binary.is_file():
        raise FileNotFoundError(f"PGDOG_BIN does not exist: {binary}")

    await prepare_database()

    pgdog = None
    recoveries_needed = 0
    completed = 0
    log_path = SCRIPT_DIR / "crash_recovery.log"
    with log_path.open("wb") as log:
        try:
            for size_name, segment_size in SEGMENT_SIZES:
                print(f"testing {size_name} WAL segments for {ITERATIONS} iterations")

                for iteration in range(1, ITERATIONS + 1):
                    pgdog = await start_pgdog(binary, log, segment_size)
                    await crash_during_two_pc(pgdog)
                    pgdog = None

                    before_recovery = await shard_prepared_transactions()
                    needed_recovery = any(before_recovery.values())
                    recoveries_needed += int(needed_recovery)

                    pgdog = await start_pgdog(binary, log, segment_size)
                    await wait_for_recovery()

                    after_recovery = await shard_prepared_transactions()
                    if any(after_recovery.values()):
                        raise RuntimeError(
                            f"{size_name} iteration {iteration}: recovery left "
                            f"prepared transactions on shards: {after_recovery}"
                        )

                    completed += 1
                    print(
                        f"{size_name} iteration {iteration}/{ITERATIONS}: "
                        f"before={before_recovery}, after={after_recovery}, "
                        f"cleanup_needed={needed_recovery}"
                    )

                    await stop_process(pgdog)
                    pgdog = None
        finally:
            await stop_process(pgdog)

    print(
        f"completed {completed} crash/recovery iterations; "
        f"cleanup was needed in {recoveries_needed}"
    )


def main() -> int:
    try:
        asyncio.run(run_test())
        return 0
    except Exception as error:
        print(f"two_pc crash recovery test failed: {error}", file=sys.stderr)
        log_path = SCRIPT_DIR / "crash_recovery.log"
        if log_path.exists():
            print(f"PgDog log: {log_path}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
