#!/usr/bin/env python3
"""
Fast data loader for copy_data schema using PostgreSQL COPY protocol.
Generates ~50-100GB of test data.
"""

import argparse
import json
import random
import string
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Generator

import numpy as np
import psycopg


def random_text(length: int) -> str:
    return "".join(random.choices(string.ascii_uppercase, k=length))


def random_texts_numpy(count: int, length: int) -> list[str]:
    """Generate random texts using numpy for speed."""
    chars = np.random.randint(65, 91, size=(count, length), dtype=np.uint8)
    return ["".join(chr(c) for c in row) for row in chars]


def random_timestamps(count: int, days_back: int = 730) -> list[str]:
    """Generate random timestamps within the last N days."""
    now = datetime.now(timezone.utc)
    offsets = np.random.uniform(0, days_back * 86400, count)
    return [(now - timedelta(seconds=float(off))).isoformat() for off in offsets]


def format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds / 60:.1f}m"
    else:
        return f"{seconds / 3600:.1f}h"


def create_schema(conninfo: str):
    """Create the schema and tables."""
    with psycopg.connect(conninfo) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS copy_data")

        # Users table with partitions
        conn.execute("""
            CREATE TABLE IF NOT EXISTS copy_data.users (
                id BIGINT NOT NULL,
                tenant_id BIGINT NOT NULL,
                email VARCHAR NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                settings JSONB NOT NULL DEFAULT '{}'::jsonb,
                PRIMARY KEY(id, tenant_id)
            ) PARTITION BY HASH(tenant_id)
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS copy_data.users_0 PARTITION OF copy_data.users
                FOR VALUES WITH (MODULUS 2, REMAINDER 0)
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS copy_data.users_1 PARTITION OF copy_data.users
                FOR VALUES WITH (MODULUS 2, REMAINDER 1)
        """)

        # Orders table
        conn.execute("DROP TABLE IF EXISTS copy_data.order_items")
        conn.execute("DROP TABLE IF EXISTS copy_data.orders")
        conn.execute("""
            CREATE TABLE copy_data.orders (
                id BIGINT PRIMARY KEY,
                user_id BIGINT NOT NULL,
                tenant_id BIGINT NOT NULL,
                amount DOUBLE PRECISION NOT NULL DEFAULT 0.0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                refunded_at TIMESTAMPTZ,
                notes TEXT
            )
        """)

        # Order items table
        conn.execute("""
            CREATE TABLE copy_data.order_items (
                id BIGINT PRIMARY KEY,
                user_id BIGINT NOT NULL,
                tenant_id BIGINT NOT NULL,
                order_id BIGINT NOT NULL,
                product_name TEXT NOT NULL,
                amount DOUBLE PRECISION NOT NULL DEFAULT 0.0,
                quantity INT NOT NULL DEFAULT 1,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                refunded_at TIMESTAMPTZ
            )
        """)

        # Log actions table
        conn.execute("DROP TABLE IF EXISTS copy_data.log_actions")
        conn.execute("""
            CREATE TABLE copy_data.log_actions (
                id BIGINT PRIMARY KEY,
                tenant_id BIGINT,
                user_id BIGINT,
                action VARCHAR(50),
                details TEXT,
                ip_address VARCHAR(45),
                user_agent TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)

        # With identity table
        conn.execute("DROP TABLE IF EXISTS copy_data.with_identity")
        conn.execute("""
            CREATE TABLE copy_data.with_identity (
                id BIGINT GENERATED ALWAYS AS IDENTITY,
                tenant_id BIGINT NOT NULL,
                data TEXT
            )
        """)

        conn.execute("TRUNCATE copy_data.users CASCADE")
        conn.commit()
        print("Schema created successfully")


def load_users(conninfo: str, total: int, batch_size: int = 100_000) -> dict:
    """Load users table using COPY."""
    start = time.time()
    loaded = 0

    themes = ["light", "dark", "auto"]
    languages = ["en", "es", "fr", "de", "ja", "zh"]
    timezones = ["UTC", "America/New_York", "Europe/London", "Asia/Tokyo"]

    with psycopg.connect(conninfo) as conn:
        with conn.cursor() as cur:
            with cur.copy(
                "COPY copy_data.users (id, tenant_id, email, created_at, settings) FROM STDIN"
            ) as copy:
                for batch_start in range(0, total, batch_size):
                    batch_end = min(batch_start + batch_size, total)
                    batch_count = batch_end - batch_start

                    timestamps = random_timestamps(batch_count)
                    bios = random_texts_numpy(batch_count, 200)
                    addresses = random_texts_numpy(batch_count, 100)
                    phones = random_texts_numpy(batch_count, 20)
                    metadata = random_texts_numpy(batch_count, 100)

                    for i, idx in enumerate(range(batch_start + 1, batch_end + 1)):
                        tenant_id = ((idx - 1) % 1000) + 1
                        email = f"user_{idx}_tenant_{tenant_id}@example.com"
                        settings = json.dumps(
                            {
                                "theme": random.choice(themes),
                                "notifications": random.random() > 0.5,
                                "preferences": {
                                    "language": random.choice(languages),
                                    "timezone": random.choice(timezones),
                                    "bio": bios[i],
                                    "address": addresses[i],
                                    "phone": phones[i],
                                    "metadata": metadata[i],
                                },
                            }
                        )
                        copy.write_row((idx, tenant_id, email, timestamps[i], settings))

                    loaded = batch_end
                    elapsed = time.time() - start
                    rate = loaded / elapsed if elapsed > 0 else 0
                    print(
                        f"  users: {loaded:,}/{total:,} ({100*loaded/total:.1f}%) - {rate:,.0f} rows/s"
                    )

        conn.commit()

    elapsed = time.time() - start
    return {"table": "users", "rows": loaded, "elapsed": elapsed}


def load_orders(conninfo: str, total: int, batch_size: int = 100_000) -> dict:
    """Load orders table using COPY."""
    start = time.time()
    loaded = 0

    with psycopg.connect(conninfo) as conn:
        with conn.cursor() as cur:
            with cur.copy(
                "COPY copy_data.orders (id, user_id, tenant_id, amount, created_at, refunded_at, notes) FROM STDIN"
            ) as copy:
                for batch_start in range(0, total, batch_size):
                    batch_end = min(batch_start + batch_size, total)
                    batch_count = batch_end - batch_start

                    user_ids = np.random.randint(1, 5_000_001, batch_count)
                    tenant_ids = np.random.randint(1, 1001, batch_count)
                    amounts = np.round(10 + np.random.random(batch_count) * 990, 2)
                    timestamps = random_timestamps(batch_count)
                    refund_flags = np.random.random(batch_count) < 0.05
                    refund_times = random_timestamps(batch_count, 365)
                    notes = random_texts_numpy(batch_count, 50)

                    for i, idx in enumerate(range(batch_start + 1, batch_end + 1)):
                        refunded_at = refund_times[i] if refund_flags[i] else None
                        copy.write_row(
                            (
                                idx,
                                int(user_ids[i]),
                                int(tenant_ids[i]),
                                float(amounts[i]),
                                timestamps[i],
                                refunded_at,
                                notes[i],
                            )
                        )

                    loaded = batch_end
                    elapsed = time.time() - start
                    rate = loaded / elapsed if elapsed > 0 else 0
                    print(
                        f"  orders: {loaded:,}/{total:,} ({100*loaded/total:.1f}%) - {rate:,.0f} rows/s"
                    )

        conn.commit()

    elapsed = time.time() - start
    return {"table": "orders", "rows": loaded, "elapsed": elapsed}


def load_order_items(
    conninfo: str, total: int, max_order_id: int, batch_size: int = 100_000
) -> dict:
    """Load order_items table using COPY."""
    start = time.time()
    loaded = 0

    with psycopg.connect(conninfo) as conn:
        with conn.cursor() as cur:
            with cur.copy(
                "COPY copy_data.order_items (id, user_id, tenant_id, order_id, product_name, amount, quantity, created_at, refunded_at) FROM STDIN"
            ) as copy:
                for batch_start in range(0, total, batch_size):
                    batch_end = min(batch_start + batch_size, total)
                    batch_count = batch_end - batch_start

                    user_ids = np.random.randint(1, 5_000_001, batch_count)
                    tenant_ids = np.random.randint(1, 1001, batch_count)
                    order_ids = np.random.randint(1, max_order_id + 1, batch_count)
                    amounts = np.round(5 + np.random.random(batch_count) * 195, 2)
                    quantities = np.random.randint(1, 6, batch_count)
                    timestamps = random_timestamps(batch_count)
                    refund_flags = np.random.random(batch_count) < 0.05
                    refund_times = random_timestamps(batch_count, 365)
                    product_names = random_texts_numpy(batch_count, 30)

                    for i, idx in enumerate(range(batch_start + 1, batch_end + 1)):
                        refunded_at = refund_times[i] if refund_flags[i] else None
                        copy.write_row(
                            (
                                idx,
                                int(user_ids[i]),
                                int(tenant_ids[i]),
                                int(order_ids[i]),
                                f"Product {product_names[i]}",
                                float(amounts[i]),
                                int(quantities[i]),
                                timestamps[i],
                                refunded_at,
                            )
                        )

                    loaded = batch_end
                    elapsed = time.time() - start
                    rate = loaded / elapsed if elapsed > 0 else 0
                    print(
                        f"  order_items: {loaded:,}/{total:,} ({100*loaded/total:.1f}%) - {rate:,.0f} rows/s"
                    )

        conn.commit()

    elapsed = time.time() - start
    return {"table": "order_items", "rows": loaded, "elapsed": elapsed}


def load_log_actions(conninfo: str, total: int, batch_size: int = 500_000) -> dict:
    """Load log_actions table using COPY."""
    start = time.time()
    loaded = 0

    actions = [
        "login",
        "logout",
        "click",
        "purchase",
        "view",
        "error",
        "search",
        "update",
        "delete",
        "create",
    ]

    with psycopg.connect(conninfo) as conn:
        with conn.cursor() as cur:
            with cur.copy(
                "COPY copy_data.log_actions (id, tenant_id, user_id, action, details, ip_address, user_agent, created_at) FROM STDIN"
            ) as copy:
                for batch_start in range(0, total, batch_size):
                    batch_end = min(batch_start + batch_size, total)
                    batch_count = batch_end - batch_start

                    tenant_null_flags = np.random.random(batch_count) < 0.1
                    tenant_ids = np.random.randint(1, 1001, batch_count)
                    user_ids = np.random.randint(1, 5_000_001, batch_count)
                    action_indices = np.random.randint(0, len(actions), batch_count)
                    timestamps = random_timestamps(batch_count)
                    details = random_texts_numpy(batch_count, 50)
                    user_agents = random_texts_numpy(batch_count, 30)
                    ip_parts = np.random.randint(0, 256, (batch_count, 4))

                    for i, idx in enumerate(range(batch_start + 1, batch_end + 1)):
                        tenant_id = None if tenant_null_flags[i] else int(tenant_ids[i])
                        ip = f"{ip_parts[i,0]}.{ip_parts[i,1]}.{ip_parts[i,2]}.{ip_parts[i,3]}"
                        copy.write_row(
                            (
                                idx,
                                tenant_id,
                                int(user_ids[i]),
                                actions[action_indices[i]],
                                details[i],
                                ip,
                                f"Mozilla/5.0 {user_agents[i]}",
                                timestamps[i],
                            )
                        )

                    loaded = batch_end
                    elapsed = time.time() - start
                    rate = loaded / elapsed if elapsed > 0 else 0
                    print(
                        f"  log_actions: {loaded:,}/{total:,} ({100*loaded/total:.1f}%) - {rate:,.0f} rows/s"
                    )

        conn.commit()

    elapsed = time.time() - start
    return {"table": "log_actions", "rows": loaded, "elapsed": elapsed}


def load_with_identity(conninfo: str, total: int, batch_size: int = 100_000) -> dict:
    """Load with_identity table using COPY."""
    start = time.time()
    loaded = 0

    with psycopg.connect(conninfo) as conn:
        with conn.cursor() as cur:
            # For GENERATED ALWAYS AS IDENTITY, we use DEFAULT
            with cur.copy(
                "COPY copy_data.with_identity (tenant_id, data) FROM STDIN"
            ) as copy:
                for batch_start in range(0, total, batch_size):
                    batch_end = min(batch_start + batch_size, total)
                    batch_count = batch_end - batch_start

                    tenant_ids = np.random.randint(1, 1001, batch_count)
                    data = random_texts_numpy(batch_count, 20)

                    for i in range(batch_count):
                        copy.write_row((int(tenant_ids[i]), data[i]))

                    loaded = batch_end
                    elapsed = time.time() - start
                    rate = loaded / elapsed if elapsed > 0 else 0
                    print(
                        f"  with_identity: {loaded:,}/{total:,} ({100*loaded/total:.1f}%) - {rate:,.0f} rows/s"
                    )

        conn.commit()

    elapsed = time.time() - start
    return {"table": "with_identity", "rows": loaded, "elapsed": elapsed}


def create_indexes(conninfo: str):
    """Create indexes after data load."""
    print("Creating indexes...")
    start = time.time()

    with psycopg.connect(conninfo) as conn:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_orders_user_tenant ON copy_data.orders(user_id, tenant_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_order_items_order ON copy_data.order_items(order_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_log_actions_tenant ON copy_data.log_actions(tenant_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_log_actions_created ON copy_data.log_actions(created_at)"
        )
        conn.commit()

    print(f"Indexes created in {format_duration(time.time() - start)}")


def analyze_tables(conninfo: str):
    """Analyze tables for query planner."""
    print("Analyzing tables...")
    start = time.time()

    with psycopg.connect(conninfo) as conn:
        conn.execute("ANALYZE copy_data.users")
        conn.execute("ANALYZE copy_data.orders")
        conn.execute("ANALYZE copy_data.order_items")
        conn.execute("ANALYZE copy_data.log_actions")
        conn.execute("ANALYZE copy_data.with_identity")
        conn.commit()

    print(f"Analyze completed in {format_duration(time.time() - start)}")


def show_table_sizes(conninfo: str):
    """Show final table sizes."""
    with psycopg.connect(conninfo) as conn:
        result = conn.execute("""
            SELECT
                tablename,
                pg_size_pretty(pg_total_relation_size('copy_data.' || tablename)) as total_size,
                pg_size_pretty(pg_relation_size('copy_data.' || tablename)) as table_size
            FROM pg_tables
            WHERE schemaname = 'copy_data'
            ORDER BY pg_total_relation_size('copy_data.' || tablename) DESC
        """).fetchall()

        print("\nTable sizes:")
        print("-" * 50)
        for row in result:
            print(f"  {row[0]:20} {row[1]:>12} (data: {row[2]})")


def create_publication(conninfo: str):
    """Create publication for replication."""
    with psycopg.connect(conninfo) as conn:
        conn.execute("DROP PUBLICATION IF EXISTS pgdog")
        conn.execute("CREATE PUBLICATION pgdog FOR TABLES IN SCHEMA copy_data")
        conn.commit()
    print("Publication 'pgdog' created")


def main():
    parser = argparse.ArgumentParser(
        description="Fast data loader for copy_data schema"
    )
    parser.add_argument(
        "--conninfo",
        default="host=localhost dbname=pgdog user=pgdog password=pgdog",
        help="PostgreSQL connection string",
    )
    parser.add_argument(
        "--scale",
        type=float,
        default=1.0,
        help="Scale factor (1.0 = ~50GB, 0.1 = ~5GB, 2.0 = ~100GB)",
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=4,
        help="Number of parallel loaders (default: 4)",
    )
    parser.add_argument(
        "--skip-schema", action="store_true", help="Skip schema creation"
    )
    parser.add_argument(
        "--skip-indexes", action="store_true", help="Skip index creation"
    )

    args = parser.parse_args()

    # Calculate row counts based on scale
    scale = args.scale
    users_count = int(5_000_000 * scale)
    orders_count = int(20_000_000 * scale)
    order_items_count = int(60_000_000 * scale)
    log_actions_count = int(300_000_000 * scale)
    with_identity_count = int(50_000_000 * scale)

    print(f"Data loader configuration:")
    print(f"  Connection: {args.conninfo}")
    print(f"  Scale: {scale}x")
    print(f"  Parallel loaders: {args.parallel}")
    print(f"  Users: {users_count:,}")
    print(f"  Orders: {orders_count:,}")
    print(f"  Order items: {order_items_count:,}")
    print(f"  Log actions: {log_actions_count:,}")
    print(f"  With identity: {with_identity_count:,}")
    print()

    total_start = time.time()

    # Create schema
    if not args.skip_schema:
        create_schema(args.conninfo)

    # Load tables in parallel
    print("\nLoading data...")
    results = []

    # Users and orders can run in parallel
    # Order items depends on orders (for valid order_ids)
    # Log actions and with_identity are independent

    with ProcessPoolExecutor(max_workers=args.parallel) as executor:
        futures = {}

        # Phase 1: users and orders in parallel
        futures[executor.submit(load_users, args.conninfo, users_count)] = "users"
        futures[executor.submit(load_orders, args.conninfo, orders_count)] = "orders"

        # Wait for orders to complete before starting order_items
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            print(
                f"  {result['table']} completed: {result['rows']:,} rows in {format_duration(result['elapsed'])}"
            )

        # Phase 2: order_items, log_actions, with_identity in parallel
        futures = {}
        futures[
            executor.submit(
                load_order_items, args.conninfo, order_items_count, orders_count
            )
        ] = "order_items"
        futures[
            executor.submit(load_log_actions, args.conninfo, log_actions_count)
        ] = "log_actions"
        futures[
            executor.submit(load_with_identity, args.conninfo, with_identity_count)
        ] = "with_identity"

        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            print(
                f"  {result['table']} completed: {result['rows']:,} rows in {format_duration(result['elapsed'])}"
            )

    # Create indexes
    if not args.skip_indexes:
        create_indexes(args.conninfo)

    # Analyze
    analyze_tables(args.conninfo)

    # Show sizes
    show_table_sizes(args.conninfo)

    # Create publication
    create_publication(args.conninfo)

    total_elapsed = time.time() - total_start
    total_rows = sum(r["rows"] for r in results)
    print(f"\nTotal: {total_rows:,} rows loaded in {format_duration(total_elapsed)}")
    print(f"Average rate: {total_rows / total_elapsed:,.0f} rows/s")


if __name__ == "__main__":
    main()
