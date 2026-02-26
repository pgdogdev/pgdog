#!/usr/bin/env python3
"""
Stress test for Postgres prepared statements using psycopg3.
Tests 1000+ unique prepared statements across 200+ client connections.
"""

import time
import random
import concurrent.futures
import threading
import os
from typing import List, Tuple
import psycopg
from psycopg import sql

CONNECTION_STRING = "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog"
NUM_CONNECTIONS = 200
NUM_PREPARED_STATEMENTS = 1000
OPERATIONS_PER_CONNECTION = 1000
DEBUG = False  # Set to True to see query details
CHAOS_ERROR_RATE = 0.05  # 5% chance to close connection during operations (0 to disable)


def generate_prepared_statements() -> List[Tuple[str, str, tuple]]:
    """Generate 1000+ unique prepared statements with varying complexity."""
    statements = []

    # Simple SELECT with single parameter
    for i in range(250):
        statements.append((
            f"simple_select_{i}",
            f"SELECT {i} + %s AS result",
            (random.randint(1, 100),)
        ))

    # SELECT with multiple parameters
    for i in range(250):
        statements.append((
            f"multi_param_select_{i}",
            f"SELECT %s + %s + {i} AS result",
            (random.randint(1, 50), random.randint(1, 50))
        ))

    # SELECT with WHERE clause
    for i in range(250):
        val = random.randint(0, 500)
        statements.append((
            f"where_select_{i}",
            f"SELECT %s::int AS value WHERE %s::int > {i}",
            (val, val)  # Same value used twice
        ))

    # SELECT with CASE expressions
    for i in range(150):
        val = random.randint(0, 300)
        statements.append((
            f"case_select_{i}",
            f"SELECT CASE WHEN %s::int > {i} THEN %s::int ELSE {i} END AS result",
            (val, val)  # Same value used twice
        ))

    # SELECT with string parameters
    for i in range(100):
        statements.append((
            f"string_select_{i}",
            f"SELECT %s::text || ' - {i}' AS result",
            (f"test_{random.randint(1, 1000)}",)
        ))

    return statements


def setup_test_table(conn_string: str):
    """Create a test table if needed."""
    with psycopg.connect(conn_string) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS bench_test")
            cur.execute("""
                CREATE TABLE bench_test (
                    id SERIAL PRIMARY KEY,
                    value INTEGER,
                    data TEXT
                )
            """)
            # Insert some test data
            for i in range(100):
                cur.execute(
                    "INSERT INTO bench_test (value, data) VALUES (%s, %s)",
                    (i, f"data_{i}")
                )
            conn.commit()


def worker(worker_id: int, statements: List[Tuple[str, str, tuple]]) -> dict:
    """Worker function that creates a connection and executes prepared statements."""
    results = {
        'worker_id': worker_id,
        'prepared_count': 0,
        'executed_count': 0,
        'errors': 0,
        'reconnections': 0,
        'chaos_events': 0,
        'duration': 0
    }

    start_time = time.time()

    # Build a dict of statements for lookup
    stmt_dict = {name: (query, params) for name, query, params in statements}
    results['prepared_count'] = len(stmt_dict)

    conn = None
    operations_completed = 0

    while operations_completed < OPERATIONS_PER_CONNECTION:
        try:
            # Connect or reconnect
            if conn is None or conn.closed:
                if conn is not None:
                    results['reconnections'] += 1
                conn = psycopg.connect(CONNECTION_STRING, prepare_threshold=0)

            # Pick random prepared statement
            name = random.choice(list(stmt_dict.keys()))
            query, params = stmt_dict[name]

            try:
                # Chaos testing: decide if and when to kill connection
                chaos_point = None
                kill_timer = None

                if CHAOS_ERROR_RATE > 0 and random.random() < CHAOS_ERROR_RATE:
                    results['chaos_events'] += 1
                    # Choose when to kill: mid_query, after_execute, or before_commit
                    chaos_point = random.choice(['mid_query', 'after_execute', 'before_commit'])

                    # Check if we can access the socket
                    try:
                        socket_fd = conn.pgconn.socket
                        print(f"Worker {worker_id}: Socket found: fd={socket_fd}, chaos_point={chaos_point}")

                        # For mid-query chaos, schedule socket closure during execution
                        if chaos_point == 'mid_query':
                            def kill_socket():
                                print(f"Worker {worker_id}: Chaos - closing socket mid-query")
                                try:
                                    os.close(socket_fd)
                                except Exception as e:
                                    print(f"Worker {worker_id}: Error closing socket: {e}")

                            kill_timer = threading.Timer(0.001, kill_socket)
                            kill_timer.start()

                    except Exception as e:
                        print(f"Worker {worker_id}: Could not access socket: {e}")

                with conn.cursor() as cur:
                    cur.execute(query, params)

                    # Chaos: kill connection right after execute, before fetch
                    if chaos_point == 'after_execute':
                        if kill_timer:
                            kill_timer.cancel()
                        if DEBUG:
                            print(f"Worker {worker_id}: Chaos - closing after execute")
                        conn.close()
                        conn = None
                        continue

                    _ = cur.fetchall()

                # Cancel kill timer if query completed normally
                if kill_timer:
                    kill_timer.cancel()

                # Chaos: kill connection before commit
                if chaos_point == 'before_commit':
                    if DEBUG:
                        print(f"Worker {worker_id}: Chaos - closing before commit")
                    conn.close()
                    conn = None
                    continue

                conn.commit()
                results['executed_count'] += 1
                operations_completed += 1

            except (psycopg.OperationalError, psycopg.InterfaceError) as e:
                # Connection error - need to reconnect
                if kill_timer:
                    kill_timer.cancel()
                results['errors'] += 1
                if DEBUG:
                    print(f"Worker {worker_id}: Connection error executing {name}: {e}")
                if conn:
                    conn.close()
                conn = None
                # Don't increment operations_completed, will retry

            except Exception as e:
                # Other errors
                if kill_timer:
                    kill_timer.cancel()
                results['errors'] += 1
                print(f"Worker {worker_id}: Error executing {name}: {e}")
                operations_completed += 1  # Count as attempted

        except Exception as e:
            # Connection establishment error
            results['errors'] += 1
            if DEBUG:
                print(f"Worker {worker_id}: Connection establishment error: {e}")
            conn = None
            time.sleep(0.01)  # Brief backoff before retry

    # Clean up
    if conn and not conn.closed:
        conn.close()

    results['duration'] = time.time() - start_time
    return results


def run_benchmark():
    """Run the stress test."""
    print(f"Generating {NUM_PREPARED_STATEMENTS} unique prepared statements...")
    all_statements = generate_prepared_statements()
    print(f"Generated {len(all_statements)} prepared statements")

    print(f"\nSetting up test table...")
    try:
        setup_test_table(CONNECTION_STRING)
        print("Test table created successfully")
    except Exception as e:
        print(f"Warning: Could not create test table: {e}")

    print(f"\nStarting stress test with {NUM_CONNECTIONS} connections...")
    print(f"Each connection will execute {OPERATIONS_PER_CONNECTION} operations")
    if CHAOS_ERROR_RATE > 0:
        print(f"Chaos mode enabled: {CHAOS_ERROR_RATE * 100:.1f}% chance of random disconnection")
    print("=" * 70)

    start_time = time.time()

    # Run workers in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_CONNECTIONS) as executor:
        # Each worker gets a random subset of statements
        futures = []
        for i in range(NUM_CONNECTIONS):
            # Give each worker a random selection of statements
            worker_statements = random.sample(
                all_statements,
                min(100, len(all_statements))  # Each worker gets up to 100 statements
            )
            futures.append(executor.submit(worker, i, worker_statements))

        # Collect results
        results = []
        completed = 0
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                results.append(result)
                completed += 1
                if completed % 20 == 0:
                    print(f"Progress: {completed}/{NUM_CONNECTIONS} connections completed")
            except Exception as e:
                print(f"Worker failed with exception: {e}")

    total_duration = time.time() - start_time

    # Calculate statistics
    total_prepared = sum(r['prepared_count'] for r in results)
    total_executed = sum(r['executed_count'] for r in results)
    total_errors = sum(r['errors'] for r in results)
    total_reconnections = sum(r['reconnections'] for r in results)
    total_chaos_events = sum(r['chaos_events'] for r in results)
    avg_worker_duration = sum(r['duration'] for r in results) / len(results)

    print("\n" + "=" * 70)
    print("BENCHMARK RESULTS")
    print("=" * 70)
    print(f"Total connections:              {NUM_CONNECTIONS}")
    print(f"Unique prepared statements:     {len(all_statements)}")
    print(f"Total statements prepared:      {total_prepared}")
    print(f"Total operations executed:      {total_executed}")
    print(f"Total errors:                   {total_errors}")
    if CHAOS_ERROR_RATE > 0:
        print(f"Chaos events (forced disconn):  {total_chaos_events}")
        print(f"Total reconnections:            {total_reconnections}")
    print(f"Total wall-clock time:          {total_duration:.2f} seconds")
    print(f"Average worker duration:        {avg_worker_duration:.2f} seconds")
    print(f"Operations per second:          {total_executed / total_duration:.2f}")
    print(f"Success rate:                   {100 * (1 - total_errors / max(1, total_executed)):.2f}%")
    print("=" * 70)

    # Clean up test table
    try:
        with psycopg.connect(CONNECTION_STRING) as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS bench_test")
                conn.commit()
    except:
        pass


if __name__ == "__main__":
    run_benchmark()
