"""
Test suite for numeric type support in pgdog's multi-shard environment.
"""

import asyncpg
import pytest
from decimal import Decimal
from globals import normal_async, sharded_async


async def setup_sharded_numeric_table(conn):
    """Helper function to create the sharded_numeric table."""
    await conn.execute("DROP TABLE IF EXISTS sharded_numeric")
    await conn.execute("""
        CREATE TABLE sharded_numeric (
            id BIGINT PRIMARY KEY,
            value NUMERIC
        )
    """)


@pytest.mark.asyncio
async def test_numeric_cross_shard_sorting():
    """Test that numeric values are correctly sorted across shards."""
    conn = await sharded_async()
    
    try:
        await setup_sharded_numeric_table(conn)
        
        # Test various numeric values across shards
        test_data = [
            # Shard 0 - mix of values
            (1, Decimal("-999.99")),
            (2, Decimal("0")),
            (3, Decimal("500.50")),
            # Shard 1 - mix of values that should interleave
            (101, Decimal("-100.00")),
            (102, Decimal("250.25")), 
            (103, Decimal("999.99")),
        ]
        
        for id_val, value in test_data:
            await conn.execute(
                "INSERT INTO sharded_numeric (id, value) VALUES ($1, $2)",
                id_val, value
            )
        
        # Test ascending order
        rows_asc = await conn.fetch(
            "SELECT value FROM sharded_numeric ORDER BY value ASC"
        )
        
        actual_asc = [row['value'] for row in rows_asc]
        expected_asc = [
            Decimal("-999.99"),
            Decimal("-100.00"),
            Decimal("0"),
            Decimal("250.25"),
            Decimal("500.50"),
            Decimal("999.99"),
        ]
        
        assert actual_asc == expected_asc
        
        # Test descending order
        rows_desc = await conn.fetch(
            "SELECT value FROM sharded_numeric ORDER BY value DESC"
        )
        
        actual_desc = [row['value'] for row in rows_desc]
        assert actual_desc == list(reversed(expected_asc))
        
    finally:
        await conn.execute("TRUNCATE TABLE sharded_numeric")
        await conn.close()


@pytest.mark.asyncio
async def test_numeric_precision_and_edge_cases():
    """Test high precision decimals and edge cases."""
    conn = await sharded_async()
    
    try:
        await setup_sharded_numeric_table(conn)
        
        # Test precision preservation and edge cases
        test_data = [
            # High precision values
            (1, Decimal("123456789.123456789")),
            (2, Decimal("0.000000000000001")),
            # Edge cases
            (3, Decimal("0")),
            (4, None),  # NULL
            # Values from different shard
            (101, Decimal("-123456789.123456789")),
            (102, Decimal("999999999999.999999")),
        ]
        
        for id_val, value in test_data:
            await conn.execute(
                "INSERT INTO sharded_numeric (id, value) VALUES ($1, $2)",
                id_val, value
            )
        
        # Verify precision is preserved
        row = await conn.fetchrow(
            "SELECT value FROM sharded_numeric WHERE id = 1"
        )
        assert row['value'] == Decimal("123456789.123456789")
        
        # Test sorting with NULLs (should be last in ASC order)
        rows = await conn.fetch(
            "SELECT id, value FROM sharded_numeric ORDER BY value ASC"
        )
        
        # Check NULL is last
        assert rows[-1]['value'] is None
        
        # Check other values are correctly sorted
        non_null_values = [row['value'] for row in rows if row['value'] is not None]
        assert non_null_values == [
            Decimal("-123456789.123456789"),
            Decimal("0"),
            Decimal("0.000000000000001"),
            Decimal("123456789.123456789"),
            Decimal("999999999999.999999"),
        ]
        
    finally:
        await conn.execute("TRUNCATE TABLE sharded_numeric")
        await conn.close()


@pytest.mark.asyncio
async def test_numeric_arithmetic_operations():
    """Test arithmetic operations preserve precision."""
    conn = await normal_async()
    
    try:
        await conn.execute("DROP TABLE IF EXISTS numeric_math")
        await conn.execute("""
            CREATE TABLE numeric_math (
                id BIGINT PRIMARY KEY,
                a NUMERIC,
                b NUMERIC
            )
        """)
        
        # Insert values that demonstrate precision
        await conn.execute(
            "INSERT INTO numeric_math (id, a, b) VALUES ($1, $2, $3)",
            1, Decimal("0.1"), Decimal("0.2")
        )
        
        # Test that 0.1 + 0.2 = 0.3 exactly (not 0.30000000000000004)
        result = await conn.fetchval(
            "SELECT a + b FROM numeric_math WHERE id = 1"
        )
        assert result == Decimal("0.3")
        
        # Test multiplication precision
        await conn.execute(
            "INSERT INTO numeric_math (id, a, b) VALUES ($1, $2, $3)",
            2, Decimal("19.99"), Decimal("3.5")
        )
        
        result = await conn.fetchval(
            "SELECT a * b FROM numeric_math WHERE id = 2"
        )
        assert result == Decimal("69.965")
        
    finally:
        await conn.execute("DROP TABLE IF EXISTS numeric_math")
        await conn.close()