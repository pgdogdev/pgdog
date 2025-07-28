"""
Test suite demonstrating numeric sorting anomalies in pgdog's multi-shard environment.

These tests show how numeric values are incorrectly sorted across shards,
similar to the timestamp sorting issue that was previously fixed.
"""

import asyncpg
import pytest
from decimal import Decimal
from globals import normal_async, sharded_async, no_out_of_sync
import pytest_asyncio


@pytest.mark.asyncio
async def test_numeric_shard_order_not_value_order():
    """
    Demonstrates that results come back in shard order, not value order.
    """
    conn = await sharded_async()
    
    try:
        await conn.execute("DROP TABLE IF EXISTS numeric_sort_test")
        await conn.execute("""
            CREATE TABLE numeric_sort_test (
                id BIGINT PRIMARY KEY,
                value NUMERIC NOT NULL
            )
        """)
        
        # Insert values where shard order != value order
        # Shard 0: IDs 1-10, Shard 1: IDs 101-110
        test_data = [
            # Shard 0 - larger values
            (1, Decimal("900.00")),
            (2, Decimal("800.00")),
            (3, Decimal("700.00")),
            # Shard 1 - smaller values  
            (101, Decimal("300.00")),
            (102, Decimal("200.00")),
            (103, Decimal("100.00")),
        ]
        
        for id_val, value in test_data:
            await conn.execute(
                "INSERT INTO numeric_sort_test (id, value) VALUES ($1, $2)",
                id_val, value
            )
        
        # Query with ORDER BY
        rows = await conn.fetch(
            "SELECT id, value FROM numeric_sort_test ORDER BY value DESC"
        )
        
        actual_values = [row['value'] for row in rows]
        expected_values = [
            Decimal("900.00"),
            Decimal("800.00"), 
            Decimal("700.00"),
            Decimal("300.00"),
            Decimal("200.00"),
            Decimal("100.00"),
        ]
        
        print(f"\nShard 0 values: 900, 800, 700")
        print(f"Shard 1 values: 300, 200, 100")
        print(f"Actual order: {actual_values}")
        print(f"Expected order: {expected_values}")
        
        # The bug has been fixed! Results are now correctly sorted
        assert actual_values == expected_values, "Results are correctly sorted across shards!"
        
        # Verify shard ordering
        shard0_results = actual_values[:3]
        shard1_results = actual_values[3:]
        
        # Within each shard, results should be sorted
        assert shard0_results == [Decimal("900.00"), Decimal("800.00"), Decimal("700.00")]
        assert shard1_results == [Decimal("300.00"), Decimal("200.00"), Decimal("100.00")]
        
    finally:
        await conn.execute("DROP TABLE IF EXISTS numeric_sort_test")
        await conn.close()


@pytest.mark.asyncio
async def test_numeric_precision_sorting_anomaly():
    """
    Tests sorting of high-precision decimals that would lose precision as floats.
    """
    conn = await sharded_async()
    
    try:
        await conn.execute("DROP TABLE IF EXISTS precision_sort_test")
        await conn.execute("""
            CREATE TABLE precision_sort_test (
                id BIGINT PRIMARY KEY,
                precise_value NUMERIC NOT NULL
            )
        """)
        
        # Values that are very close together
        test_data = [
            # Shard 0
            (1, Decimal("1.00000000000000001")),
            (2, Decimal("1.00000000000000003")),
            # Shard 1
            (101, Decimal("1.00000000000000002")),
            (102, Decimal("1.00000000000000004")),
        ]
        
        for id_val, value in test_data:
            await conn.execute(
                "INSERT INTO precision_sort_test (id, precise_value) VALUES ($1, $2)",
                id_val, value
            )
        
        rows = await conn.fetch(
            "SELECT id, precise_value FROM precision_sort_test ORDER BY precise_value"
        )
        
        actual_ids = [row['id'] for row in rows]
        expected_ids = [1, 101, 2, 102]  # Correct interleaved order
        
        print(f"\nHigh precision sorting:")
        print(f"Actual ID order: {actual_ids}")
        print(f"Expected ID order: {expected_ids}")
        
        # Bug fixed! High precision values are now sorted correctly
        assert actual_ids == expected_ids, "High precision values sorted correctly!"
        
    finally:
        await conn.execute("DROP TABLE IF EXISTS precision_sort_test")
        await conn.close()


@pytest.mark.asyncio
async def test_negative_positive_numeric_sorting():
    """
    Tests sorting across negative and positive numeric values.
    """
    conn = await sharded_async()
    
    try:
        await conn.execute("DROP TABLE IF EXISTS negative_sort_test")
        await conn.execute("""
            CREATE TABLE negative_sort_test (
                id BIGINT PRIMARY KEY,
                amount NUMERIC NOT NULL
            )
        """)
        
        # Mix of negative and positive across shards
        test_data = [
            # Shard 0 - negative values
            (1, Decimal("-100.50")),
            (2, Decimal("-50.25")),
            (3, Decimal("-10.00")),
            # Shard 1 - positive values
            (101, Decimal("10.00")),
            (102, Decimal("50.25")), 
            (103, Decimal("100.50")),
        ]
        
        for id_val, amount in test_data:
            await conn.execute(
                "INSERT INTO negative_sort_test (id, amount) VALUES ($1, $2)",
                id_val, amount
            )
        
        # Test ascending order
        rows_asc = await conn.fetch(
            "SELECT id, amount FROM negative_sort_test ORDER BY amount ASC"
        )
        
        actual_asc = [row['amount'] for row in rows_asc]
        expected_asc = [
            Decimal("-100.50"),
            Decimal("-50.25"),
            Decimal("-10.00"),
            Decimal("10.00"),
            Decimal("50.25"),
            Decimal("100.50"),
        ]
        
        print(f"\nNegative/Positive sorting (ASC):")
        print(f"Actual: {actual_asc}")
        print(f"Expected: {expected_asc}")
        
        # Bug fixed! Negative and positive values are now properly sorted
        assert actual_asc == expected_asc, "Negative/positive sorting works correctly!"
        
    finally:
        await conn.execute("DROP TABLE IF EXISTS negative_sort_test")
        await conn.close()


@pytest.mark.asyncio
async def test_decimal_arithmetic_precision_sorting():
    """
    Tests sorting of decimal arithmetic results (e.g., 0.1 + 0.2).
    This would fail with float but should work with Decimal.
    """
    conn = await sharded_async()
    
    try:
        await conn.execute("DROP TABLE IF EXISTS arithmetic_sort_test")
        await conn.execute("""
            CREATE TABLE arithmetic_sort_test (
                id BIGINT PRIMARY KEY,
                a NUMERIC,
                b NUMERIC
            )
        """)
        
        # Insert values that cause float precision issues
        test_data = [
            # Shard 0
            (1, Decimal("0.1"), Decimal("0.2")),    # 0.1 + 0.2 = 0.3
            (2, Decimal("0.3"), Decimal("0.0")),    # 0.3 + 0.0 = 0.3
            # Shard 1  
            (101, Decimal("0.15"), Decimal("0.15")), # 0.15 + 0.15 = 0.3
            (102, Decimal("0.2"), Decimal("0.1")),   # 0.2 + 0.1 = 0.3
        ]
        
        for id_val, a, b in test_data:
            await conn.execute(
                "INSERT INTO arithmetic_sort_test (id, a, b) VALUES ($1, $2, $3)",
                id_val, a, b
            )
        
        # Sort by computed sum
        rows = await conn.fetch("""
            SELECT id, a, b, (a + b) as sum 
            FROM arithmetic_sort_test 
            ORDER BY (a + b), id
        """)
        
        # All sums should be exactly 0.3
        sums = [row['sum'] for row in rows]
        assert all(s == Decimal("0.3") for s in sums), "Arithmetic precision lost!"
        
        # But due to shard ordering, IDs won't be in expected order
        actual_ids = [row['id'] for row in rows]
        expected_ids = [1, 2, 101, 102]  # All have same sum, so order by id
        
        print(f"\nArithmetic precision sorting:")
        print(f"All sums: {sums}")
        print(f"Actual ID order: {actual_ids}")
        print(f"Expected ID order: {expected_ids}")
        
        # Bug fixed! Equal sums are now sorted correctly by ID
        assert actual_ids == expected_ids, "Equal sums sorted correctly by ID!"
        
    finally:
        await conn.execute("DROP TABLE IF EXISTS arithmetic_sort_test")
        await conn.close()


@pytest.mark.asyncio
async def test_null_handling_in_numeric_sort():
    """
    Tests how NULL values are handled in cross-shard numeric sorting.
    """
    conn = await sharded_async()
    
    try:
        await conn.execute("DROP TABLE IF EXISTS null_sort_test")
        await conn.execute("""
            CREATE TABLE null_sort_test (
                id BIGINT PRIMARY KEY,
                optional_value NUMERIC
            )
        """)
        
        # Mix of NULL and non-NULL values
        test_data = [
            # Shard 0
            (1, None),
            (2, Decimal("100.00")),
            (3, None),
            # Shard 1
            (101, Decimal("50.00")),
            (102, None),
            (103, Decimal("150.00")),
        ]
        
        for id_val, value in test_data:
            await conn.execute(
                "INSERT INTO null_sort_test (id, optional_value) VALUES ($1, $2)",
                id_val, value
            )
        
        # PostgreSQL sorts NULLs last by default in ASC order
        rows = await conn.fetch(
            "SELECT id, optional_value FROM null_sort_test ORDER BY optional_value ASC"
        )
        
        actual_values = [(row['id'], row['optional_value']) for row in rows]
        
        print(f"\nNULL sorting test:")
        for id_val, val in actual_values:
            print(f"  ID {id_val}: {val}")
        
        # Check that non-NULL values aren't properly sorted across shards
        non_null_values = [v for _, v in actual_values if v is not None]
        expected_non_null = [Decimal("50.00"), Decimal("100.00"), Decimal("150.00")]
        
        # Check that NULL sorting now works correctly
        assert non_null_values == expected_non_null, "Cross-shard NULL sorting works correctly!"
        print("✓ Bug fixed! NULLs and values sorted correctly!")
            
    finally:
        await conn.execute("DROP TABLE IF EXISTS null_sort_test")
        await conn.close()


if __name__ == "__main__":
    import asyncio
    
    print("=== Numeric Sorting Anomaly Tests ===")
    print("These tests demonstrate cross-shard sorting issues with NUMERIC values")
    print("Similar to the timestamp sorting bug that was fixed\n")
    
    # Run each test
    asyncio.run(test_numeric_shard_order_not_value_order())
    asyncio.run(test_numeric_precision_sorting_anomaly())
    asyncio.run(test_negative_positive_numeric_sorting())
    asyncio.run(test_decimal_arithmetic_precision_sorting())
    asyncio.run(test_null_handling_in_numeric_sort())