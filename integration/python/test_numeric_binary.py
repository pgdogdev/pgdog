import asyncpg
import pytest
from decimal import Decimal
from globals import normal_async, sharded_async, no_out_of_sync
import pytest_asyncio


@pytest.mark.asyncio
async def test_numeric_binary_format():
    """Test numeric types with binary format through asyncpg."""
    conn = await normal_async()
    
    try:
        await conn.execute("DROP TABLE IF EXISTS numeric_test CASCADE")
        
        await conn.execute("""
            CREATE TABLE numeric_test (
                id INTEGER PRIMARY KEY,
                num_val NUMERIC,
                float_val REAL,
                double_val DOUBLE PRECISION
            )
        """)
        
        # Test cases covering various numeric scenarios
        test_data = [
            # Basic values
            (1, Decimal("123.456"), 123.456, 123.456),
            (2, Decimal("0"), 0.0, 0.0),
            (3, Decimal("-999.99"), -999.99, -999.99),
            
            # Edge cases for decimal
            (4, Decimal("0.0001"), 0.0001, 0.0001),
            (5, Decimal("10000"), 10000.0, 10000.0),
            (6, Decimal("0.3"), 0.3, 0.3),  # Classic floating point issue
            
            # Large values
            (7, Decimal("999999999999999999"), 1e18, 1e18),
            
            # Precision tests
            (8, Decimal("0.1") + Decimal("0.2"), 0.1 + 0.2, 0.1 + 0.2),  # Should be exactly 0.3 for Decimal
            
            # Special float values
            (9, Decimal("123"), float('inf'), float('inf')),
            (10, Decimal("456"), float('-inf'), float('-inf')),
            (11, Decimal("789"), float('nan'), float('nan')),
        ]
        
        # Insert data
        for row in test_data:
            await conn.execute(
                "INSERT INTO numeric_test (id, num_val, float_val, double_val) VALUES ($1, $2, $3, $4)",
                *row
            )
        
        # Fetch and verify - asyncpg uses binary protocol by default
        rows = await conn.fetch("SELECT * FROM numeric_test ORDER BY id")
        
        for i, row in enumerate(rows):
            expected = test_data[i]
            
            # Check ID
            assert row['id'] == expected[0], f"ID mismatch for row {i}"
            
            # Check NUMERIC (Decimal)
            if expected[1] is not None:
                fetched_decimal = row['num_val']
                expected_decimal = expected[1]
                assert fetched_decimal == expected_decimal, \
                    f"Numeric mismatch for row {i}: got {fetched_decimal}, expected {expected_decimal}"
            
            # Check REAL (float)
            fetched_float = row['float_val']
            expected_float = expected[2]
            if expected_float != expected_float:  # NaN check
                assert fetched_float != fetched_float, f"Expected NaN for row {i}"
            elif expected_float == float('inf'):
                assert fetched_float == float('inf'), f"Expected infinity for row {i}"
            elif expected_float == float('-inf'):
                assert fetched_float == float('-inf'), f"Expected -infinity for row {i}"
            else:
                # Float4 has limited precision (~7 significant digits)
                # Use relative tolerance for comparison
                if expected_float != 0:
                    relative_error = abs(fetched_float - expected_float) / abs(expected_float)
                    assert relative_error < 1e-5, \
                        f"Float mismatch for row {i}: got {fetched_float}, expected {expected_float}, relative error {relative_error}"
                else:
                    assert abs(fetched_float) < 1e-6, \
                        f"Float mismatch for row {i}: got {fetched_float}, expected 0"
            
            # Check DOUBLE PRECISION
            fetched_double = row['double_val']
            expected_double = expected[3]
            if expected_double != expected_double:  # NaN check
                assert fetched_double != fetched_double, f"Expected NaN for row {i}"
            elif expected_double == float('inf'):
                assert fetched_double == float('inf'), f"Expected infinity for row {i}"
            elif expected_double == float('-inf'):
                assert fetched_double == float('-inf'), f"Expected -infinity for row {i}"
            else:
                assert abs(fetched_double - expected_double) < 1e-10, \
                    f"Double mismatch for row {i}: got {fetched_double}, expected {expected_double}"
        
        await conn.execute("DROP TABLE numeric_test CASCADE")
        
    finally:
        await conn.close()
    
    no_out_of_sync()


@pytest.mark.asyncio
async def test_numeric_sorting_binary():
    """Test that numeric types sort correctly with binary format."""
    conn = await sharded_async()
    
    try:
        await conn.execute("DROP TABLE IF EXISTS sort_test CASCADE")
        
        await conn.execute("""
            CREATE TABLE sort_test (
                id BIGINT PRIMARY KEY,
                num_val NUMERIC,
                float_val REAL,
                double_val DOUBLE PRECISION
            )
        """)
        
        # Insert values in random order
        test_data = [
            (3, Decimal("100.5"), 100.5, 100.5),
            (1, Decimal("-50"), -50.0, -50.0),
            (103, Decimal("0"), 0.0, 0.0),
            (2, Decimal("999.99"), 999.99, 999.99),
            (102, Decimal("-999.99"), -999.99, -999.99),
            (101, Decimal("50"), 50.0, 50.0),
            # Special values that should sort last
            (104, Decimal("0"), float('nan'), float('nan')),
        ]
        
        for row in test_data:
            await conn.execute(
                "INSERT INTO sort_test (id, num_val, float_val, double_val) VALUES ($1, $2, $3, $4)",
                *row
            )
        
        # Test NUMERIC sorting
        rows = await conn.fetch("SELECT id, num_val FROM sort_test WHERE num_val IS NOT NULL ORDER BY num_val")
        numeric_order = [row['num_val'] for row in rows]
        expected_numeric = [Decimal("-999.99"), Decimal("-50"), Decimal("0"), Decimal("0"), 
                           Decimal("50"), Decimal("100.5"), Decimal("999.99")]
        assert numeric_order == expected_numeric, f"Numeric sorting failed: {numeric_order}"
        
        # Test REAL sorting (NaN should be last)
        rows = await conn.fetch("SELECT id, float_val FROM sort_test ORDER BY float_val")
        float_ids = [row['id'] for row in rows]
        # NaN should sort last in PostgreSQL
        assert float_ids[-1] == 104, "NaN should sort last for REAL"
        
        # Test DOUBLE sorting (NaN should be last)
        rows = await conn.fetch("SELECT id, double_val FROM sort_test ORDER BY double_val")
        double_ids = [row['id'] for row in rows]
        assert double_ids[-1] == 104, "NaN should sort last for DOUBLE PRECISION"
        
        await conn.execute("DROP TABLE sort_test CASCADE")
        
    finally:
        await conn.close()
    
    no_out_of_sync()


@pytest.mark.asyncio
async def test_numeric_aggregates_binary():
    """Test numeric aggregates with binary format."""
    conn = await normal_async()
    
    try:
        await conn.execute("DROP TABLE IF EXISTS agg_test CASCADE")
        
        await conn.execute("""
            CREATE TABLE agg_test (
                id INTEGER PRIMARY KEY,
                num_val NUMERIC,
                float_val REAL,
                double_val DOUBLE PRECISION
            )
        """)
        
        # Insert test data
        for i in range(1, 11):
            await conn.execute(
                "INSERT INTO agg_test VALUES ($1, $2, $3, $4)",
                i, Decimal(str(i * 10.5)), i * 10.5, i * 10.5
            )
        
        # Test SUM
        row = await conn.fetchrow("SELECT SUM(num_val) as n, SUM(float_val) as f, SUM(double_val) as d FROM agg_test")
        assert row['n'] == Decimal("577.5")  # 10.5 + 21 + 31.5 + ... + 105
        assert abs(row['f'] - 577.5) < 0.1
        assert abs(row['d'] - 577.5) < 0.001
        
        # Test AVG
        row = await conn.fetchrow("SELECT AVG(num_val) as n, AVG(float_val) as f, AVG(double_val) as d FROM agg_test")
        assert row['n'] == Decimal("57.75")
        assert abs(row['f'] - 57.75) < 0.01
        assert abs(row['d'] - 57.75) < 0.001
        
        # Test MIN/MAX
        row = await conn.fetchrow("SELECT MIN(num_val) as min_n, MAX(num_val) as max_n FROM agg_test")
        assert row['min_n'] == Decimal("10.5")
        assert row['max_n'] == Decimal("105")
        
        await conn.execute("DROP TABLE agg_test CASCADE")
        
    finally:
        await conn.close()
    
    no_out_of_sync()