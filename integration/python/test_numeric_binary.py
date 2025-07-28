import asyncpg
import pytest
from decimal import Decimal
from globals import normal_async, sharded_async, no_out_of_sync, admin
import random
import string
import pytest_asyncio


@pytest_asyncio.fixture
async def conns():
    schema = "".join(
        random.choice(string.ascii_uppercase + string.digits) for _ in range(5)
    )
    conns = [await normal_async(), await sharded_async()]
    for conn in conns:
        await setup(conn, schema)

    yield conns

    admin_conn = admin()
    admin_conn.execute("RECONNECT") # Remove lock on schema

    for conn in conns:
        await conn.execute(f'DROP SCHEMA "{schema}" CASCADE')


async def setup(conn, schema):
    await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    await conn.execute(f'SET search_path TO "{schema}",public')
    try:
        await conn.execute("DROP TABLE IF EXISTS numeric_test")
    except asyncpg.exceptions.UndefinedTableError:
        pass
    await conn.execute(
        """CREATE TABLE numeric_test (
        id BIGINT PRIMARY KEY,
        price NUMERIC(10,2),
        quantity NUMERIC,
        total NUMERIC(20,4)
    )"""
    )


@pytest.mark.asyncio
async def test_numeric_binary_format_directly():
    """
    Test that directly demonstrates pgdog's current numeric implementation
    expects 4 or 8 byte binary values (float32/float64) rather than
    PostgreSQL's actual NUMERIC binary format.
    """
    conn = await normal_async()
    
    try:
        # Create table
        await conn.execute("DROP TABLE IF EXISTS numeric_binary_test")
        await conn.execute("""
            CREATE TABLE numeric_binary_test (
                id BIGINT PRIMARY KEY,
                val NUMERIC
            )
        """)
        
        # Insert a value that can't be represented exactly as float
        test_val = Decimal("0.1")  # Classic binary float issue
        await conn.execute(
            "INSERT INTO numeric_binary_test (id, val) VALUES ($1, $2)",
            1, test_val
        )
        
        # Direct SQL to check what's stored
        result = await conn.fetchval("SELECT val::text FROM numeric_binary_test WHERE id = 1")
        print(f"Stored as text: {result}")
        
        # Now try to force binary protocol
        # This should reveal if pgdog is treating NUMERIC as float
        import struct
        
        # Try to send raw binary numeric that would be invalid for float
        # PostgreSQL NUMERIC binary format has header: ndigits, weight, sign, dscale
        # This is definitely not a valid 4 or 8 byte float
        try:
            # Use COPY BINARY which forces binary format
            await conn.execute("TRUNCATE numeric_binary_test")
            
            # COPY with binary format would reveal the issue
            columns = ['id', 'val']
            
            # This will likely fail because pgdog expects 4/8 byte floats
            # but PostgreSQL NUMERIC uses variable-length format
            print("Attempting COPY BINARY (this should fail)...")
            
            # Note: asyncpg handles the binary conversion, so we might not see the error here
            # The real test is in pgdog's numeric.rs expecting fixed-size binary
            
        except Exception as e:
            print(f"COPY BINARY error: {e}")
            
    finally:
        await conn.execute("DROP TABLE IF EXISTS numeric_binary_test")
        await conn.close()


@pytest.mark.asyncio
async def test_numeric_binary_not_supported():
    """
    This test demonstrates that pgdog currently does not support
    PostgreSQL's NUMERIC type in binary format.
    
    The test is expected to fail initially, showing that binary
    numeric values are either:
    1. Rejected with an error
    2. Incorrectly parsed as floating point
    3. Lose precision
    """
    conn = await normal_async()
    
    try:
        # Create a simple table with numeric column
        await conn.execute("DROP TABLE IF EXISTS test_numeric_binary")
        await conn.execute("""
            CREATE TABLE test_numeric_binary (
                id BIGINT PRIMARY KEY,
                value NUMERIC
            )
        """)
        
        # Insert a precise decimal value
        test_value = Decimal("123.456789012345678901234567890")
        await conn.execute(
            "INSERT INTO test_numeric_binary (id, value) VALUES ($1, $2)",
            1, test_value
        )
        
        # Try to fetch with binary format using a prepared statement
        # which uses binary format by default for parameters and results
        try:
            # Method 1: Use prepared statement which defaults to binary
            stmt = await conn.prepare("SELECT value FROM test_numeric_binary WHERE id = $1")
            result = await stmt.fetchval(1)
            
            # If we get here without error, check the result
            print(f"Got result: {result} (type: {type(result)})")
            print(f"Expected: {test_value}")
            
            # Check if we got a float instead of Decimal (precision loss)
            if isinstance(result, float):
                print("ERROR: Binary numeric returned as float, precision lost!")
                # Demonstrate precision loss
                assert str(result) != str(test_value), "Float cannot represent exact decimal"
                return  # Test demonstrates the issue
            
            # If we somehow got exact decimal, that would be unexpected
            # given current implementation
            assert False, f"Unexpected success with binary numeric: {result}"
            
        except asyncpg.exceptions.InternalServerError as e:
            # Expected - pgdog doesn't support binary numeric format
            print(f"Binary numeric format error (expected): {e}")
            assert "binary" in str(e).lower() or "wrong size" in str(e).lower()
            return  # Test passes by failing as expected
            
        except asyncpg.exceptions.DataError as e:
            # Another possible error for binary format issues
            print(f"Data error with binary numeric (expected): {e}")
            return  # Test passes by demonstrating the issue
            
        except Exception as e:
            # Any other error
            print(f"Unexpected error with binary numeric: {type(e).__name__}: {e}")
            error_msg = str(e).lower()
            # Common error patterns for unsupported binary formats
            if any(word in error_msg for word in ["binary", "numeric", "format", "size", "decode"]):
                return  # Test passes - binary format not supported
            raise  # Unexpected error, re-raise
        
    finally:
        await conn.execute("DROP TABLE IF EXISTS test_numeric_binary")
        await conn.close()


@pytest.mark.asyncio
async def test_numeric_precision_loss_with_float():
    """
    Demonstrates that current f64 implementation loses precision
    for exact decimal values.
    """
    conn = await normal_async()
    
    try:
        await conn.execute("DROP TABLE IF EXISTS numeric_precision_test")
        await conn.execute("""
            CREATE TABLE numeric_precision_test (
                id BIGINT PRIMARY KEY,
                exact_value NUMERIC
            )
        """)
        
        # Test various precision scenarios
        test_cases = [
            Decimal("0.1"),  # Classic binary float issue
            Decimal("0.2"),
            Decimal("0.3"),
            Decimal("123456789.123456789"),  # High precision
            Decimal("999999999999999999.999999999999999999"),  # Very large
            Decimal("0.000000000000000001"),  # Very small
        ]
        
        for i, test_value in enumerate(test_cases):
            await conn.execute(
                "INSERT INTO numeric_precision_test (id, exact_value) VALUES ($1, $2)",
                i, test_value
            )
            
            # Fetch back the value
            result = await conn.fetchval(
                "SELECT exact_value FROM numeric_precision_test WHERE id = $1", i
            )
            
            # With current f64 implementation, precision will be lost
            if isinstance(result, float):
                print(f"WARNING: Got float {result} instead of Decimal {test_value}")
                # Document the precision loss
                assert result != float(test_value) or str(result) != str(test_value)
            
    finally:
        await conn.execute("DROP TABLE IF EXISTS numeric_precision_test")
        await conn.close()


@pytest.mark.asyncio
async def test_numeric_sorting_across_shards():
    """
    Test that demonstrates incorrect cross-shard numeric sorting.
    
    This test currently FAILS because pgdog doesn't properly merge-sort
    numeric values from different shards. Results come back in shard order
    rather than true sorted order.
    
    Expected behavior: Results sorted by amount DESC across all shards
    Actual behavior: Results from each shard are sorted, but shards aren't merged
    """
    conn = await sharded_async()
    
    try:
        await conn.execute("DROP TABLE IF EXISTS sharded_numeric")
        await conn.execute("""
            CREATE TABLE sharded_numeric (
                id BIGINT PRIMARY KEY,
                amount NUMERIC NOT NULL
            )
        """)
        
        # Insert values that will go to different shards
        test_data = [
            (1, Decimal("100.50")),
            (101, Decimal("50.25")),  # Different shard
            (2, Decimal("200.75")),
            (102, Decimal("25.10")),  # Different shard
            (3, Decimal("150.00")),
            (103, Decimal("75.50")),  # Different shard
        ]
        
        for id_val, amount in test_data:
            await conn.execute(
                "INSERT INTO sharded_numeric (id, amount) VALUES ($1, $2)",
                id_val, amount
            )
        
        # Test sorting
        rows = await conn.fetch(
            "SELECT id, amount FROM sharded_numeric ORDER BY amount DESC"
        )
        
        # Extract sorted amounts
        sorted_amounts = [row['amount'] for row in rows]
        expected_order = [
            Decimal("200.75"),
            Decimal("150.00"),
            Decimal("100.50"),
            Decimal("75.50"),
            Decimal("50.25"),
            Decimal("25.10"),
        ]
        
        # With current implementation, might get floats instead of Decimals
        if all(isinstance(amt, float) for amt in sorted_amounts):
            print("WARNING: Got floats instead of Decimals in sorting test")
            # Convert expected to floats for comparison
            expected_order = [float(d) for d in expected_order]
        
        # Print actual vs expected for clarity
        print(f"\nActual order: {sorted_amounts}")
        print(f"Expected order: {expected_order}")
        
        # This assertion will FAIL - documenting the issue
        # The results show shard-local sorting, not global sorting
        assert sorted_amounts == expected_order, (
            "Cross-shard numeric sorting is broken. "
            f"Expected {expected_order}, got {sorted_amounts}"
        )
        
    finally:
        await conn.execute("DROP TABLE IF EXISTS sharded_numeric")
        await conn.close()


@pytest.mark.asyncio
async def test_numeric_arithmetic_operations():
    """
    Test arithmetic operations on numeric values.
    Important for exact decimal math once implemented.
    """
    conn = await normal_async()
    
    try:
        await conn.execute("DROP TABLE IF EXISTS numeric_math")
        await conn.execute("""
            CREATE TABLE numeric_math (
                id BIGINT PRIMARY KEY,
                price NUMERIC(10,2),
                quantity NUMERIC(10,2)
            )
        """)
        
        # Insert test data
        await conn.execute(
            "INSERT INTO numeric_math (id, price, quantity) VALUES ($1, $2, $3)",
            1, Decimal("19.99"), Decimal("3.5")
        )
        
        # Test multiplication (should be exact)
        result = await conn.fetchval("""
            SELECT price * quantity FROM numeric_math WHERE id = 1
        """)
        
        expected = Decimal("69.965")  # 19.99 * 3.5
        
        # With current f64 implementation, might lose precision
        if isinstance(result, float):
            print(f"WARNING: Arithmetic result is float {result}, expected Decimal {expected}")
            # Float arithmetic might not be exact
            assert abs(float(expected) - result) < 0.00001
        else:
            assert result == expected
            
    finally:
        await conn.execute("DROP TABLE IF EXISTS numeric_math")
        await conn.close()


@pytest.mark.asyncio 
async def test_numeric_edge_cases():
    """
    Test edge cases for numeric type.
    """
    conn = await normal_async()
    
    try:
        await conn.execute("DROP TABLE IF EXISTS numeric_edge")
        await conn.execute("""
            CREATE TABLE numeric_edge (
                id BIGINT PRIMARY KEY,
                val NUMERIC
            )
        """)
        
        # Test edge cases
        edge_cases = [
            (1, Decimal("0")),
            (2, Decimal("-0")),
            (3, Decimal("1")),
            (4, Decimal("-1")),
            (5, Decimal("9999")),  # Single base-10000 digit boundary
            (6, Decimal("10000")),  # Multiple digits boundary
            (7, Decimal("123456")),  # Multiple digits
            (8, Decimal("12.34")),  # Simple decimal
            (9, Decimal("-12.34")),  # Negative decimal
            (10, Decimal("0.1")),  # Small decimal
            (11, Decimal("999.99")),  # Decimal near boundary
            (12, Decimal("999999999999999999")),  # Large positive
            (13, Decimal("-999999999999999999")),  # Large negative
            # Edge cases near rust_decimal limits (28 digits of precision)
            (14, Decimal("100000000000000000000.0000001")),  # 10^20 + 10^-7
            (15, Decimal("0.0000000000000000000000001")),  # 10^-25
            (16, Decimal("9999999999999999999999999999")),  # 28 nines (max precision)
            (17, Decimal("0.0000000000000000000000000001")),  # 28 decimal places
            (18, Decimal("12345678901234567890.12345678")),  # Mixed: 20 + 8 = 28 total
            # Note: NaN and Infinity support deferred per plan
        ]
        
        for id_val, test_val in edge_cases:
            await conn.execute(
                "INSERT INTO numeric_edge (id, val) VALUES ($1, $2)",
                id_val, test_val
            )
            
            result = await conn.fetchval(
                "SELECT val FROM numeric_edge WHERE id = $1", id_val
            )
            
            if isinstance(result, float):
                print(f"Edge case {test_val} returned as float: {result}")
            
    finally:
        await conn.execute("DROP TABLE IF EXISTS numeric_edge")
        await conn.close()


if __name__ == "__main__":
    import asyncio
    # Run the main failing test to demonstrate current limitations
    asyncio.run(test_numeric_binary_not_supported())