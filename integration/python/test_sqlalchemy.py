from __future__ import annotations
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.exc import IntegrityError, DBAPIError
from sqlalchemy import select, text
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
import pytest_asyncio
import pytest
from sqlalchemy.sql.expression import delete
from globals import admin, schema_sharded_async


class Base(AsyncAttrs, DeclarativeBase):
    pass


class Sharded(Base):
    __tablename__ = "sharded"

    id: Mapped[int] = mapped_column(primary_key=True)
    value: Mapped[str]


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    email: Mapped[str]

# class Products(Base):
#     __tablename__ == "products"

#     id: Mapped[int] = mapped_column(primary_key=True)
#     name: Mapped[str]


@pytest_asyncio.fixture
async def engines():
    # Configure connection pool for stress testing
    normal = create_async_engine(
        "postgresql+asyncpg://pgdog:pgdog@127.0.0.1:6432/pgdog",
        pool_size=20,          # Number of connections to maintain in pool
        max_overflow=30,       # Additional connections beyond pool_size
        pool_timeout=30,       # Timeout when getting connection from pool
        pool_recycle=3600,     # Recycle connections after 1 hour
        pool_pre_ping=True,    # Verify connections before use
    )
    normal_sessions = async_sessionmaker(normal, expire_on_commit=True)

    sharded = create_async_engine(
        "postgresql+asyncpg://pgdog:pgdog@127.0.0.1:6432/pgdog_sharded",
        pool_size=20,
        max_overflow=30,
        pool_timeout=30,
        pool_recycle=3600,
        pool_pre_ping=True,
    )
    sharded_sessions = async_sessionmaker(sharded, expire_on_commit=True)

    return [(normal, normal_sessions), (sharded, sharded_sessions)]

@pytest_asyncio.fixture
async def schema_sharding_engine():
    from sqlalchemy import event

    pool = create_async_engine(
        "postgresql+asyncpg://pgdog:pgdog@127.0.0.1:6432/pgdog_schema",
        pool_size=20,          # Number of connections to maintain in pool
        max_overflow=30,       # Additional connections beyond pool_size
        pool_timeout=30,       # Timeout when getting connection from pool
        pool_recycle=3600,     # Recycle connections after 1 hour
        pool_pre_ping=True,    # Verify connections before use
    )

    @event.listens_for(pool.sync_engine, "connect")
    def set_search_path(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("SET search_path TO shard_0, public")
        cursor.close()
        dbapi_connection.commit()

    session = async_sessionmaker(pool, expire_on_commit=True)
    return pool, session

@pytest_asyncio.fixture
async def schema_sharding_startup_param():
    pool = create_async_engine(
        "postgresql+asyncpg://pgdog:pgdog@127.0.0.1:6432/pgdog_schema",
        connect_args={
            "server_settings": {
                "search_path": "shard_0,public",
            }
        },
        pool_size=20,          # Number of connections to maintain in pool
        max_overflow=30,       # Additional connections beyond pool_size
        pool_timeout=30,       # Timeout when getting connection from pool
        pool_recycle=3600,     # Recycle connections after 1 hour
        pool_pre_ping=True,    # Verify connections before use
    )

    session = async_sessionmaker(pool, expire_on_commit=True)
    return pool, session

@pytest.mark.asyncio
async def test_session_manager(engines):
    for engine, session_factory in engines:
        async with session_factory() as session:
            await session.execute(text("DROP TABLE IF EXISTS sharded"))
            await session.execute(
                text("CREATE TABLE sharded (id BIGINT PRIMARY KEY, value VARCHAR)")
            )
            await session.commit()

            async with session.begin():
                stmt = delete(Sharded)
                await session.execute(stmt)
            # Transaction auto-commits with engine.begin()

            async with session.begin():
                session.add_all(
                    [
                        Sharded(id=1, value="test@test.com"),
                    ]
                )

            stmt = select(Sharded).order_by(Sharded.id).where(Sharded.id == 1)
            result = await session.execute(stmt)
            rows = result.fetchall()
            assert len(rows) == 1


@pytest.mark.asyncio
async def test_with_errors(engines):
    for engine, session_factory in engines:
        async with session_factory() as session:
            await session.execute(text("DROP TABLE IF EXISTS sharded"))
            await session.execute(
                text("CREATE TABLE sharded (id BIGINT PRIMARY KEY, value VARCHAR)")
            )
            await session.commit()

        async with session_factory() as session:
            async with session.begin():
                try:
                    session.add_all(
                        [
                            Sharded(id=1, value="test"),
                            Sharded(id=1, value="test"),  # duplicate key constraint
                        ]
                    )
                    await session.flush()  # Force the constraint error
                except IntegrityError as e:
                    assert (
                        'duplicate key value violates unique constraint "sharded_pkey"'
                        in str(e)
                    )
                    # The transaction will be automatically rolled back when exiting the context

        async with session_factory() as session:
            session.add_all([Sharded(id=3, value="test")])
            await session.commit()
    for engine, session_factory in engines:
        async with session_factory() as session:
            session.add(Sharded(id=5, value="random"))
            session.add(Sharded(id=6, value="random"))
            await session.commit()
            result = await session.execute(select(Sharded).where(Sharded.id == 6))
            rows = result.fetchall()
            assert len(rows) == 1


@pytest.mark.asyncio
async def test_reads_writes(engines):
    normal_engine, normal = engines[0]  # Not sharded
    reads = set()
    admin().cursor().execute("SET read_write_split TO 'exclude_primary'")

    for i in range(50):
        email = f"test-{i}@test.com"
        async with normal() as session:
            await session.execute(text("DROP TABLE IF EXISTS users CASCADE"))
            await session.execute(
                text("CREATE TABLE users (id BIGSERIAL PRIMARY KEY, email VARCHAR)")
            )
            await session.commit()
        async with normal() as session:
            session.add(User(email=email))
            await session.commit()
        async with normal() as session:
            await session.begin()
            stmt = select(User).filter(User.email == email)
            user = await session.execute(stmt)
            user = user.scalar_one_or_none()
            assert user.email == email
            result = await session.execute(text("SHOW default_transaction_read_only"))
            rows = result.fetchone()
            reads.add(rows[0])
            await session.commit()
    assert list(reads) == ["on"]
    admin().cursor().execute("RELOAD")


@pytest.mark.asyncio
async def test_write_in_read(engines):
    normal_engine, normal = engines[0]

    for i in range(50):
        async with normal() as session:
            # Setup
            await session.execute(text("DROP TABLE IF EXISTS test_read_write"))
            await session.execute(text("CREATE TABLE test_read_write (id BIGINT)"))
            await session.commit()
            # Trigger PgDog to route this to a replica with a read
            await session.begin()
            await session.execute(text("SELECT * FROM test_read_write"))
            try:
                # This is still inside the same transaction. The entire transaction
                # is going to a replica right now.
                await session.execute(text("INSERT INTO test_read_write VALUES (1)"))
            except DBAPIError as e:
                assert "cannot execute INSERT in a read-only transaction" in str(e)


@pytest.mark.asyncio
async def test_connection_pool_stress(engines):
    """
    Test prepared statement cache implementation by hitting pgdog with many different queries
    using a connection pool. This should exercise the prepared statement cache heavily.
    """
    import asyncio
    import random
    import string

    normal_engine, normal = engines[0]

    # Setup test table
    async with normal() as session:
        await session.execute(text("DROP TABLE IF EXISTS stress_test"))
        await session.execute(text("""
            CREATE TABLE stress_test (
                id BIGSERIAL PRIMARY KEY,
                name VARCHAR(50),
                age INTEGER,
                score DECIMAL(5,2),
                active BOOLEAN,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """))
        await session.commit()

    # Insert initial data
    async with normal() as session:
        for i in range(100):
            name = ''.join(random.choices(string.ascii_letters, k=10))
            age = random.randint(18, 80)
            score = round(random.uniform(0, 100), 2)
            active = random.choice([True, False])
            await session.execute(text("""
                INSERT INTO stress_test (name, age, score, active)
                VALUES (:name, :age, :score, :active)
            """), {"name": name, "age": age, "score": score, "active": active})
        await session.commit()

    async def run_varied_queries(engine, task_id):
        """Run many different queries to stress the prepared statement cache"""
        queries_run = 0

        for i in range(20):  # 20 queries per task
            max_retries = 3
            for retry in range(max_retries):
                try:
                    # Use engine directly for better connection pool control
                    async with engine.begin() as conn:
                        # Vary the query patterns to create different prepared statements
                        # Use task_id and iteration to create more unique queries
                        query_type = random.randint(1, 12)
                        variation = (task_id * 20 + i) % 10  # Creates 200 unique variations

                        if query_type == 1:
                            # Simple select with WHERE - vary the column to create unique statements
                            age_filter = 20 + variation
                            if variation % 3 == 0:
                                result = await conn.execute(text(
                                    "SELECT COUNT(*) FROM stress_test WHERE age > :age"
                                ), {"age": age_filter})
                            elif variation % 3 == 1:
                                result = await conn.execute(text(
                                    "SELECT COUNT(*) FROM stress_test WHERE age >= :age"
                                ), {"age": age_filter})
                            else:
                                result = await conn.execute(text(
                                    "SELECT COUNT(*) FROM stress_test WHERE age = :age"
                                ), {"age": age_filter})

                        elif query_type == 2:
                            # Complex WHERE with multiple conditions
                            min_age = random.randint(18, 40)
                            max_score = random.uniform(50, 100)
                            result = await conn.execute(text("""
                                SELECT name, age, score FROM stress_test
                                WHERE age >= :min_age AND score <= :max_score AND active = true
                                LIMIT 10
                            """), {"min_age": min_age, "max_score": max_score})

                        elif query_type == 3:
                            # Aggregation with GROUP BY
                            result = await conn.execute(text("""
                                SELECT active, AVG(score) as avg_score, COUNT(*) as count
                                FROM stress_test
                                GROUP BY active
                                HAVING COUNT(*) > :min_count
                            """), {"min_count": random.randint(1, 10)})

                        elif query_type == 4:
                            # ORDER BY with different columns - use variation for uniqueness
                            order_col = ['age', 'score', 'name', 'created_at'][variation % 4]
                            order_dir = ['ASC', 'DESC'][variation % 2]
                            result = await conn.execute(text(f"""
                                SELECT * FROM stress_test
                                WHERE score > :score
                                ORDER BY {order_col} {order_dir}
                                LIMIT :limit
                            """), {"score": variation * 5, "limit": 5 + variation})

                        elif query_type == 5:
                            # JOIN with subquery
                            result = await conn.execute(text("""
                                SELECT s.name, s.age
                                FROM stress_test s
                                WHERE s.score > (
                                    SELECT AVG(score) FROM stress_test WHERE active = :active
                                )
                                LIMIT :limit
                            """), {"active": random.choice([True, False]), "limit": random.randint(3, 8)})

                        elif query_type == 6:
                            # UPDATE with different conditions - use task_id to avoid conflicts
                            min_age_base = 20 + (task_id * 5)  # Each task gets different age range
                            await conn.execute(text("""
                                UPDATE stress_test
                                SET score = score + :bonus
                                WHERE age BETWEEN :min_age AND :max_age
                            """), {
                                "bonus": random.uniform(-5, 5),
                                "min_age": min_age_base,
                                "max_age": min_age_base + 4
                            })
                            # Transaction auto-commits with engine.begin()

                        elif query_type == 7:
                            # INSERT with varying values
                            name = ''.join(random.choices(string.ascii_letters, k=8))
                            await conn.execute(text("""
                                INSERT INTO stress_test (name, age, score, active)
                                VALUES (:name, :age, :score, :active)
                            """), {
                                "name": f"stress_{name}_{task_id}",
                                "age": random.randint(18, 80),
                                "score": round(random.uniform(0, 100), 2),
                                "active": random.choice([True, False])
                            })
                            # Transaction auto-commits with engine.begin()

                        elif query_type == 8:
                            # DELETE with different conditions
                            await conn.execute(text("""
                                DELETE FROM stress_test
                                WHERE name LIKE :pattern AND score < :max_score
                            """), {
                                "pattern": f"stress_%_{task_id}",
                                "max_score": random.uniform(10, 30)
                            })
                            # Transaction auto-commits with engine.begin()

                        elif query_type == 9:
                            # Different SELECT with JOIN-like pattern
                            result = await conn.execute(text(f"""
                                SELECT name, score FROM stress_test
                                WHERE active = :active AND score BETWEEN :min_score AND :max_score
                                ORDER BY score {['ASC', 'DESC'][variation % 2]}
                                LIMIT :limit
                            """), {
                                "active": variation % 2 == 0,
                                "min_score": variation * 10,
                                "max_score": variation * 10 + 20,
                                "limit": 5 + variation
                            })

                        elif query_type == 10:
                            # Window function queries
                            result = await conn.execute(text(f"""
                                SELECT name, age, score,
                                       ROW_NUMBER() OVER (ORDER BY score {['ASC', 'DESC'][variation % 2]}) as rank
                                FROM stress_test
                                WHERE age > :min_age
                                LIMIT :limit
                            """), {
                                "min_age": 20 + variation,
                                "limit": 10 + variation
                            })

                        elif query_type == 11:
                            # CASE statement variations
                            result = await conn.execute(text(f"""
                                SELECT name,
                                       CASE
                                           WHEN score > :high_threshold THEN 'High'
                                           WHEN score > :med_threshold THEN 'Medium'
                                           ELSE 'Low'
                                       END as score_category,
                                       age
                                FROM stress_test
                                WHERE active = :active
                                ORDER BY {['age', 'score', 'name'][variation % 3]}
                                LIMIT :limit
                            """), {
                                "high_threshold": 70 + variation,
                                "med_threshold": 40 + variation,
                                "active": variation % 2 == 0,
                                "limit": 8 + variation
                            })

                        elif query_type == 12:
                            # Advanced aggregation with different GROUP BY
                            if variation % 2 == 0:
                                result = await conn.execute(text("""
                                    SELECT
                                        CASE WHEN age < :age_threshold THEN 'Young' ELSE 'Old' END as age_group,
                                        AVG(score) as avg_score,
                                        COUNT(*) as count,
                                        MIN(score) as min_score,
                                        MAX(score) as max_score
                                    FROM stress_test
                                    GROUP BY CASE WHEN age < :age_threshold THEN 'Young' ELSE 'Old' END
                                    HAVING COUNT(*) > :min_count
                                """), {
                                    "age_threshold": 30 + variation,
                                    "min_count": variation + 1
                                })
                            else:
                                result = await conn.execute(text("""
                                    SELECT active,
                                           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY score) as median_score,
                                           COUNT(*) as total
                                    FROM stress_test
                                    WHERE score > :min_score
                                    GROUP BY active
                                """), {
                                    "min_score": variation * 5
                                })

                    queries_run += 1
                    break  # Success, break out of retry loop

                except Exception as e:
                    # Handle serialization errors with retry
                    if "serialization" in str(e).lower() and retry < max_retries - 1:
                        await asyncio.sleep(0.01 * (retry + 1))  # Small backoff
                        continue  # Retry
                    else:
                        raise  # Re-raise if not a serialization error or out of retries

        return queries_run

    # Run multiple concurrent tasks to stress the connection pool and prepared statement cache
    tasks = []
    for task_id in range(20):  # 20 concurrent tasks
        task = asyncio.create_task(run_varied_queries(normal_engine, task_id))
        tasks.append(task)

    # Wait for all tasks to complete
    results = await asyncio.gather(*tasks)

    # Verify we ran the expected number of queries
    total_queries = sum(results)
    assert total_queries == 400  # 20 tasks * 20 queries each

    # Verify data integrity - table should still be accessible
    async with normal() as session:
        result = await session.execute(text("SELECT COUNT(*) FROM stress_test"))
        count = result.scalar()
        assert count > 0  # Should have some data left


@pytest.mark.asyncio
async def test_schema_sharding_with_startup_param_and_set_after_query(schema_sharding_startup_param):
    import asyncio

    admin().cursor().execute("SET cross_shard_disabled TO true")

    (pool, session_factory) = schema_sharding_startup_param

    async def run_test(task_id):
        async with session_factory() as session:
            async with session.begin():
                await session.execute(text("SELECT 1"))
                await session.execute(text("SET LOCAL work_mem TO '4MB'"))
                for row in await session.execute(text("SHOW search_path")):
                    print(row)

    tasks = [asyncio.create_task(run_test(i)) for i in range(10)]
    await asyncio.gather(*tasks)

    admin().cursor().execute("SET cross_shard_disabled TO false")


@pytest.mark.asyncio
async def test_schema_sharding(schema_sharding_engine):
    import asyncio

    admin().cursor().execute("SET cross_shard_disabled TO true")
    # All queries should touch shard_0 only.
    # Set it up separately
    conn = await schema_sharded_async()
    await conn.execute("SET search_path TO shard_0, public")
    await conn.execute("CREATE SCHEMA IF NOT EXISTS shard_0")
    await conn.execute("DROP TABLE IF EXISTS shard_0.test_schema_sharding")
    await conn.execute("CREATE TABLE shard_0.test_schema_sharding(id BIGINT)")
    await conn.close()

    (pool, session_factory) = schema_sharding_engine

    async def run_schema_sharding_test(task_id):
        for _ in range(10):
            async with session_factory() as session:
                async with session.begin():
                    await session.execute(text("SET LOCAL work_mem TO '4MB'"))
                    for row in await session.execute(text("SHOW search_path")):
                        print(row)
                    await session.execute(text("SELECT 1"))
                    await session.execute(text("SELECT * FROM test_schema_sharding"))

    # Run 10 concurrent executions in parallel
    tasks = [asyncio.create_task(run_schema_sharding_test(i)) for i in range(1)]
    await asyncio.gather(*tasks)

    admin().cursor().execute("SET cross_shard_disabled TO false")

@pytest.mark.asyncio
async def test_role_selection():
    engine = create_async_engine(
        "postgresql+asyncpg://pgdog:pgdog@127.0.0.1:6432/pgdog",
        pool_size=20,
        max_overflow=30,
        pool_timeout=30,
        pool_recycle=3600,
        pool_pre_ping=True,
        connect_args={"server_settings": {"pgdog.role": "primary"}},
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=True)

    for _ in range(1):
        async with session_factory() as session:
            async with session.begin():
                await session.execute(text("CREATE TABLE IF NOT EXISTS test_role_selection(id BIGINT)"))
                await session.execute(text("CREATE TABLE IF NOT EXISTS test_role_selection(id BIGINT)"))
                await session.rollback()
