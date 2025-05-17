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


@pytest_asyncio.fixture
async def engines():
    normal = create_async_engine(
        "postgresql+asyncpg://pgdog:pgdog@127.0.0.1:6432/pgdog"
    )
    normal = async_sessionmaker(normal, expire_on_commit=True)

    sharded = create_async_engine(
        "postgresql+asyncpg://pgdog:pgdog@127.0.0.1:6432/pgdog_sharded"
    )
    sharded = async_sessionmaker(sharded, expire_on_commit=True)

    return [normal, sharded]


@pytest.mark.asyncio
async def test_session_manager(engines):
    for engine in engines:
        async with engine() as session:
            await session.execute(text("DROP TABLE IF EXISTS sharded"))
            await session.execute(
                text("CREATE TABLE sharded (id BIGINT PRIMARY KEY, value VARCHAR)")
            )
            await session.commit()

            async with session.begin():
                stmt = delete(Sharded)
                await session.execute(stmt)
            await session.commit()

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
    for engine in engines:
        async with engine() as session:
            await session.execute(text("DROP TABLE IF EXISTS sharded"))
            await session.execute(
                text("CREATE TABLE sharded (id BIGINT PRIMARY KEY, value VARCHAR)")
            )
            await session.commit()

        async with engine() as session:
            try:
                session.add_all(
                    [
                        Sharded(id=1, value="test"),
                        Sharded(id=1, value="test"),  # duplicate key constraint
                    ]
                )
                await session.commit()
            except IntegrityError as e:
                assert (
                    'duplicate key value violates unique constraint "sharded_pkey"'
                    in str(e)
                )
                await session.rollback()

            session.add_all([Sharded(id=3, value="test")])
            await session.commit()
    for engine in engines:
        async with engine() as session:
            session.add(Sharded(id=5, value="random"))
            await session.commit()
            session.add(Sharded(id=6, value="random"))
            result = await session.execute(select(Sharded).where(Sharded.id == 6))
            rows = result.fetchall()
            assert len(rows) == 1


@pytest.mark.asyncio
async def test_reads_writes(engines):
    normal = engines[0]  # Not sharded
    reads = set()

    for i in range(50):
        email = f"test-{i}@test.com"
        async with normal() as session:
            await session.execute(text("DROP TABLE IF EXISTS users"))
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
    assert len(reads) == 2


@pytest.mark.asyncio
async def test_write_in_read(engines):
    normal = engines[0]

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
