from __future__ import annotations
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine
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
