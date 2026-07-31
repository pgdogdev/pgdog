from typing import Any, Callable

from alembic import context
from sqlalchemy import engine_from_config, event, pool, text
from sqlalchemy.engine import Connection
from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.pool import ConnectionPoolEntry


config = context.config
target_metadata = None
SHARDED_SCHEMAS = {"shard_0", "shard_1"}


def _patch_dialect_initialize_wrapper(
    shard: int | None = None,
    tenant: str | None = None,
) -> Callable[[Dialect, ConnectionPoolEntry, tuple[Any, ...], dict[str, Any]], None]:
    if shard is not None and tenant is not None:
        raise TypeError("Cannot provide both tenant and shard")

    def _do_connect(
        dialect: Dialect,
        conn_rec: ConnectionPoolEntry,
        cargs: tuple[Any, ...],
        cparams: dict[str, Any],
    ) -> None:
        """
        Before connecting to the database, patch the initialize method on the dialect to ensure
        statements executed on the dbapi connection is routed to the proper shard or tenant.

        For engines used to execute migrations that target a specific shard, we set `pgdog.shard` to
        the shard number. This query routing should persist for the duration of the transaction.

        For engines used to execute migrations that target a specific tenant, we set
        `search_path` to the tenant name. This query routing should persist for the duration
        of the transaction.
        """
        del conn_rec, cargs, cparams
        original_initialize = dialect.initialize

        def patch_initialize(connection: Connection) -> None:
            with connection.begin():
                if shard is not None:
                    connection.execute(text(f"SET pgdog.shard TO {shard}"))
                if tenant is not None:
                    connection.execute(text(f"SET LOCAL search_path TO '{tenant}'"))
                original_initialize(connection)

        dialect.initialize = patch_initialize  # type: ignore[method-assign]

    return _do_connect


def migration_schema():
    schema = context.get_x_argument(as_dictionary=True).get("schema")
    if schema not in SHARDED_SCHEMAS:
        raise ValueError("Alembic requires -x schema=shard_0 or -x schema=shard_1")
    return schema


def run_migrations_offline():
    schema = migration_schema()
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    schema = migration_schema()
    configuration = config.get_section(config.config_ini_section, {})
    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    event.listen(
        connectable,
        "do_connect",
        _patch_dialect_initialize_wrapper(tenant=schema),
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
        )

        with context.begin_transaction():
            connection.execute(text(f"SET LOCAL search_path TO '{schema}', public"))
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
