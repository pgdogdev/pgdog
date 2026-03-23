#!/usr/bin/env python3
"""Generate PgDog wildcard configuration for production readiness testing."""

import argparse
import os
import textwrap


def generate_pgdog_toml(args: argparse.Namespace) -> str:
    return textwrap.dedent(f"""\
        [general]
        host = "0.0.0.0"
        port = 6432
        workers = 4
        default_pool_size = {args.pool_size}
        min_pool_size = 0
        pooler_mode = "transaction"
        idle_timeout = 60000
        checkout_timeout = 5000
        healthcheck_interval = 30000
        healthcheck_timeout = 5000
        connect_timeout = 5000
        query_timeout = 30000
        load_balancing_strategy = "random"
        prepared_statements = "extended"
        passthrough_auth = "enabled_plain"
        openmetrics_port = 9090
        openmetrics_namespace = "pgdog"
        max_wildcard_pools = {args.max_wildcard_pools}
        wildcard_pool_idle_timeout = {args.wildcard_idle_timeout}

        [[databases]]
        name = "*"
        host = "{args.host}"
        port = {args.port}
        database_name = "*"
    """)


def generate_users_toml() -> str:
    return textwrap.dedent("""\
        [[users]]
        name = "*"
        database = "*"
    """)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate PgDog config for production readiness testing"
    )
    parser.add_argument("--tenant-count", type=int, default=2000)
    parser.add_argument("--pool-size", type=int, default=10)
    parser.add_argument("--max-wildcard-pools", type=int, default=0)
    parser.add_argument("--wildcard-idle-timeout", type=int, default=300)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=15432)
    parser.add_argument("--output-dir", default=os.path.join(os.path.dirname(__file__), "..", "config"))
    args = parser.parse_args()

    output_dir = os.path.abspath(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)

    pgdog_path = os.path.join(output_dir, "pgdog.toml")
    with open(pgdog_path, "w") as f:
        f.write(generate_pgdog_toml(args))

    users_path = os.path.join(output_dir, "users.toml")
    with open(users_path, "w") as f:
        f.write(generate_users_toml())

    print(f"Generated: {pgdog_path}")
    print(f"Generated: {users_path}")
    print(f"Configured for {args.tenant_count} tenants, pool_size={args.pool_size}, "
          f"max_wildcard_pools={args.max_wildcard_pools}")


if __name__ == "__main__":
    main()
