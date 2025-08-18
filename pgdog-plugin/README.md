# PgDog plugins

[![Documentation](https://img.shields.io/badge/documentation-blue?style=flat)](https://docsrs.pgdog.dev/pgdog_plugin/index.html)
[![Latest crate](https://img.shields.io/crates/v/pgdog-plugin.svg)](https://crates.io/crates/pgdog-plugin)

PgDog plugin system is based around shared libraries loaded at runtime. The plugins currently can only be
written in Rust. This is because PgDog passes Rust-specific data types to plugin functions, and those cannot
be easily made C ABI-compatible.

This crate implements the bridge between PgDog and plugins, making sure data types can be safely passed through the FFI.

Automatic checks include:

- Rust compiler version check
- `pg_query` version check

This crate should be linked at compile time against your plugins.

## Writing plugins

See [documentation](https://docsrs.pgdog.dev/pgdog_plugin/index.html) for examples. Example plugins are [available in GitHub](https://github.com/pgdogdev/pgdog/tree/main/plugins) as well.

## License

This library is distributed under the MIT license. See [LICENSE](LICENSE) for details.
