# pgDog plugins

[![Documentation](https://img.shields.io/badge/documentation-blue?style=flat)](https://pgdog.dev)
[![Latest crate](https://img.shields.io/crates/v/pgdog-plugin.svg)](https://crates.io/crates/pgdog-plugin)
[![Reference docs](https://img.shields.io/docsrs/pgdog-plugin)](https://docs.rs/pgdog-plugin/)

pgDog plugin system is based around shared libraries loaded at runtime.
These libraries can be written in any language as long as they are compiled to `.so` (or `.dylib` on Mac),
and can expose predefined C ABI functions.

This crate implements the bridge between the C ABI and pgDog, defines common C types and interface to use,
and exposes internal pgDog configuration.

This crate is a C (and Rust) library that should be linked at compile time against your plugins.

## Writing plugins

Examples of plugins written in C and Rust are available [here](https://github.com/levkk/pgdog/tree/main/examples).
