# pgDog plugins

pgDog plugin system is based around shared libraries loaded at runtime.
These libraries can be written in any language as long as they are compiled to `.so` (or `.dylib` on Mac),
and can expose predefined C ABI functions.

This crate implements the bridge between the C ABI and pgDog, defines common C types and interface to use,
and exposes internal pgDog functionality to plugins to query pooler state and
create objects that can be shared between the two.

This crate is a C (and Rust) library that should be linked at compile time against your plugins.

## Writing plugins

Examples of plugins written in C and Rust are available [here](https://github.com/levkk/pgdog/tree/main/examples).
