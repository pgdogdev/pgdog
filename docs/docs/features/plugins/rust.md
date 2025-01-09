# Plugins in Rust

Writing pgDog plugins in Rust has first class support built into the [`pgdog-plugin`](https://github.com/levkk/pgdog/tree/main/pgdog-plugin) crate. The crate acts
as a bridge between plugins and pgDog internals, and provides safe methods for constructing C-compatible structs.

## How it works

For plugins to be trully dynamic, they have to be compiled into shared libraries (`.so` on Linux, `.dylib` on Mac). This way you can load arbitrary plugins into pgDog at runtime without having to recompile it. Since Rust doesn't have a stable [ABI](https://en.wikipedia.org/wiki/Application_binary_interface), we have to use the only stable ABI available to all programming languages: C.

### C ABI

Rust has great bindings for using (and exposing) C-compatible functions. You can learn more about this by reading the [`std::ffi`](https://doc.rust-lang.org/stable/std/ffi/index.html) documentation and other great sources like The Embedded Rust Book[^1].

The [`pgdog-plugin`](https://github.com/levkk/pgdog/tree/main/pgdog-plugin) crate contains C [headers](https://github.com/levkk/pgdog/tree/main/pgdog-plugin/include) that define
types and functions pgDog expects its plugins to use, with Rust bindings generated with [bindgen](https://docs.rs/bindgen/latest/bindgen/).

[^1]: [https://docs.rust-embedded.org/book/interoperability/rust-with-c.html](https://docs.rust-embedded.org/book/interoperability/rust-with-c.html)


## Getting started

Create a new library crate with Cargo, like so:

```bash
cargo new --lib my_pgdog_plugin
```

Since plugins have to be C ABI compatible, you'll need to change the crate type to `cdylib` (C dynamic library).
Edit your `Cargo.toml` and add the following:

```toml
[lib]
crate-type = ["rlib", "cdylib"]
```

### Add `pgdog-plugin`

To make building plugins easier, pgDog provides a crate that defines and implements the structs used by
plugin functions.

Before proceeding, add this crate to your dependencies:

```bash
cargo add pgdog-plugin
```

### Implement the API

The [plugin API](../plugins/index.md) is pretty simple. For this tutorial, we'll implement the query routing function `pgdog_route_query`, which is called for every transaction pgDog receives.

#### `pgdog_route_query`

This function has the following signature:

```rust
use pgdog_plugin::*;

pub extern "C" fn pgdog_route_query(input: Input) -> Output {
  todo()
}
```

## Learn more

- [pgdog-routing](https://github.com/levkk/pgdog/tree/main/plugins/pgdog-routing) plugin
