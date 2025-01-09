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

The [plugin API](../plugins/index.md) is pretty simple. For this tutorial, we'll implement the query routing function `pgdog_route_query`, which is called for the first query in every transaction pgDog receives.


This function has the following signature:

```rust
use pgdog_plugin::*;

pub extern "C" fn pgdog_route_query(input: Input) -> Output {
  todo!()
}
```

The [`Input`](https://docs.rs/pgdog-plugin/latest/pgdog_plugin/input/index.html) structure contains the query pgDog received and the current state of the pooler configuration, like
the number of shards, the number of replicas and their addresses, and other information which the plugin can use
to determine where the query should go. 

The plugin is expected to return an [`Output`](https://docs.rs/pgdog-plugin/latest/pgdog_plugin/output/index.html) structure which contains its routing decision and any additional data
the plugin wants pgDog to use, like an error it wants pgDog to return to the client instead, for example.

Both structures have Rust implementations which can help us avoid having to write C-like initialization code.

### Parse the input

You can get the query pgDog received from the input structure like so:

```rust
if let Some(query) = input.query() {
  // Parse the query.
}
```

The query is a Rust string, so your routing algorithm can be as simple as:

```rust
let route = if query.starts_with("SELECT") {
  // Route this to any replica.
  Route::read_any()
} else {
  // Send the query to a primary.
  Route::write_any()
}
```

Both `read_any` and `write_any` are typically used in a single shard configuration and tell pgDog
that the shard number is not important. pgDog will send the query to the first shard in the configuration.

### Return the output

The `Output` structure contains the routing decision and any additional metadata. Since our plugin parsed the query and decided to forward this query to a database without modifications, the return value for `Output` should be:

```rust
return Output::forward(route)
```

Not all plugins have to make a routing decision. For example, if your plugin just wants to count how many queries of a certain type your database receives but doesn't care about routing, you can tell pgDog to skip your plugin's routing decision:

```rust
return Output::skip()
```

pgDog will ignore this output and pass the query to the next plugin in the chain.

### Parsing query parameters

PostgreSQL protocol has two ways to send queries to the database: using the simple query method with the parameters
included in the query text, and the extended protocol which sends parameters separately to prevent SQL injection attacks and allow for query re-use (prepared statements).

The extended protocol is widely used, so queries your plugins will see will typically look like this:

```postgresql
SELECT * FROM users WHERE id = $1
```

If your plugin is sharding requests based on a hash (or some other function) of the `"users"."id"` column, you need
to see the value of `$1` before your plugin can make a decision.

pgDog supports parsing the extended protocol and provides the full query text and parameters to its plugins. You can access a specific parameter by calling `Query::parameter`:

```rust
if let Some(id) = query.parameter(0) {
  // Parse the parameter.
}
```

!!! note
    PostgreSQL uses a lot of 1-based indexing, e.g. parameters and arrays
    start at 1. pgDog is more "rusty" and uses 0-based indexing. To access the first
    parameter in a query, index it by `0`, not `1`.

Parameters are encoded using PostgreSQL wire protocol, so they can be either UTF-8 text or binary. If they are text,
which is often the case, you can access it like so:

```rust
if let Some(id) = id.as_str() {
  let id = id.parse::<i64>();
}
```

In the case of binary encoding, `as_str()` will return `None` and you can parse the binary encoding instead:

```rust
if let Ok(id) = id.as_bytes().try_into() {
  let id = i64::from_be_bytes(id);
}
```

While this may seem tedious at first, this provides the highest flexibility for parsing parameters. A plugin
can use any kind of field for routing, e.g. cosine similarity of a vector column (to another), which requires
parsing vector-encoded fields.

## Learn more

- [pgdog-routing](https://github.com/levkk/pgdog/tree/main/plugins/pgdog-routing) plugin
