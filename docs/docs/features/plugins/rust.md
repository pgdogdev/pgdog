# Plugins in Rust

Writing PgDog plugins in Rust has first class support built into the [`pgdog-plugin`](https://github.com/levkk/pgdog/tree/main/pgdog-plugin) crate. The crate acts
as a bridge between plugins and PgDog internals, and provides safe methods for constructing C-compatible structs.

## How it works

For plugins to be truly dynamic, they have to be compiled into shared libraries (`.so` on Linux, `.dylib` on Mac). This way you can load arbitrary plugins into PgDog at runtime without having to recompile it. Since Rust doesn't have a stable [ABI](https://en.wikipedia.org/wiki/Application_binary_interface), we have to use the only stable ABI available to all programming languages: C.

### C ABI

Rust has great bindings for using (and exposing) C-compatible functions. You can learn more about this by reading the [`std::ffi`](https://doc.rust-lang.org/stable/std/ffi/index.html) documentation and other great sources like The Embedded Rust Book[^1].

The [`pgdog-plugin`](https://github.com/levkk/pgdog/tree/main/pgdog-plugin) crate contains C [headers](https://github.com/levkk/pgdog/tree/main/pgdog-plugin/include) that define
types and functions PgDog expects its plugins to use, with Rust bindings generated with [bindgen](https://docs.rs/bindgen/latest/bindgen/).

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

To make building plugins easier, PgDog provides a crate that defines and implements the structs used by
plugin functions.

Before proceeding, add this crate to your dependencies:

```bash
cargo add pgdog-plugin
```

### Implement the API

The [plugin API](../plugins/index.md) is pretty simple. For this tutorial, we'll implement the query routing function `pgdog_route_query`, which is called for the first query in every transaction PgDog receives.


This function has the following signature:

```rust
use pgdog_plugin::*;

pub extern "C" fn pgdog_route_query(input: Input) -> Output {
  todo!()
}
```

The [`Input`](https://docs.rs/pgdog-plugin/latest/pgdog_plugin/input/index.html) structure contains the query PgDog received and the current state of the pooler configuration, like
the number of shards, the number of replicas and their addresses, and other information which the plugin can use
to determine where the query should go.

The plugin is expected to return an [`Output`](https://docs.rs/pgdog-plugin/latest/pgdog_plugin/output/index.html) structure which contains its routing decision and any additional data
the plugin wants PgDog to use, like an error it wants PgDog to return to the client instead, for example.

Both structures have Rust implementations which can help us avoid having to write C-like initialization code.

### Parse the input

You can get the query PgDog received from the input structure like so:

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

Both `read_any` and `write_any` are typically used in a single shard configuration and tell PgDog
that the shard number is not important. PgDog will send the query to the first shard in the configuration.

### Return the output

The `Output` structure contains the routing decision and any additional metadata. Since our plugin parsed the query and decided to forward this query to a database without modifications, the return value for `Output` should be:

```rust
return Output::forward(route)
```

Not all plugins have to make a routing decision. For example, if your plugin just wants to count how many queries of a certain type your database receives but doesn't care about routing, you can tell PgDog to skip your plugin's routing decision:

```rust
return Output::skip()
```

PgDog will ignore this output and pass the query to the next plugin in the chain.

### Parsing query parameters

PostgreSQL protocol has two ways to send queries to the database: using the simple query method with the parameters
included in the query text, and the extended protocol which sends parameters separately to prevent SQL injection attacks and allow for query re-use (prepared statements).

The extended protocol is widely used, so queries your plugins will see will typically look like this:

```postgresql
SELECT * FROM users WHERE id = $1
```

If your plugin is sharding requests based on a hash (or some other function) of the `"users"."id"` column, you need
to see the value of `$1` before your plugin can make a decision.

PgDog supports parsing the extended protocol and provides the full query text and parameters to its plugins. You can access a specific parameter by calling `Query::parameter`:

```rust
if let Some(id) = query.parameter(0) {
  // Parse the parameter.
}
```

!!! note
    PostgreSQL uses a lot of 1-based indexing, e.g. parameters and arrays
    start at 1. PgDog is more "rusty" and uses 0-based indexing. To access the first
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

!!! note
    As the project evolves, I expect we'll add
    more helpers to the `pgdog-plugin` crate to help parse
    parameters automatically.


## SQL parsers

Parsing SQL manually can be error-prone, and there are multiple great SQL parsers you can pick off the shelf. The [pgdog-routing](https://github.com/levkk/pgdog/tree/main/plugins/pgdog-routing) plugin which ships with PgDog uses `pg_query.rs`, which in turn uses the internal PostgreSQL query
parser. This ensures all valid PostgreSQL queries are recognized and parsed correctly.

Other SQL parsers in the Rust community include [sqlparser](https://docs.rs/sqlparser/latest/sqlparser/) which
can parse many dialects, including other databases like MySQL, if you wanted to rewrite MySQL queries to PostgreSQL queries transparently for example.

## Handling errors

Since plugins use the C ABI, PgDog is not able to catch panics inside plugins. Therefore, if a plugin panics, this will cause an abort and shutdown the pooler.

The vast majority of the Rust standard library and crates avoid panicking and return errors instead. Plugin code must check for error conditions and handle them internally. Notably, don't use `unwrap()` on `Option` or `Result` types and handle each case instead.

!!! note
    Better error handling is on the roadmap, e.g. by using macros
    that wrap plugin code into a panic handler. That being said, since
    plugins do expose `extern "C"` functions, this limitation should be
    explicitely stated to plugin authors.

## Learn more

PgDog plugins are in their infancy and many more features will be added over time. For now, the API
is pretty bare bones but can already do useful things. Our bundled plugin we use for routing is called
[pgdog-routing](https://github.com/levkk/pgdog/tree/main/plugins/pgdog-routing) and it can be used
as the basis for your plugin development.
