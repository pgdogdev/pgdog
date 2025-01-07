# Plugins

One of features that make pgDog particularly powerful is its plugin system. Users of pgDog can write plugins
in any language and inject them inside the query router to direct query traffic, to rewrite queries, or to block
them entirely and return a custom result.

## API

pgDog plugins are shared libraries loaded at application startup. They can be written in any programming language, as long
as that language can be compiled to a shared library, and can expose a predefined set of C ABI-compatible functions.

### Functions

#### `pgdog_init`

This function is executed once when pgDog loads the plugin, at application startup. It allows to initialize any
kind of internal plugin state. Execution of this function is synchronized, so it's safe to execute any thread-unsafe
functions or initialize synchronization primitives, like mutexes.


This function has the following signature:

=== "C/C++"
    ```c
    void pgdog_init();
    ```
=== "Rust"
    ```rust
    pub extern "C" fn pgdog_init() {}
    ```


#### `pgdog_route_query`

This function is called every time the query router sees a new query and needs to figure out
where this query should be sent. The query text and parameters will be provided and the router
expects the plugin to parse the query and provide a route.

This function has the following signature:

=== "C/C++"
    ```c
    Route pgdog_route_query(Query query);
    ```
=== "Rust"
    ```rust
    use pgdog_plugin::bindings;

    pub extern "C" fn pgdog_route_query(bindings::Query query) -> Route {
        Route::unknown()
    }
    ```

## Examples

Example plugins written in Rust and C are
included in [GitHub](https://github.com/levkk/pgdog/tree/main/examples).

## Learn more

- [Plugins in Rust](rust.md)
- [Plugins in C](c.md)
