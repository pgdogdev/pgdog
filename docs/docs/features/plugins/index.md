# Plugins overview

One of features that make PgDog particularly powerful is its plugin system. Users of PgDog can write plugins
in any language and inject them inside the query router to direct query traffic, to rewrite queries, or to block
them entirely and return custom results.

## API

PgDog plugins are shared libraries loaded at application startup. They can be written in any programming language, as long
as that language can be compiled to a shared library, and can expose a predefined set of C ABI-compatible functions.

### Functions

#### `pgdog_init`

This function is executed once when PgDog loads the plugin, at application startup. It allows to initialize any
kind of internal plugin state. Execution of this function is synchronized, so it's safe to execute any thread-unsafe
functions or initialize synchronization primitives, like mutexes.


This function has the following signature:

=== "Rust"
    ```rust
    pub extern "C" fn pgdog_init() {}
    ```
=== "C/C++"
    ```c
    void pgdog_init();
    ```


#### `pgdog_route_query`

This function is called every time the query router sees a new query and needs to figure out
where this query should be sent. The query text and parameters will be provided and the router
expects the plugin to parse the query and provide a route.

This function has the following signature:

=== "Rust"
    ```rust
    use pgdog_plugin::*;

    pub extern "C" fn pgdog_route_query(Input query) -> Output {
        Route::unknown()
    }
    ```
=== "C/C++"
    ```c
    Output pgdog_route_query(Input query);
    ```


##### Data structures

This function expects an input of type `Input` and must return a struct of type `Output`. The input contains
the query PgDog received and the current database configuration, e.g. number of shards, replicas, and if there
is a primary database that can serve writes.

The output structure contains the routing decision (e.g. query should go to a replica) and any additional information that the plugin wants to communicate, which depends on the routing decision. For example,
if the plugin wants PgDog to intercept this query and return a custom result, rows of that result will be
included in the output.


#### `pgdog_fini`

This function is called before the pooler is shut down. This allows plugins to perform any tasks, like saving
some internal state to a durable medium.

This function has the following signature:

=== "Rust"
    ```rust
    pub extern "C" fn pgdog_fini() {}
    ```
=== "C/C++"
    ```c
    void pgdog_fini();
    ```

## Examples

Example plugins written in Rust and C are
included in [GitHub](https://github.com/levkk/pgdog/tree/main/examples).

## Learn more

- [Plugins in Rust](rust.md)
- [Plugins in C](c.md)
