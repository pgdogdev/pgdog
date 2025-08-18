
#include <stddef.h>
#include <stdint.h>

/**
 * Wrapper around Rust's [`&str`], without allocating memory, unlike [`std::ffi::CString`].
 * The caller must use it as a Rust string. This is not a C-string.
 */
typedef struct PdStr {
    size_t len;
    void *data;
} RustString;

/*
 * Wrapper around output by pg_query.
 */
typedef struct PdStatement {
    int32_t version;
    uint64_t len;
    void *data;
} PdStatement;

/**
 * Context on the database cluster configuration and the currently processed
 * PostgreSQL statement.
 *
 * This struct is C FFI-safe and therefore uses C types. Use public methods to interact with it instead
 * of reading the data directly.
 */
typedef struct PdRouterContext {
    /** How many shards are configured. */
    uint64_t shards;
    /** Does the database cluster have replicas? `1` = `true`, `0` = `false`. */
    uint8_t has_replicas;
    /** Does the database cluster have a primary? `1` = `true`, `0` = `false`. */
    uint8_t has_primary;
    /** Is the query being executed inside a transaction? `1` = `true`, `0` = `false`. */
    uint8_t in_transaction;
    /** PgDog strongly believes this statement should go to a primary. `1` = `true`, `0` = `false`. */
    uint8_t write_override;
    /** pg_query generated Abstract Syntax Tree of the statement. */
    PdStatement query;
} PdRouterContext;

/**
 * Routing decision returned by the plugin.
 */
 typedef struct PdRoute {
     /** Which shard the query should go to.
      *
      * `-1` for all shards, `-2` for unknown, this setting is ignored.
      */
     int64_t shard;
     /** Is the query a read and should go to a replica?
      *
      * `1` for `true`, `0` for `false`, `2` for unknown, this setting is ignored.
      */
     uint8_t read_write;
 } PdRoute;
