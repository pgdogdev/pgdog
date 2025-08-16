#include <stddef.h>
#include <stdint.h>

/*
 * Rust string.
 */
typedef struct PdStr {
    size_t len;
    void *data;
} RustString;

/*
 * Wrapper around output by pg_query.
 */
typedef struct PdQuery {
    int32_t version;
    uint64_t len;
    void *data;
} PdQuery;

/*
 * Configuration.
 */
typedef struct PdRouterContext {
    /* How many shards are configured. */
    uint64_t shards;
    /* Does the cluster have replicas? */
    uint8_t has_replicas;
    /* Does the cluster have a primary? */
    uint8_t has_primary;
    /* Are we inside a transaction? */
    uint8_t in_transaction;
    /* Write override */
    uint8_t write_override;
    /* Query */
    PdQuery query;
} PdConfig;

/*
 * Routing decision.
 */
 typedef struct PdRoute {
     /* Which shard the query should go to. -1 for cross-shard */
     int64_t shard;
     /* Is the query a read? */
     uint8_t read_write;
 } PdRoute;
