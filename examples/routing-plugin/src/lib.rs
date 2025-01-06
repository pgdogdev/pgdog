//! Simple routing plugin example using Rust.
use pgdog_plugin::{
    bindings::{self, Shard_ANY},
    Query, Route,
};

/// Route query.
#[no_mangle]
pub extern "C" fn pgdog_route_query(query: bindings::Query) -> Route {
    let query = Query::from(query);
    if query.query().to_lowercase().starts_with("select") {
        Route {
            shard: Shard_ANY, // Any shard.
            affinity: bindings::Affinity_READ,
        }
    } else {
        Route {
            shard: Shard_ANY, // Any shard.
            affinity: bindings::Affinity_WRITE,
        }
    }
}
