use pgdog_plugin::{bindings, Query, Route};

/// Route query.
#[no_mangle]
pub unsafe extern "C" fn pgdog_route_query(query: bindings::Query) -> Route {
    let query = Query::from(query);
    if query.query().to_lowercase().starts_with("select") {
        Route {
            shard: -1, // Any shard.
            affinity: bindings::Affinity_READ,
        }
    } else {
        Route {
            shard: -1, // Any shard.
            affinity: bindings::Affinity_WRITE,
        }
    }
}
