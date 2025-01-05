use pgdog_plugin::{bindings, Affinity_READ, Affinity_WRITE, Query, Route};

/// Route query.
#[no_mangle]
pub unsafe extern "C" fn route(query: bindings::Query) -> Route {
    let query = Query::from(query);
    if query.query().to_lowercase().starts_with("select") {
        Route {
            shard: -1,
            affinity: Affinity_READ,
        }
    } else {
        Route {
            shard: -1,
            affinity: Affinity_WRITE,
        }
    }
}
