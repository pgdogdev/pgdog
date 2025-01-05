use pgdog_plugin::{Affinity, Query, Route};

/// Route query.
#[no_mangle]
pub unsafe extern "C" fn route(query: Query) -> Route {
    let query = query.query();
    if query.to_lowercase().starts_with("select") {
        Route {
            shard: -1,
            affinity: Affinity::Read,
        }
    } else {
        Route {
            shard: -1,
            affinity: Affinity::Write,
        }
    }
}
