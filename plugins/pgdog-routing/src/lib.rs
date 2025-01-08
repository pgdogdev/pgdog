//! Parse queries using pg_query and route all SELECT queries
//! to replicas. All other queries are routed to a primary.

use pg_query::{parse, NodeEnum};
use pgdog_plugin::bindings::{Output, Shard_ANY};
use pgdog_plugin::{bindings, Affinity_READ, Affinity_WRITE};
use pgdog_plugin::{Query, Route};

#[no_mangle]
pub extern "C" fn pgdog_route_query(query: bindings::Query) -> Output {
    let query = Query::from(query);
    let route = match route_internal(query.query()) {
        Ok(route) => route,
        Err(_) => Route::unknown(),
    };

    Output::forward(route)
}

fn route_internal(query: &str) -> Result<Route, pg_query::Error> {
    let ast = parse(query)?;

    if let Some(query) = ast.protobuf.stmts.first() {
        if let Some(ref node) = query.stmt {
            match node.node {
                Some(NodeEnum::SelectStmt(ref _stmt)) => {
                    return Ok(Route {
                        affinity: Affinity_READ,
                        shard: Shard_ANY,
                    });
                }

                Some(_) => (),

                None => (),
            }
        }
    }

    Ok(Route {
        affinity: Affinity_WRITE,
        shard: Shard_ANY,
    })
}
