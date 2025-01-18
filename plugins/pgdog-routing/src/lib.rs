//! Parse queries using pg_query and route all SELECT queries
//! to replicas. All other queries are routed to a primary.

use once_cell::sync::Lazy;
use pg_query::{
    parse,
    protobuf::{a_const::Val, SelectStmt},
    NodeEnum,
};
use pgdog_plugin::{
    bindings::{Config, Input, Output},
    OrderByDirection_ASCENDING, OrderByDirection_DESCENDING,
};
use pgdog_plugin::{OrderBy, Route};

use tracing::trace;
use tracing::{debug, level_filters::LevelFilter};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use std::io::IsTerminal;
use std::sync::atomic::{AtomicUsize, Ordering};

static SHARD_ROUND_ROBIN: Lazy<AtomicUsize> = Lazy::new(|| AtomicUsize::new(0));

#[no_mangle]
pub extern "C" fn pgdog_init() {
    let format = fmt::layer()
        .with_ansi(std::io::stderr().is_terminal())
        .with_file(false);

    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();

    tracing_subscriber::registry()
        .with(format)
        .with(filter)
        .init();

    // TODO: This is more for fun/demo, but in prod, we want
    // this logger to respect options passed to pgDog proper, e.g.
    // use JSON output.
    debug!("🐕 pgDog routing plugin v{}", env!("CARGO_PKG_VERSION"));
}

#[no_mangle]
pub extern "C" fn pgdog_route_query(input: Input) -> Output {
    if let Some(query) = input.query() {
        let query = query;
        let route = match route_internal(query.query(), input.config) {
            Ok(route) => route,
            Err(_) => Route::unknown(),
        };
        Output::forward(route)
    } else {
        Output::skip()
    }
}

fn route_internal(query: &str, config: Config) -> Result<Route, pg_query::Error> {
    let ast = parse(query)?;
    let shards = config.shards;
    let databases = config.databases();

    // Shortcut for typical single shard replicas-only/primary-only deployments.
    if shards == 1 {
        let read_only = databases.iter().all(|d| d.replica());
        let write_only = databases.iter().all(|d| d.primary());
        if read_only {
            return Ok(Route::read(0));
        }
        if write_only {
            return Ok(Route::read(0));
        }
    }

    for database in databases {
        debug!(
            "{}:{} [shard: {}][role: {}]",
            database.host(),
            database.port(),
            database.shard(),
            if database.replica() {
                "replica"
            } else {
                "primary"
            }
        );
    }

    trace!("{:#?}", ast);

    // For cases like SELECT NOW(), or SELECT 1, etc.
    let tables = ast.tables();
    if tables.is_empty() {
        // Better than random for load distribution.
        let shard_counter = SHARD_ROUND_ROBIN.fetch_add(1, Ordering::Relaxed);
        return Ok(Route::read(shard_counter % shards as usize));
    }

    if let Some(query) = ast.protobuf.stmts.first() {
        if let Some(ref node) = query.stmt {
            match node.node {
                Some(NodeEnum::SelectStmt(ref stmt)) => {
                    let order_by = order_by(stmt)?;
                    let mut route = if shards == 1 {
                        Route::read(0)
                    } else {
                        Route::read_all()
                    };

                    if !order_by.is_empty() {
                        route.order_by(&order_by);
                    }

                    return Ok(route);
                }

                Some(_) => (),

                None => (),
            }
        }
    }

    Ok(if shards == 1 {
        Route::write(0)
    } else {
        Route::write_all()
    })
}

fn order_by(stmt: &SelectStmt) -> Result<Vec<OrderBy>, pg_query::Error> {
    let mut order_by = vec![];
    for clause in &stmt.sort_clause {
        if let Some(ref node) = clause.node {
            if let NodeEnum::SortBy(sort_by) = node {
                let asc = match sort_by.sortby_dir {
                    0..=2 => true,
                    _ => false,
                };
                if let Some(ref node) = sort_by.node {
                    if let Some(ref node) = node.node {
                        match node {
                            NodeEnum::AConst(aconst) => {
                                if let Some(ref val) = aconst.val {
                                    if let Val::Ival(integer) = val {
                                        order_by.push(OrderBy::column_index(
                                            integer.ival as usize,
                                            if asc {
                                                OrderByDirection_ASCENDING
                                            } else {
                                                OrderByDirection_DESCENDING
                                            },
                                        ));
                                    }
                                }
                            }

                            NodeEnum::ColumnRef(column_ref) => {
                                if let Some(field) = column_ref.fields.first() {
                                    if let Some(ref node) = field.node {
                                        if let NodeEnum::String(string) = node {
                                            order_by.push(OrderBy::column_name(
                                                &string.sval,
                                                if asc {
                                                    OrderByDirection_ASCENDING
                                                } else {
                                                    OrderByDirection_DESCENDING
                                                },
                                            ));
                                        }
                                    }
                                }
                            }
                            _ => (),
                        }
                    }
                }
            }
        }
    }
    Ok(order_by)
}

#[no_mangle]
pub extern "C" fn pgdog_fini() {
    debug!(
        "🐕 pgDog routing plugin v{} shutting down",
        env!("CARGO_PKG_VERSION")
    );
}
