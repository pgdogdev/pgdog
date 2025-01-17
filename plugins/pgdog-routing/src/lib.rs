//! Parse queries using pg_query and route all SELECT queries
//! to replicas. All other queries are routed to a primary.

use pg_query::{parse, NodeEnum};
use pgdog_plugin::bindings::{Config, Input, Output};
use pgdog_plugin::Route;

use tracing::trace;
use tracing::{debug, level_filters::LevelFilter};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use std::io::IsTerminal;

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

    for database in config.databases() {
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

    if let Some(query) = ast.protobuf.stmts.first() {
        if let Some(ref node) = query.stmt {
            match node.node {
                Some(NodeEnum::SelectStmt(ref _stmt)) => {
                    trace!("{:#?}", _stmt);
                    return Ok(if shards == 1 {
                        Route::read(0)
                    } else {
                        Route::read_all()
                    });
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

#[no_mangle]
pub extern "C" fn pgdog_fini() {
    debug!(
        "🐕 pgDog routing plugin v{} shutting down",
        env!("CARGO_PKG_VERSION")
    );
}
