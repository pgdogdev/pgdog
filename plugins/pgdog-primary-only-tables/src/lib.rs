use std::{
    fs::File,
    io::{IsTerminal, Read},
    path::{Path, PathBuf},
    sync::Arc,
};

use arc_swap::ArcSwap;
use once_cell::sync::Lazy;
use pgdog_plugin::{Context, PdStr, ReadWrite, Route, Shard, macros, pg_query::NodeRef};
use serde::{Deserialize, Serialize};
use tracing::{error, info, level_filters::LevelFilter};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

static CONFIG: Lazy<ArcSwap<Config>> = Lazy::new(|| ArcSwap::from_pointee(Config::default()));

macros::plugin!();

#[macros::init]
fn init() {
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();

    let format = fmt::layer()
        .with_ansi(std::io::stderr().is_terminal())
        .with_writer(std::io::stderr)
        .with_file(false);
    #[cfg(not(debug_assertions))]
    let format = format.with_target(false);

    let _ = tracing_subscriber::registry()
        .with(format)
        .with(filter)
        .try_init();
}

#[macros::route]
fn route(context: Context) -> Route {
    route_query(context).unwrap_or(Route::unknown())
}

#[macros::config]
fn config(config: PdStr, result: *mut u8) {
    let path = PathBuf::from(config.to_string());
    if let Err(err) = read_config(&path) {
        error!("[pgdog_primary_only_tables] failed to load config: {}", err);

        unsafe {
            *result = 1;
        }
    } else {
        unsafe {
            *result = 0;
        }
    }
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
struct Table {
    name: String,
    schema: Option<String>,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
struct Config {
    tables: Vec<Table>,
}

fn read_config(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = File::open(path)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;

    let config: Config = toml::from_str(&contents)?;

    info!(
        "[pgdog_primary_only_tables] loaded config \"{}\" with {} tables",
        path.display(),
        config.tables.len(),
    );

    CONFIG.store(Arc::new(config));

    Ok(())
}

fn route_query(context: Context) -> Result<Route, Box<dyn std::error::Error>> {
    let config = CONFIG.load().clone();
    let ast = context.statement().protobuf();

    for node in ast.nodes() {
        if let NodeRef::RangeVar(range_var) = node.0 {
            for table in &config.tables {
                let name_matches = table.name == range_var.relname;
                let schema_matches = match &table.schema {
                    Some(schema) => schema == &range_var.schemaname,
                    None => true,
                };

                if name_matches && schema_matches {
                    return Ok(Route::new(Shard::Unknown, ReadWrite::Write));
                }
            }
        }
    }

    Ok(Route::default())
}
