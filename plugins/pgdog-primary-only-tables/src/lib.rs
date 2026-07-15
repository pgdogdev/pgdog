use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
};

use arc_swap::ArcSwap;
use once_cell::sync::Lazy;
use pgdog_plugin::{
    Config as PluginConfig, Context, PdStr, Plugin, ReadWrite, Route, Shard,
    pg_query::{NodeEnum, NodeRef},
};
use serde::{Deserialize, Serialize};
use tracing::{error, info};

static CONFIG: Lazy<ArcSwap<Config>> = Lazy::new(|| ArcSwap::from_pointee(Config::default()));

pgdog_plugin::plugin!(PrimaryOnlyTables);

struct PrimaryOnlyTables;

impl Plugin for PrimaryOnlyTables {
    extern "C-unwind" fn version() -> PdStr<'static> {
        env!("CARGO_PKG_VERSION").into()
    }

    fn route(context: Context<'_>) -> Route {
        route_query(context).unwrap_or(Route::unknown())
    }

    extern "C-unwind" fn config(config: PluginConfig<'_>) -> bool {
        let plugin_config = config.plugin_config;
        if plugin_config.is_empty() {
            return true;
        }

        let path = PathBuf::from(&*plugin_config);
        if let Err(err) = read_config(&path) {
            error!("[pgdog_primary_only_tables] failed to load config: {}", err);

            false
        } else {
            true
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

fn route_query(context: Context<'_>) -> Result<Route, Box<dyn std::error::Error>> {
    let ast = &context.query;

    let root_node = ast
        .stmts
        .first()
        .and_then(|s| s.stmt.as_ref())
        .and_then(|s| s.node.as_ref());

    let is_select = root_node.is_some_and(|node| match node {
        NodeEnum::SelectStmt(_) => true,
        NodeEnum::ExplainStmt(stmt) => stmt
            .query
            .as_ref()
            .and_then(|q| q.node.as_ref())
            .is_some_and(|n| matches!(n, NodeEnum::SelectStmt(_))),
        _ => false,
    });

    if !is_select {
        return Ok(Route::default());
    }

    let config = CONFIG.load().clone();

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
