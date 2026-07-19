use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use once_cell::sync::Lazy;
use parking_lot::Mutex;
#[cfg(not(feature = "new_parser"))]
use pg_query::{NodeEnum, protobuf::RangeVar};
#[cfg(feature = "new_parser")]
use pg_raw_parse::Node;
use pgdog_plugin::prelude::*;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PluginError {
    #[error("{0}")]
    #[cfg(not(feature = "new_parser"))]
    PgQuery(#[from] pg_query::Error),

    #[error("{0}")]
    #[cfg(feature = "pg_raw_parse")]
    Parser(#[from] pg_raw_parse::Error),

    #[error("empty query")]
    EmptyQuery,
}

static WRITE_TIMES: Lazy<Mutex<HashMap<String, Instant>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

/// Route query to a replica or a primary, depending on when was the last time
/// we wrote to the table.
#[cfg(feature = "new_parser")]
pub(crate) fn route_query(context: Context<'_>) -> Result<Route, PluginError> {
    // PgDog really thinks this should be a write.
    // This could be because there is an INSERT statement in a CTE,
    // or something else. You could override its decision here, but make
    // sure you checked the AST first.
    let write_override = context.write_override();

    let root = context
        .query
        .stmts()
        .next()
        .ok_or(PluginError::EmptyQuery)?;

    match root {
        Node::SelectStmt(stmt) => {
            if write_override {
                return Ok(Route::unknown());
            }

            let table_name = stmt.from_clause().first().ok_or(PluginError::EmptyQuery)?;

            if let Node::RangeVar(range_var) = table_name {
                // Got info on last write.
                if let Some(last_write) = {
                    WRITE_TIMES
                        .lock()
                        .get(range_var.relname().unwrap())
                        .cloned()
                } {
                    if last_write.elapsed() > Duration::from_secs(5) && context.has_replicas() {
                        return Ok(Route::new(Shard::Unknown, ReadWrite::Read));
                    }
                } else if context.has_replicas() {
                    return Ok(Route::new(Shard::Unknown, ReadWrite::Read));
                }
            }
        }
        Node::InsertStmt(stmt) => {
            if let Some(relation) = stmt.relation() {
                WRITE_TIMES
                    .lock()
                    .insert(relation.relname().unwrap().to_owned(), Instant::now());
            }
        }
        Node::UpdateStmt(stmt) => {
            if let Some(relation) = stmt.relation() {
                WRITE_TIMES
                    .lock()
                    .insert(relation.relname().unwrap().to_owned(), Instant::now());
            }
        }
        Node::DeleteStmt(stmt) => {
            if let Some(relation) = stmt.relation() {
                WRITE_TIMES
                    .lock()
                    .insert(relation.relname().unwrap().to_owned(), Instant::now());
            }
        }
        _ => {}
    }

    // Get prepared statement parameters.
    let params = context.parameters();
    if params.parameters.is_empty() {
        // No params bound.
    } else {
        let param = params
            .parameters
            .first()
            .and_then(|p| p.decode(params.parameter_format(0)));
        if let Some(param) = param {
            println!("Decoded parameter 0 ($1): {:?}", param);
        }
    }

    // Let PgDog decide.
    Ok(Route::unknown())
}

cfg_select! {
    not(feature = "new_parser") => {
        pub(crate) fn route_query(context: Context<'_>) -> Result<Route, PluginError> {
            // PgDog really thinks this should be a write.
            // This could be because there is an INSERT statement in a CTE,
            // or something else. You could override its decision here, but make
            // sure you checked the AST first.
            let write_override = context.write_override();

            let root = context
                .query
                .stmts
                .first()
                .ok_or(PluginError::EmptyQuery)?
                .stmt
                .as_ref()
                .ok_or(PluginError::EmptyQuery)?;

            match root.node.as_ref() {
                Some(NodeEnum::SelectStmt(stmt)) => {
                    if write_override {
                        return Ok(Route::unknown());
                    }

                    let table_name = stmt
                        .from_clause
                        .first()
                        .ok_or(PluginError::EmptyQuery)?
                        .node
                        .as_ref()
                        .ok_or(PluginError::EmptyQuery)?;

                    if let NodeEnum::RangeVar(RangeVar { relname, .. }) = table_name {
                        // Got info on last write.
                        if let Some(last_write) = { WRITE_TIMES.lock().get(relname).cloned() } {
                            if last_write.elapsed() > Duration::from_secs(5) && context.has_replicas() {
                                return Ok(Route::new(Shard::Unknown, ReadWrite::Read));
                            }
                        } else if context.has_replicas() {
                            return Ok(Route::new(Shard::Unknown, ReadWrite::Read));
                        }
                    }
                }
                Some(NodeEnum::InsertStmt(stmt)) => {
                    if let Some(ref relation) = stmt.relation {
                        WRITE_TIMES
                            .lock()
                            .insert(relation.relname.clone(), Instant::now());
                    }
                }
                Some(NodeEnum::UpdateStmt(stmt)) => {
                    if let Some(ref relation) = stmt.relation {
                        WRITE_TIMES
                            .lock()
                            .insert(relation.relname.clone(), Instant::now());
                    }
                }
                Some(NodeEnum::DeleteStmt(stmt)) => {
                    if let Some(ref relation) = stmt.relation {
                        WRITE_TIMES
                            .lock()
                            .insert(relation.relname.clone(), Instant::now());
                    }
                }
                _ => {}
            }

            // Get prepared statement parameters.
            let params = context.parameters();
            if params.parameters.is_empty() {
                // No params bound.
            } else {
                let param = params
                    .parameters
                    .first()
                    .and_then(|p| p.decode(params.parameter_format(0)));
                if let Some(param) = param {
                    println!("Decoded parameter 0 ($1): {:?}", param);
                }
            }

            // Let PgDog decide.
            Ok(Route::unknown())
        }
    }
    _ => {}
}

#[cfg(test)]
mod test {
    use pgdog_plugin::parameters::Parameters;

    use super::*;

    #[test]
    fn test_routing_plugin() {
        // Keep protobuf in memory.
        #[cfg(not(feature = "new_parser"))]
        let query = pg_query::parse("SELECT * FROM users").unwrap().protobuf;
        #[cfg(feature = "new_parser")]
        let query = pg_raw_parse::parse("SELECT * FROM users").unwrap();
        let context = pgdog_plugin::Context {
            shards: 1,
            has_replicas: true,
            has_primary: true,
            in_transaction: false,
            write_override: false,
            query: &query,
            params: Parameters::default(),
        };
        let route = route_query(context).unwrap();
        let read_write: ReadWrite = route.read_write.try_into().unwrap();
        let shard: Shard = route.shard.try_into().unwrap();

        assert_eq!(read_write, ReadWrite::Read);
        assert_eq!(shard, Shard::Unknown);
    }
}
