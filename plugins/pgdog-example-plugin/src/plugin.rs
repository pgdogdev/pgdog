use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use pg_query::{NodeEnum, protobuf::RangeVar};
use pgdog_plugin::prelude::*;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PluginError {
    #[error("{0}")]
    PgQuery(#[from] pg_query::Error),

    #[error("empty query")]
    EmptyQuery,
}

static WRITE_TIMES: Lazy<Mutex<HashMap<String, Instant>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

/// Route query to a replica or a primary, depending on when was the last time
/// we wrote to the table.
pub(crate) fn route_query(context: Context) -> Result<Route, PluginError> {
    // PgDog really thinks this should be a write.
    // This could be because there is an INSERT statement in a CTE,
    // or something else. You could override its decision here, but make
    // sure you checked the AST first.
    let write_override = context.write_override();

    let proto = context.statement().protobuf();
    let root = proto
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
                if let Some(last_write) = { WRITE_TIMES.lock().get(relname).cloned() }
                    && last_write.elapsed() > Duration::from_secs(5)
                    && context.has_replicas()
                {
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

    // Let PgDog decide.
    Ok(Route::unknown())
}

#[cfg(test)]
mod test {
    use pgdog_plugin::PdStatement;

    use super::*;

    #[test]
    fn test_routing_plugin() {
        // Keep protobuf in memory.
        let proto = pg_query::parse("SELECT * FROM users").unwrap().protobuf;
        let query = unsafe { PdStatement::from_proto(&proto) };
        let context = pgdog_plugin::PdRouterContext {
            shards: 1,
            has_replicas: 1,
            has_primary: 1,
            in_transaction: 0,
            write_override: 0,
            query,
        };
        let route = route_query(context.into()).unwrap();
        let read_write: ReadWrite = route.read_write.try_into().unwrap();
        let shard: Shard = route.shard.try_into().unwrap();

        assert_eq!(read_write, ReadWrite::Read);
        assert_eq!(shard, Shard::Unknown);
    }
}
