use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use pg_query::{NodeEnum, protobuf::RangeVar};
use pgdog_plugin::{PdRoute, PdRouterContext, ReadWrite, Shard};
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

/// Perform any plugin initialization routines here.
/// These are running sync on boot, so make these fast.
#[unsafe(no_mangle)]
pub extern "C" fn pgdog_init() {}

/// Called on every single query going through PgDog.
#[unsafe(no_mangle)]
pub extern "C" fn pgdog_route(context: PdRouterContext) -> PdRoute {
    // This function can't return errors or panic.

    route_query(context).unwrap_or(PdRoute::unknown())
}

/// Route query to a replica or a primary, depending on when was the last time
/// we wrote to the table.
fn route_query(context: PdRouterContext) -> Result<PdRoute, PluginError> {
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
            let table_name = stmt
                .from_clause
                .first()
                .ok_or(PluginError::EmptyQuery)?
                .node
                .as_ref()
                .ok_or(PluginError::EmptyQuery)?;
            match table_name {
                NodeEnum::RangeVar(RangeVar { relname, .. }) => {
                    // Got info on last write.
                    if let Some(last_write) = { WRITE_TIMES.lock().get(relname).cloned() } {
                        if last_write.elapsed() > Duration::from_secs(5) {
                            if context.has_replicas() {
                                return Ok(PdRoute::new(Shard::Unknown, ReadWrite::Read));
                            }
                        }
                    } else {
                        if context.has_replicas() {
                            // Don't have it, assume we're good.
                            return Ok(PdRoute::new(Shard::Unknown, ReadWrite::Read));
                        }
                    }
                }

                _ => (),
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

    Ok(PdRoute::new(Shard::Unknown, ReadWrite::Write))
}

#[cfg(test)]
mod test {
    use pgdog_plugin::PdQuery;

    use super::*;

    #[test]
    fn test_routing_plugin() {
        // Keep protobuf in memory.
        let proto = pg_query::parse("SELECT * FROM users").unwrap().protobuf;
        let query = unsafe { PdQuery::from_proto(&proto) };
        let context = PdRouterContext {
            shards: 1,
            has_replicas: 1,
            has_primary: 1,
            in_transaction: 0,
            query,
        };
        let route = route_query(context).unwrap();
        let read_write: ReadWrite = route.read_write.try_into().unwrap();
        let shard: Shard = route.shard.try_into().unwrap();

        assert_eq!(read_write, ReadWrite::Read);
        assert_eq!(shard, Shard::Unknown);
    }
}
