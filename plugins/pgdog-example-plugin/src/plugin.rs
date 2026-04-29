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
    if params.is_empty() {
        // No params bound.
    } else {
        let param = params
            .first()
            .and_then(|p| p.decode(params.parameter_format(0)));
        if let Some(param) = param {
            println!("Decoded parameter 0 ($1): {:?}", param);
        }
    }

    // Let PgDog decide.
    Ok(Route::unknown())
}

/// Route a COPY row to the correct shard.
///
/// Finds the "id" column and hashes its value to pick a shard.
pub(crate) fn route_copy(row: PdCopyRow) -> Result<Route, PluginError> {
    let columns = row.columns();
    let shards = row.shards() as usize;
    let record = row.record();

    if columns.is_empty() {
        return Ok(Route::unknown());
    }

    // Find the position of the "id" column.
    let id_pos = match columns.iter().position(|&c| c == "id") {
        Some(pos) => pos,
        None => return Ok(Route::unknown()),
    };

    let field = match record.get(id_pos) {
        Some(f) => f,
        None => return Ok(Route::unknown()),
    };

    // NULL values go to all shards.
    if field == record.null_string {
        return Ok(Route::new(Shard::All, ReadWrite::Write));
    }

    // Parse the id and hash to a shard.
    let id: i64 = match field.parse() {
        Ok(v) => v,
        Err(_) => return Ok(Route::unknown()),
    };

    println!(
        "copy decoded row with id {} (table={}.{})",
        id,
        row.schema_name(),
        row.table_name()
    );

    let shard = (id.unsigned_abs() as usize) % shards;
    Ok(Route::new(Shard::Direct(shard), ReadWrite::Write))
}

#[cfg(test)]
mod test {
    use pgdog_plugin::{PdParameters, PdStatement, PdStr};

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
            params: PdParameters::default(),
        };
        let route = route_query(context.into()).unwrap();
        let read_write: ReadWrite = route.read_write.try_into().unwrap();
        let shard: Shard = route.shard.try_into().unwrap();

        assert_eq!(read_write, ReadWrite::Read);
        assert_eq!(shard, Shard::Unknown);
    }

    #[test]
    fn test_copy_routes_by_id() {
        let columns = [PdStr::from("id"), PdStr::from("name")];

        // "7" + "Alice" concatenated, ends at [1, 6]
        let record = Record::new(b"7Alice", &[1, 6], '\t', CopyFormat::Text, "\\N");
        let row = PdCopyRow::from_proto(
            4,
            &record,
            &columns,
            &PdStr::from("users"),
            &PdStr::from("public"),
        );
        let route = route_copy(row).unwrap();
        assert_eq!(route.shard.try_into(), Ok(Shard::Direct(3))); // 7 % 4 = 3

        // "0" + "Bob" concatenated, ends at [1, 4]
        let record = Record::new(b"0Bob", &[1, 4], '\t', CopyFormat::Text, "\\N");
        let row = PdCopyRow::from_proto(
            4,
            &record,
            &columns,
            &PdStr::from("users"),
            &PdStr::from("public"),
        );
        let route = route_copy(row).unwrap();
        assert_eq!(route.shard.try_into(), Ok(Shard::Direct(0))); // 0 % 4 = 0
    }

    #[test]
    fn test_copy_null_id_routes_to_all() {
        let columns = [PdStr::from("id"), PdStr::from("name")];

        let record = Record::new(b"\\NAlice", &[2, 7], '\t', CopyFormat::Text, "\\N");
        let row = PdCopyRow::from_proto(
            4,
            &record,
            &columns,
            &PdStr::from("users"),
            &PdStr::from("public"),
        );
        let route = route_copy(row).unwrap();
        assert_eq!(route.shard.try_into(), Ok(Shard::All));
    }

    #[test]
    fn test_copy_csv_delimiter() {
        let columns = [PdStr::from("id"), PdStr::from("name")];

        let record = Record::new(b"5Charlie", &[1, 8], ',', CopyFormat::Csv, "\\N");
        let row = PdCopyRow::from_proto(
            3,
            &record,
            &columns,
            &PdStr::from("users"),
            &PdStr::from("public"),
        );
        let route = route_copy(row).unwrap();
        assert_eq!(route.shard.try_into(), Ok(Shard::Direct(2))); // 5 % 3 = 2
    }

    #[test]
    fn test_copy_no_id_column() {
        let columns = [PdStr::from("name"), PdStr::from("email")];

        let record = Record::new(
            b"Alicealice@test.com",
            &[5, 19],
            '\t',
            CopyFormat::Text,
            "\\N",
        );
        let row = PdCopyRow::from_proto(
            4,
            &record,
            &columns,
            &PdStr::from("users"),
            &PdStr::from("public"),
        );
        let route = route_copy(row).unwrap();
        assert_eq!(route.shard.try_into(), Ok(Shard::Unknown));
    }
}
