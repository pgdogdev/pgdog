//! Recording of writes to sharding key lookup tables.
//!
//! The query engine flushes the recorded tables from the lookup cache
//! when the write completes.

use pgdog_config::{DataType, SystemCatalogsBehavior};

use super::*;
use crate::backend::{ShardedTables, ShardingSchema};
use crate::frontend::router::sharding::ShardedTable;

fn test_schema() -> ShardingSchema {
    ShardingSchema {
        shards: 3,
        tables: ShardedTables::new(
            vec![ShardedTable {
                column: "org_id".into(),
                name: Some("sharded_lookup_flush".into()),
                data_type: DataType::Varchar,
                lookup: Some("org_family_roots_flush".into()),
                query: Some(
                    "SELECT root_org_id FROM org_family_roots_flush WHERE org_id = $1".into(),
                ),
                ..Default::default()
            }],
            vec![],
            false,
            SystemCatalogsBehavior::default(),
        ),
        ..Default::default()
    }
}

fn written_lookups(stmt: &str, schema: &ShardingSchema) -> Vec<std::string::String> {
    #[cfg(not(feature = "new_parser"))]
    let raw = pg_query::parse(stmt).unwrap();
    #[cfg(not(feature = "new_parser"))]
    let update = match raw
        .protobuf
        .stmts
        .first()
        .and_then(|stmt| stmt.stmt.as_ref())
        .and_then(|node| node.node.as_ref())
        .unwrap()
    {
        NodeEnum::UpdateStmt(update) => update,
        _ => panic!("expected update stmt"),
    };
    #[cfg(feature = "new_parser")]
    let raw = pg_raw_parse::parse(stmt).unwrap();
    #[cfg(feature = "new_parser")]
    let update = raw.stmts().next().unwrap();

    let mut parser = StatementParser::from_update(update, None, schema, None);
    let mut written = Vec::new();
    QueryParser::record_lookup_writes(&mut parser, schema, &mut written);
    written
}

#[test]
fn test_write_to_lookup_table_recorded() {
    let schema = test_schema();
    let written = written_lookups(
        "UPDATE org_family_roots_flush SET root_org_id = 'new_root' WHERE org_id = 'org_child_flush'",
        &schema,
    );

    assert_eq!(written, vec!["org_family_roots_flush".to_string()]);
}

#[test]
fn test_write_to_other_table_not_recorded() {
    let schema = test_schema();
    let written = written_lookups(
        "UPDATE some_other_table SET value = 'new' WHERE id = 1",
        &schema,
    );

    assert!(written.is_empty());
}
