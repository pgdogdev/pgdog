use super::setup::*;
use crate::backend::ShardedTables;
use crate::frontend::router::{
    parser::Shard,
    sharding::{Mapping, ShardedTable},
};
use crate::net::messages::bind::Parameter;
use pgdog_config::{DataType, FlexibleType, ShardedMappingConfig, ShardedMappingRange};

fn joined_tables() -> QueryParserTest {
    let tables = [("ol", "ol_w_id"), ("st", "s_w_id")]
        .into_iter()
        .map(|(name, column)| ShardedTable {
            name: Some(name.into()),
            column: column.into(),
            data_type: DataType::Bigint,
            mapping: Mapping::new(vec![
                ShardedMappingConfig::Range(ShardedMappingRange {
                    start: Some(FlexibleType::Integer(1)),
                    end: Some(FlexibleType::Integer(101)),
                    shard: 0,
                }),
                ShardedMappingConfig::Range(ShardedMappingRange {
                    start: Some(FlexibleType::Integer(101)),
                    end: None,
                    shard: 1,
                }),
            ]),
            ..Default::default()
        })
        .collect();

    QueryParserTest::new()
        .without_sharded_schemas()
        .with_sharded_tables(ShardedTables::new(
            tables,
            vec![],
            false,
            Default::default(),
        ))
}

#[test]
fn test_unqualified_join_sharding_columns() {
    let mut test = joined_tables();
    for key in [3, 103] {
        for sql in [
            format!(
                "SELECT COUNT(DISTINCT s_i_id) FROM ol, st \
                 WHERE ol_w_id = {key} AND ol_d_id = 1 \
                 AND ol_o_id < 3001 AND ol_o_id >= 2981 \
                 AND s_w_id = {key} AND s_i_id = ol_i_id AND s_quantity < 15"
            ),
            format!(
                "SELECT COUNT(DISTINCT s_i_id) FROM ol JOIN st \
                 ON s_w_id = ol_w_id AND s_i_id = ol_i_id WHERE ol_w_id = {key}"
            ),
            format!(
                "SELECT COUNT(DISTINCT s_i_id) FROM ol o JOIN st s \
                 ON s.s_w_id = o.ol_w_id WHERE s_w_id = {key}"
            ),
            format!("SELECT * FROM ol LEFT JOIN st ON s_w_id = ol_w_id WHERE ol_w_id = {key}"),
            format!("SELECT * FROM ol o, st ol WHERE ol_w_id = {key}"),
        ] {
            let command = test.execute(vec![Query::new(&sql).into()]);
            assert_eq!(
                command.route().shard(),
                &Shard::Direct(usize::from(key >= 101)),
                "{sql}"
            );
        }
    }
}

#[test]
fn test_unqualified_join_bound_sharding_column() {
    let mut test = joined_tables();
    for (value, shard) in [("3", 0), ("103", 1)] {
        let command = test.execute(vec![
            Parse::named(
                "__join_key",
                "SELECT COUNT(DISTINCT s_i_id) FROM ol JOIN st \
                 ON s_w_id = ol_w_id WHERE ol_w_id = $1",
            )
            .into(),
            Bind::new_params("__join_key", &[Parameter::new(value.as_bytes())]).into(),
            Execute::new().into(),
            Sync.into(),
        ]);
        assert_eq!(command.route().shard(), &Shard::Direct(shard));
    }
}

#[test]
fn test_unqualified_join_key_requires_unique_table_in_scope() {
    let mut test = joined_tables();
    for sql in [
        // The configured owner of ol_w_id is absent from this query scope.
        "SELECT * FROM unrelated, st WHERE ol_w_id = 3",
        "SELECT * FROM (SELECT * FROM ol) o, st WHERE ol_w_id = 3",
        // Both occurrences of ol are candidates for the bare column.
        "SELECT * FROM ol a, ol b WHERE ol_w_id = 3",
        // A table alias isn't a reference to the configured table.
        "SELECT * FROM unrelated ol, st WHERE ol_w_id = 3",
        // A CTE named ol need not expose the real table's sharding column.
        "WITH ol AS (SELECT s_i_id AS ol_w_id FROM st) \
         SELECT * FROM ol, st WHERE ol_w_id = 3",
        // ON doesn't restrict the preserved rows of an outer join.
        "SELECT * FROM ol LEFT JOIN st ON s_w_id = 3 AND s_w_id = ol_w_id",
        "SELECT * FROM ol RIGHT JOIN st ON ol_w_id = 3 AND s_w_id = ol_w_id",
        "SELECT * FROM ol FULL JOIN st ON ol_w_id = 3 AND s_w_id = ol_w_id",
        // OR still cannot pin the query to a single shard.
        "SELECT * FROM ol, st WHERE ol_w_id = 3 OR s_w_id = 103",
    ] {
        let command = test.execute(vec![Query::new(sql).into()]);
        assert_eq!(command.route().shard(), &Shard::All, "{sql}");
    }
}
