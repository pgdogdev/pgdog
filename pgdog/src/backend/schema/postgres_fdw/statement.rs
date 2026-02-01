//! CREATE FOREIGN TABLE statement generation.

use std::fmt::Write;

use crate::backend::replication::ShardedTables;
use crate::config::ShardedTable;
use crate::frontend::router::sharding::Mapping;

use super::ForeignTableColumn;

/// Partition strategy for a sharded table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionStrategy {
    Hash,
    List,
    Range,
}

impl PartitionStrategy {
    /// Determine partition strategy from sharded table config.
    fn from_sharded_table(table: &ShardedTable) -> Self {
        match &table.mapping {
            Some(Mapping::List(_)) => Self::List,
            Some(Mapping::Range(_)) => Self::Range,
            None => Self::Hash,
        }
    }

    /// SQL keyword for this partition strategy.
    fn as_sql(&self) -> &'static str {
        match self {
            Self::Hash => "HASH",
            Self::List => "LIST",
            Self::Range => "RANGE",
        }
    }
}

/// Quote an identifier if needed (simple Postgres-style quoting).
pub(crate) fn quote_identifier(name: &str) -> String {
    let needs_quoting = name.is_empty()
        || !name.starts_with(|c: char| c.is_ascii_lowercase() || c == '_')
        || name.starts_with('_') && name.chars().nth(1).is_some_and(|c| c.is_ascii_digit())
        || !name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');

    if needs_quoting {
        format!("\"{}\"", name.replace('"', "\"\""))
    } else {
        name.to_string()
    }
}

/// Escape a string literal for use in SQL.
fn escape_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Builder for CREATE FOREIGN TABLE statements.
pub struct ForeignTableBuilder<'a> {
    columns: &'a [ForeignTableColumn],
    server_name: &'a str,
    sharded_tables: &'a ShardedTables,
}

impl<'a> ForeignTableBuilder<'a> {
    /// Create a new builder with required parameters.
    pub fn new(
        columns: &'a [ForeignTableColumn],
        server_name: &'a str,
        sharded_tables: &'a ShardedTables,
    ) -> Self {
        Self {
            columns,
            server_name,
            sharded_tables,
        }
    }

    /// Find the sharding configuration for this table, if any.
    fn find_sharded_config(&self) -> Option<&ShardedTable> {
        let first = self.columns.first()?;
        let table_name = &first.table_name;
        let schema_name = &first.schema_name;

        for candidate in self.sharded_tables.tables() {
            // Match table name if specified
            if let Some(ref name) = candidate.name {
                if name != table_name {
                    continue;
                }
            }

            // Match schema if specified
            if let Some(ref schema) = candidate.schema {
                if schema != schema_name {
                    continue;
                }
            }

            // Check if the shard column exists in this table
            let has_column = self
                .columns
                .iter()
                .any(|col| col.column_name == candidate.column);

            if has_column {
                return Some(candidate);
            }
        }

        None
    }

    /// Build the CREATE FOREIGN TABLE statement.
    pub fn build(self) -> Option<String> {
        let first = self.columns.first()?;
        let schema_name = &first.schema_name;
        let table_name = &first.table_name;

        let mut sql = String::new();
        writeln!(
            sql,
            "CREATE FOREIGN TABLE {} (",
            quote_identifier(table_name)
        )
        .ok()?;

        let mut first_col = true;
        for col in self.columns {
            if col.column_name.is_empty() {
                continue;
            }

            if first_col {
                first_col = false;
            } else {
                sql.push_str(",\n");
            }

            write!(
                sql,
                "  {} {}",
                quote_identifier(&col.column_name),
                col.column_type
            )
            .ok()?;

            write!(
                sql,
                " OPTIONS (column_name {})",
                escape_literal(&col.column_name)
            )
            .ok()?;

            if col.has_collation() {
                write!(
                    sql,
                    " COLLATE {}.{}",
                    quote_identifier(&col.collation_schema),
                    quote_identifier(&col.collation_name)
                )
                .ok()?;
            }

            if !col.column_default.is_empty() {
                if col.is_generated() {
                    write!(sql, " GENERATED ALWAYS AS ({}) STORED", col.column_default).ok()?;
                } else {
                    write!(sql, " DEFAULT {}", col.column_default).ok()?;
                }
            }

            if col.is_not_null {
                sql.push_str(" NOT NULL");
            }
        }

        sql.push('\n');
        sql.push(')');

        // Add PARTITION BY clause if table is sharded
        if let Some(sharded) = self.find_sharded_config() {
            let strategy = PartitionStrategy::from_sharded_table(sharded);
            write!(
                sql,
                " PARTITION BY {} ({})",
                strategy.as_sql(),
                quote_identifier(&sharded.column)
            )
            .ok()?;
        }

        // Add SERVER and OPTIONS
        write!(
            sql,
            "\nSERVER {}\nOPTIONS (schema_name {}, table_name {})",
            quote_identifier(self.server_name),
            escape_literal(schema_name),
            escape_literal(table_name)
        )
        .ok()?;

        Some(sql)
    }
}

/// Generate a CREATE FOREIGN TABLE statement from column definitions.
///
/// All columns must belong to the same table. The server_name is the foreign server
/// to reference in the statement. If the table is found in sharded_tables configuration,
/// adds the appropriate PARTITION BY clause (HASH, LIST, or RANGE).
///
/// TODO: handle partitioned tables by creating the partitions
/// and sharding the child tables using our partition algorithm.
pub fn create_foreign_table(
    columns: &[ForeignTableColumn],
    server_name: &str,
    sharded_tables: &ShardedTables,
) -> Option<String> {
    ForeignTableBuilder::new(columns, server_name, sharded_tables).build()
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use super::*;
    use crate::config::{DataType, FlexibleType, ShardedMapping, ShardedMappingKind};

    fn test_column(name: &str, col_type: &str) -> ForeignTableColumn {
        ForeignTableColumn {
            schema_name: "public".into(),
            table_name: "test_table".into(),
            column_name: name.into(),
            column_type: col_type.into(),
            is_not_null: false,
            column_default: String::new(),
            generated: String::new(),
            collation_name: String::new(),
            collation_schema: String::new(),
        }
    }

    fn test_sharded_table(table: &str, column: &str) -> ShardedTable {
        ShardedTable {
            database: "test".into(),
            name: Some(table.into()),
            schema: Some("public".into()),
            column: column.into(),
            primary: false,
            centroids: vec![],
            centroids_path: None,
            data_type: DataType::Bigint,
            centroid_probes: 0,
            hasher: Default::default(),
            mapping: None,
        }
    }

    fn test_sharded_table_with_mapping(
        table: &str,
        column: &str,
        mapping: Mapping,
    ) -> ShardedTable {
        ShardedTable {
            mapping: Some(mapping),
            ..test_sharded_table(table, column)
        }
    }

    fn list_mapping() -> Mapping {
        let mapping = ShardedMapping {
            database: "test".into(),
            column: "region".into(),
            table: Some("test_table".into()),
            schema: Some("public".into()),
            kind: ShardedMappingKind::List,
            start: None,
            end: None,
            values: HashSet::from([FlexibleType::String("us".into())]),
            shard: 0,
        };
        Mapping::new(&[mapping]).unwrap()
    }

    fn range_mapping() -> Mapping {
        let mapping = ShardedMapping {
            database: "test".into(),
            column: "id".into(),
            table: Some("test_table".into()),
            schema: Some("public".into()),
            kind: ShardedMappingKind::Range,
            start: Some(FlexibleType::Integer(0)),
            end: Some(FlexibleType::Integer(1000)),
            values: HashSet::new(),
            shard: 0,
        };
        Mapping::new(&[mapping]).unwrap()
    }

    #[test]
    fn test_create_foreign_table_basic() {
        let columns = vec![
            ForeignTableColumn {
                is_not_null: true,
                ..test_column("id", "bigint")
            },
            ForeignTableColumn {
                column_default: "'unknown'::character varying".into(),
                ..test_column("name", "character varying(100)")
            },
        ];

        let sharded_tables = ShardedTables::default();
        let sql = create_foreign_table(&columns, "remote_server", &sharded_tables).unwrap();

        assert!(sql.contains("CREATE FOREIGN TABLE"));
        assert!(sql.contains("test_table"));
        assert!(sql.contains("bigint"));
        assert!(sql.contains("NOT NULL"));
        assert!(sql.contains("OPTIONS (column_name 'id')"));
        assert!(sql.contains("character varying(100)"));
        assert!(sql.contains("DEFAULT 'unknown'::character varying"));
        assert!(sql.contains("SERVER"));
        assert!(sql.contains("remote_server"));
        assert!(sql.contains("schema_name 'public'"));
        assert!(!sql.contains("PARTITION BY"));
    }

    #[test]
    fn test_create_foreign_table_with_hash_sharding() {
        let columns = vec![
            ForeignTableColumn {
                is_not_null: true,
                ..test_column("id", "bigint")
            },
            test_column("name", "text"),
        ];

        let sharded_tables: ShardedTables =
            [test_sharded_table("test_table", "id")].as_slice().into();

        let sql = create_foreign_table(&columns, "remote_server", &sharded_tables).unwrap();

        assert!(sql.contains("CREATE FOREIGN TABLE"));
        assert!(sql.contains("PARTITION BY HASH (id)"));
        assert!(sql.contains("SERVER remote_server"));
    }

    #[test]
    fn test_create_foreign_table_with_list_sharding() {
        let columns = vec![test_column("id", "bigint"), test_column("region", "text")];

        let sharded_tables: ShardedTables = [test_sharded_table_with_mapping(
            "test_table",
            "region",
            list_mapping(),
        )]
        .as_slice()
        .into();

        let sql = create_foreign_table(&columns, "remote_server", &sharded_tables).unwrap();

        assert!(sql.contains("PARTITION BY LIST (region)"));
    }

    #[test]
    fn test_create_foreign_table_with_range_sharding() {
        let columns = vec![test_column("id", "bigint"), test_column("name", "text")];

        let sharded_tables: ShardedTables = [test_sharded_table_with_mapping(
            "test_table",
            "id",
            range_mapping(),
        )]
        .as_slice()
        .into();

        let sql = create_foreign_table(&columns, "remote_server", &sharded_tables).unwrap();

        assert!(sql.contains("PARTITION BY RANGE (id)"));
    }

    #[test]
    fn test_create_foreign_table_no_shard_match() {
        let columns = vec![test_column("id", "bigint"), test_column("name", "text")];

        // Sharded table config for different table
        let sharded_tables: ShardedTables = [test_sharded_table("other_table", "user_id")]
            .as_slice()
            .into();

        let sql = create_foreign_table(&columns, "remote_server", &sharded_tables).unwrap();

        assert!(!sql.contains("PARTITION BY"));
    }

    #[test]
    fn test_create_foreign_table_column_mismatch() {
        let columns = vec![test_column("id", "bigint"), test_column("name", "text")];

        // Sharded table matches by name but column doesn't exist
        let sharded_tables: ShardedTables = [test_sharded_table("test_table", "user_id")]
            .as_slice()
            .into();

        let sql = create_foreign_table(&columns, "remote_server", &sharded_tables).unwrap();

        assert!(!sql.contains("PARTITION BY"));
    }

    #[test]
    fn test_create_foreign_table_with_generated() {
        let columns = vec![ForeignTableColumn {
            column_default: "(price * quantity)".into(),
            generated: "s".into(),
            ..test_column("total", "numeric")
        }];

        let sharded_tables = ShardedTables::default();
        let sql = create_foreign_table(&columns, "srv", &sharded_tables).unwrap();

        assert!(sql.contains("GENERATED ALWAYS AS ((price * quantity)) STORED"));
        assert!(!sql.contains("DEFAULT"));
    }

    #[test]
    fn test_create_foreign_table_with_collation() {
        let columns = vec![ForeignTableColumn {
            collation_name: "en_US".into(),
            collation_schema: "pg_catalog".into(),
            ..test_column("title", "text")
        }];

        let sharded_tables = ShardedTables::default();
        let sql = create_foreign_table(&columns, "srv", &sharded_tables).unwrap();

        assert!(sql.contains("COLLATE pg_catalog.\"en_US\""));
    }

    #[test]
    fn test_create_foreign_table_empty_columns() {
        let sharded_tables = ShardedTables::default();
        let result = create_foreign_table(&[], "srv", &sharded_tables);
        assert!(result.is_none());
    }

    #[test]
    fn test_quote_identifier() {
        assert_eq!(quote_identifier("users"), "users");
        assert_eq!(quote_identifier("my table"), "\"my table\"");
        assert_eq!(quote_identifier("123abc"), "\"123abc\"");
        assert_eq!(quote_identifier("has\"quote"), "\"has\"\"quote\"");
        assert_eq!(quote_identifier("CamelCase"), "\"CamelCase\"");
        assert_eq!(quote_identifier("_valid"), "_valid");
    }
}
