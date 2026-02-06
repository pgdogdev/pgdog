//! CREATE FOREIGN TABLE statement generation.

use std::fmt::Write;

use rand::Rng;

use crate::backend::pool::ShardingSchema;
use crate::config::{DataType, FlexibleType, ShardedTable};
use crate::frontend::router::parser::Column;
use crate::frontend::router::sharding::Mapping;

use super::{Error, ForeignTableColumn};

/// A type mismatch between a table column and the configured sharding data type.
#[derive(Debug, Clone)]
pub struct TypeMismatch {
    pub schema_name: String,
    pub table_name: String,
    pub column_name: String,
    pub column_type: String,
    pub configured_type: DataType,
}

impl std::fmt::Display for TypeMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}.{}.{}: column type '{}' does not match configured data_type '{:?}'",
            self.schema_name,
            self.table_name,
            self.column_name,
            self.column_type,
            self.configured_type
        )
    }
}

/// Result of creating foreign table statements.
pub struct CreateForeignTableResult {
    pub statements: Vec<String>,
    pub type_mismatches: Vec<TypeMismatch>,
}

/// Format a FlexibleType as a SQL literal.
fn flexible_type_to_sql(value: &FlexibleType) -> String {
    match value {
        FlexibleType::Integer(i) => i.to_string(),
        FlexibleType::Uuid(u) => format!("'{}'", u),
        FlexibleType::String(s) => format!("'{}'", s.replace('\'', "''")),
    }
}

/// Check if a PostgreSQL column type string matches the configured DataType.
fn column_type_matches_data_type(column_type: &str, data_type: DataType) -> bool {
    let col_lower = column_type.to_lowercase();
    match data_type {
        DataType::Bigint => {
            col_lower.starts_with("bigint")
                || col_lower.starts_with("int8")
                || col_lower.starts_with("bigserial")
                || col_lower.starts_with("serial8")
                || col_lower.starts_with("integer")
                || col_lower.starts_with("int4")
                || col_lower.starts_with("int")
                || col_lower.starts_with("smallint")
                || col_lower.starts_with("int2")
        }
        DataType::Uuid => col_lower.starts_with("uuid"),
        DataType::Varchar => {
            col_lower.starts_with("character varying")
                || col_lower.starts_with("varchar")
                || col_lower.starts_with("text")
        }
        DataType::Vector => col_lower.starts_with("vector"),
    }
}

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
        format!(r#""{}""#, name.to_string())
    }
}

/// Escape a string literal for use in SQL.
fn escape_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Format a fully qualified table name (schema.table).
fn qualified_table(schema: &str, table: &str) -> String {
    format!("{}.{}", quote_identifier(schema), quote_identifier(table))
}

/// Builder for CREATE FOREIGN TABLE statements.
pub struct ForeignTableBuilder<'a> {
    columns: &'a [ForeignTableColumn],
    sharding_schema: &'a ShardingSchema,
    type_mismatches: Vec<TypeMismatch>,
    /// Child partitions for two-tier partitioning (each Vec is columns for one partition).
    children: Vec<Vec<ForeignTableColumn>>,
}

impl<'a> ForeignTableBuilder<'a> {
    /// Create a new builder with required parameters.
    pub fn new(columns: &'a [ForeignTableColumn], sharding_schema: &'a ShardingSchema) -> Self {
        Self {
            columns,
            sharding_schema,
            type_mismatches: Vec::new(),
            children: Vec::new(),
        }
    }

    /// Add child partitions for two-tier partitioning.
    /// Each Vec represents columns for one child partition.
    pub fn with_children(mut self, children: Vec<Vec<ForeignTableColumn>>) -> Self {
        self.children = children;
        self
    }

    /// Find the sharding configuration for this table, if any.
    /// Records any type mismatches encountered and returns a cloned config.
    fn find_sharded_config(&mut self) -> Option<ShardedTable> {
        let first = self.columns.first()?;
        let table_name = &first.table_name;
        let schema_name = &first.schema_name;

        for col in self.columns {
            let column = Column {
                name: &col.column_name,
                table: Some(table_name.as_str()),
                schema: Some(schema_name.as_str()),
            };

            if let Some(sharded) = self.sharding_schema.tables().get_table(column) {
                if !column_type_matches_data_type(&col.column_type, sharded.data_type) {
                    let mismatch = TypeMismatch {
                        schema_name: schema_name.clone(),
                        table_name: table_name.clone(),
                        column_name: col.column_name.clone(),
                        column_type: col.column_type.clone(),
                        configured_type: sharded.data_type,
                    };
                    self.type_mismatches.push(mismatch);
                    continue;
                }
                return Some(sharded.clone());
            }
        }

        None
    }

    /// Build column definitions SQL fragment (shared between parent and foreign tables).
    fn build_columns(&self) -> Result<String, Error> {
        let mut sql = String::new();
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
            )?;

            if col.has_collation() {
                write!(
                    sql,
                    " COLLATE {}.{}",
                    quote_identifier(&col.collation_schema),
                    quote_identifier(&col.collation_name)
                )?;
            }

            if col.is_not_null {
                sql.push_str(" NOT NULL");
            }
        }

        Ok(sql)
    }

    /// Build the CREATE TABLE / CREATE FOREIGN TABLE statement(s).
    pub fn build(mut self) -> Result<CreateForeignTableResult, Error> {
        let first = self.columns.first().ok_or(Error::NoColumns)?;
        let schema_name = &first.schema_name.clone();
        let table_name = &first.table_name.clone();

        let statements = if let Some(sharded) = self.find_sharded_config() {
            self.build_sharded(table_name, schema_name, &sharded)?
        } else if !self.type_mismatches.is_empty() {
            // Skip tables with type mismatches entirely
            vec![]
        } else {
            self.build_foreign_table(table_name, schema_name)?
        };

        Ok(CreateForeignTableResult {
            statements,
            type_mismatches: self.type_mismatches,
        })
    }

    /// Build a simple foreign table (non-sharded).
    fn build_foreign_table(
        &self,
        table_name: &str,
        schema_name: &str,
    ) -> Result<Vec<String>, Error> {
        let mut sql = String::new();
        writeln!(
            sql,
            "CREATE FOREIGN TABLE {} (",
            qualified_table(schema_name, table_name)
        )?;

        // Column definitions with OPTIONS for foreign tables
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
            )?;

            write!(
                sql,
                " OPTIONS (column_name {})",
                escape_literal(&col.column_name)
            )?;

            if col.has_collation() {
                write!(
                    sql,
                    " COLLATE {}.{}",
                    quote_identifier(&col.collation_schema),
                    quote_identifier(&col.collation_name)
                )?;
            }

            if col.is_not_null {
                sql.push_str(" NOT NULL");
            }
        }

        sql.push('\n');
        sql.push(')');

        let shard = rand::rng().random_range(0..self.sharding_schema.shards.max(1));
        write!(
            sql,
            "\nSERVER shard_{}\nOPTIONS (schema_name {}, table_name {})",
            shard,
            escape_literal(schema_name),
            escape_literal(table_name)
        )?;

        Ok(vec![sql])
    }

    /// Build a sharded table: parent table + foreign table partitions.
    /// If children partitions exist, creates two-tier partitioning.
    fn build_sharded(
        &self,
        table_name: &str,
        schema_name: &str,
        sharded: &ShardedTable,
    ) -> Result<Vec<String>, Error> {
        if self.children.is_empty() {
            self.build_sharded_single_tier(table_name, schema_name, sharded)
        } else {
            self.build_sharded_two_tier(table_name, schema_name, sharded)
        }
    }

    /// Build single-tier sharding: parent table with foreign table partitions.
    fn build_sharded_single_tier(
        &self,
        table_name: &str,
        schema_name: &str,
        sharded: &ShardedTable,
    ) -> Result<Vec<String>, Error> {
        let strategy = PartitionStrategy::from_sharded_table(sharded);
        let mut statements = Vec::new();

        // Create parent table with PARTITION BY
        let mut parent = String::new();
        let qualified_name = qualified_table(schema_name, table_name);
        writeln!(parent, "CREATE TABLE {} (", qualified_name)?;
        parent.push_str(&self.build_columns()?);
        parent.push('\n');
        write!(
            parent,
            ") PARTITION BY {} ({})",
            strategy.as_sql(),
            quote_identifier(&sharded.column)
        )?;
        statements.push(parent);

        // Create foreign table partitions for each shard
        self.build_foreign_partitions(
            &mut statements,
            table_name,
            schema_name,
            &qualified_name,
            sharded,
        )?;

        Ok(statements)
    }

    /// Build two-tier sharding: parent table -> intermediate partitions -> foreign table partitions.
    fn build_sharded_two_tier(
        &self,
        table_name: &str,
        schema_name: &str,
        sharded: &ShardedTable,
    ) -> Result<Vec<String>, Error> {
        let shard_strategy = PartitionStrategy::from_sharded_table(sharded);
        let mut statements = Vec::new();

        // Get the parent's original partition key (e.g., "RANGE (created_at)")
        let parent_partition_key = self
            .columns
            .first()
            .map(|c| c.partition_key.as_str())
            .unwrap_or("");

        // Create parent table with original PARTITION BY
        let mut parent = String::new();
        let qualified_name = qualified_table(schema_name, table_name);
        writeln!(parent, "CREATE TABLE {} (", qualified_name)?;
        parent.push_str(&self.build_columns()?);
        parent.push('\n');
        write!(parent, ") PARTITION BY {}", parent_partition_key)?;
        statements.push(parent);

        // For each child partition, create an intermediate partition that is itself partitioned
        for child_columns in &self.children {
            let Some(first_col) = child_columns.first() else {
                continue;
            };

            let child_table_name = &first_col.table_name;
            let child_schema_name = &first_col.schema_name;
            let partition_bound = &first_col.partition_bound;

            // Create intermediate partition table (partitioned by hash on shard key)
            let mut intermediate = String::new();
            let qualified_child = qualified_table(child_schema_name, child_table_name);

            write!(
                intermediate,
                "CREATE TABLE {} PARTITION OF {} ",
                qualified_child, qualified_name
            )?;
            write!(intermediate, "{}", partition_bound)?;
            write!(
                intermediate,
                " PARTITION BY {} ({})",
                shard_strategy.as_sql(),
                quote_identifier(&sharded.column)
            )?;
            statements.push(intermediate);

            // Create foreign table partitions for this intermediate partition
            self.build_foreign_partitions(
                &mut statements,
                child_table_name,
                child_schema_name,
                &qualified_child,
                sharded,
            )?;
        }

        Ok(statements)
    }

    /// Build foreign table partitions for each shard.
    fn build_foreign_partitions(
        &self,
        statements: &mut Vec<String>,
        table_name: &str,
        schema_name: &str,
        qualified_parent: &str,
        sharded: &ShardedTable,
    ) -> Result<(), Error> {
        for shard in 0..self.sharding_schema.shards {
            let mut partition = String::new();
            let partition_table_name = format!("{}_shard_{}", table_name, shard);
            let qualified_partition = qualified_table(schema_name, &partition_table_name);
            let server_name = format!("shard_{}", shard);

            write!(
                partition,
                "CREATE FOREIGN TABLE {} PARTITION OF {} ",
                qualified_partition, qualified_parent
            )?;

            // Partition bounds (always hash for foreign partitions in two-tier)
            match &sharded.mapping {
                None => {
                    write!(
                        partition,
                        "FOR VALUES WITH (MODULUS {}, REMAINDER {})",
                        self.sharding_schema.shards, shard
                    )?;
                }
                Some(Mapping::List(list_shards)) => {
                    let values = list_shards.values_for_shard(shard);
                    if values.is_empty() {
                        write!(partition, "DEFAULT")?;
                    } else {
                        let values_sql: Vec<_> =
                            values.iter().map(|v| flexible_type_to_sql(v)).collect();
                        write!(partition, "FOR VALUES IN ({})", values_sql.join(", "))?;
                    }
                }
                Some(Mapping::Range(ranges)) => {
                    if let Some(range) = ranges.iter().find(|r| r.shard == shard) {
                        let start = range
                            .start
                            .as_ref()
                            .map(flexible_type_to_sql)
                            .unwrap_or_else(|| "MINVALUE".to_string());
                        let end = range
                            .end
                            .as_ref()
                            .map(flexible_type_to_sql)
                            .unwrap_or_else(|| "MAXVALUE".to_string());
                        write!(partition, "FOR VALUES FROM ({}) TO ({})", start, end)?;
                    } else {
                        write!(partition, "DEFAULT")?;
                    }
                }
            }

            write!(
                partition,
                "\nSERVER {}\nOPTIONS (schema_name {}, table_name {})",
                quote_identifier(&server_name),
                escape_literal(schema_name),
                escape_literal(table_name)
            )?;

            statements.push(partition);
        }
        Ok(())
    }
}

/// Generate CREATE FOREIGN TABLE statements from column definitions.
///
/// All columns must belong to the same table. If the table is found in sharded_tables
/// configuration, creates a partitioned parent table with foreign table partitions
/// for each shard. Server names are generated as `shard_{n}`.
///
/// Returns statements and any type mismatches encountered.
pub fn create_foreign_table(
    columns: &[ForeignTableColumn],
    sharding_schema: &ShardingSchema,
) -> Result<CreateForeignTableResult, Error> {
    ForeignTableBuilder::new(columns, sharding_schema).build()
}

/// Generate CREATE FOREIGN TABLE statements with two-tier partitioning.
///
/// For sharded tables with existing partitions, creates:
/// 1. Parent table with PARTITION BY (on shard key)
/// 2. Intermediate partition tables (with original bounds, further partitioned by shard key)
/// 3. Foreign table partitions for each shard
///
/// Each entry in `children` is columns for one child partition.
pub fn create_foreign_table_with_children(
    columns: &[ForeignTableColumn],
    sharding_schema: &ShardingSchema,
    children: Vec<Vec<ForeignTableColumn>>,
) -> Result<CreateForeignTableResult, Error> {
    ForeignTableBuilder::new(columns, sharding_schema)
        .with_children(children)
        .build()
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use super::*;
    use crate::backend::replication::ShardedTables;
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
            is_partition: false,
            parent_table_name: String::new(),
            parent_schema_name: String::new(),
            partition_bound: String::new(),
            partition_key: String::new(),
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
        data_type: DataType,
    ) -> ShardedTable {
        ShardedTable {
            mapping: Some(mapping),
            data_type,
            ..test_sharded_table(table, column)
        }
    }

    fn sharding_schema_with_tables(tables: ShardedTables, shards: usize) -> ShardingSchema {
        ShardingSchema {
            shards,
            tables,
            schemas: Default::default(),
            rewrite: Default::default(),
            query_parser_engine: Default::default(),
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

        let schema = sharding_schema_with_tables(ShardedTables::default(), 1);
        let statements = create_foreign_table(&columns, &schema).unwrap();

        assert_eq!(statements.statements.len(), 1);
        let sql = &statements.statements[0];
        assert!(sql.contains(r#"CREATE FOREIGN TABLE "public"."test_table""#));
        assert!(sql.contains("bigint"));
        assert!(sql.contains("NOT NULL"));
        assert!(sql.contains("OPTIONS (column_name 'id')"));
        assert!(sql.contains("character varying(100)"));
        assert!(!sql.contains("DEFAULT")); // Defaults handled by remote table
        assert!(sql.contains("SERVER"));
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

        let tables: ShardedTables = [test_sharded_table("test_table", "id")].as_slice().into();
        let schema = sharding_schema_with_tables(tables, 2);

        let statements = create_foreign_table(&columns, &schema).unwrap();

        assert_eq!(statements.statements.len(), 3); // parent + 2 partitions
        assert!(statements.statements[0].contains(r#"CREATE TABLE "public"."test_table""#));
        assert!(statements.statements[0].contains(r#"PARTITION BY HASH ("id")"#));
        assert!(statements.statements[1].contains(
            r#"CREATE FOREIGN TABLE "public"."test_table_shard_0" PARTITION OF "public"."test_table""#
        ));
        assert!(statements.statements[1].contains("FOR VALUES WITH (MODULUS 2, REMAINDER 0)"));
        assert!(statements.statements[1].contains(r#"SERVER "shard_0""#));
        assert!(statements.statements[2].contains(
            r#"CREATE FOREIGN TABLE "public"."test_table_shard_1" PARTITION OF "public"."test_table""#
        ));
        assert!(statements.statements[2].contains("FOR VALUES WITH (MODULUS 2, REMAINDER 1)"));
        assert!(statements.statements[2].contains(r#"SERVER "shard_1""#));
    }

    #[test]
    fn test_create_foreign_table_with_list_sharding() {
        let columns = vec![test_column("id", "bigint"), test_column("region", "text")];

        let tables: ShardedTables = [test_sharded_table_with_mapping(
            "test_table",
            "region",
            list_mapping(),
            DataType::Varchar,
        )]
        .as_slice()
        .into();
        let schema = sharding_schema_with_tables(tables, 1);

        let statements = create_foreign_table(&columns, &schema).unwrap();

        assert!(statements.statements[0].contains(r#"CREATE TABLE "public"."test_table""#));
        assert!(statements.statements[0].contains(r#"PARTITION BY LIST ("region")"#));
    }

    #[test]
    fn test_create_foreign_table_with_range_sharding() {
        let columns = vec![test_column("id", "bigint"), test_column("name", "text")];

        let tables: ShardedTables = [test_sharded_table_with_mapping(
            "test_table",
            "id",
            range_mapping(),
            DataType::Bigint,
        )]
        .as_slice()
        .into();
        let schema = sharding_schema_with_tables(tables, 1);

        let statements = create_foreign_table(&columns, &schema).unwrap();

        assert!(statements.statements[0].contains(r#"CREATE TABLE "public"."test_table""#));
        assert!(statements.statements[0].contains(r#"PARTITION BY RANGE ("id")"#));
    }

    #[test]
    fn test_create_foreign_table_no_shard_match() {
        let columns = vec![test_column("id", "bigint"), test_column("name", "text")];

        let tables: ShardedTables = [test_sharded_table("other_table", "user_id")]
            .as_slice()
            .into();
        let schema = sharding_schema_with_tables(tables, 2);

        let statements = create_foreign_table(&columns, &schema).unwrap();

        assert_eq!(statements.statements.len(), 1);
        assert!(statements.statements[0].contains(r#"CREATE FOREIGN TABLE "public"."test_table""#));
        assert!(!statements.statements[0].contains("PARTITION BY"));
    }

    #[test]
    fn test_create_foreign_table_column_mismatch() {
        let columns = vec![test_column("id", "bigint"), test_column("name", "text")];

        let tables: ShardedTables = [test_sharded_table("test_table", "user_id")]
            .as_slice()
            .into();
        let schema = sharding_schema_with_tables(tables, 2);

        let statements = create_foreign_table(&columns, &schema).unwrap();

        assert_eq!(statements.statements.len(), 1);
        assert!(statements.statements[0].contains(r#"CREATE FOREIGN TABLE "public"."test_table""#));
        assert!(!statements.statements[0].contains("PARTITION BY"));
    }

    #[test]
    fn test_create_foreign_table_with_generated() {
        let columns = vec![ForeignTableColumn {
            column_default: "(price * quantity)".into(),
            generated: "s".into(),
            ..test_column("total", "numeric")
        }];

        let schema = sharding_schema_with_tables(ShardedTables::default(), 1);
        let statements = create_foreign_table(&columns, &schema).unwrap();

        assert!(statements.statements[0].contains(r#"CREATE FOREIGN TABLE "public"."test_table""#));
        // Defaults and generated columns handled by remote table
        assert!(!statements.statements[0].contains("GENERATED"));
        assert!(!statements.statements[0].contains("DEFAULT"));
    }

    #[test]
    fn test_create_foreign_table_with_collation() {
        let columns = vec![ForeignTableColumn {
            collation_name: "en_US".into(),
            collation_schema: "pg_catalog".into(),
            ..test_column("title", "text")
        }];

        let schema = sharding_schema_with_tables(ShardedTables::default(), 1);
        let statements = create_foreign_table(&columns, &schema).unwrap();

        assert!(statements.statements[0].contains(r#"CREATE FOREIGN TABLE "public"."test_table""#));
        assert!(statements.statements[0].contains(r#"COLLATE "pg_catalog"."en_US""#));
    }

    #[test]
    fn test_create_foreign_table_empty_columns() {
        let schema = sharding_schema_with_tables(ShardedTables::default(), 1);
        let result = create_foreign_table(&[], &schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_quote_identifier() {
        // All identifiers are now quoted
        assert_eq!(quote_identifier("users"), "\"users\"");
        assert_eq!(quote_identifier("my table"), "\"my table\"");
        assert_eq!(quote_identifier("123abc"), "\"123abc\"");
        assert_eq!(quote_identifier("has\"quote"), "\"has\"\"quote\"");
        assert_eq!(quote_identifier("CamelCase"), "\"CamelCase\"");
        assert_eq!(quote_identifier("_valid"), "\"_valid\"");
    }

    fn test_partition_column(
        table_name: &str,
        name: &str,
        col_type: &str,
        parent_table: &str,
        partition_bound: &str,
    ) -> ForeignTableColumn {
        ForeignTableColumn {
            schema_name: "public".into(),
            table_name: table_name.into(),
            column_name: name.into(),
            column_type: col_type.into(),
            is_not_null: false,
            column_default: String::new(),
            generated: String::new(),
            collation_name: String::new(),
            collation_schema: String::new(),
            is_partition: true,
            parent_table_name: parent_table.into(),
            parent_schema_name: "public".into(),
            partition_bound: partition_bound.into(),
            partition_key: String::new(),
        }
    }

    fn test_partitioned_parent_column(
        name: &str,
        col_type: &str,
        partition_key: &str,
    ) -> ForeignTableColumn {
        ForeignTableColumn {
            partition_key: partition_key.into(),
            ..test_column(name, col_type)
        }
    }

    #[test]
    fn test_create_foreign_table_two_tier_partitioning() {
        // Parent table "orders" partitioned by RANGE on date, with children partitioned by hash across shards
        let parent_columns = vec![
            test_partitioned_parent_column("id", "bigint", "RANGE (created_at)"),
            test_partitioned_parent_column("created_at", "date", "RANGE (created_at)"),
            test_partitioned_parent_column("data", "text", "RANGE (created_at)"),
        ];

        // Child partitions with their bounds
        let partition_2024 = vec![
            test_partition_column(
                "orders_2024",
                "id",
                "bigint",
                "test_table",
                "FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')",
            ),
            test_partition_column(
                "orders_2024",
                "created_at",
                "date",
                "test_table",
                "FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')",
            ),
            test_partition_column(
                "orders_2024",
                "data",
                "text",
                "test_table",
                "FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')",
            ),
        ];

        let partition_2025 = vec![
            test_partition_column(
                "orders_2025",
                "id",
                "bigint",
                "test_table",
                "FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')",
            ),
            test_partition_column(
                "orders_2025",
                "created_at",
                "date",
                "test_table",
                "FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')",
            ),
            test_partition_column(
                "orders_2025",
                "data",
                "text",
                "test_table",
                "FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')",
            ),
        ];

        let tables: ShardedTables = [test_sharded_table("test_table", "id")].as_slice().into();
        let schema = sharding_schema_with_tables(tables, 2);

        let result = ForeignTableBuilder::new(&parent_columns, &schema)
            .with_children(vec![partition_2024, partition_2025])
            .build()
            .unwrap();

        // Expected structure:
        // 1. Parent table with original PARTITION BY RANGE (created_at)
        // 2. orders_2024 partition (with original bounds) that is itself PARTITION BY HASH
        // 3. orders_2024_shard_0 foreign table
        // 4. orders_2024_shard_1 foreign table
        // 5. orders_2025 partition (with original bounds) that is itself PARTITION BY HASH
        // 6. orders_2025_shard_0 foreign table
        // 7. orders_2025_shard_1 foreign table
        assert_eq!(result.statements.len(), 7);

        // Parent table - uses original partition key (RANGE on date)
        assert!(result.statements[0].contains(r#"CREATE TABLE "public"."test_table""#));
        assert!(result.statements[0].contains("PARTITION BY RANGE (created_at)"));

        // orders_2024 intermediate partition
        assert!(result.statements[1]
            .contains(r#"CREATE TABLE "public"."orders_2024" PARTITION OF "public"."test_table""#));
        assert!(result.statements[1].contains("FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')"));
        assert!(result.statements[1].contains(r#"PARTITION BY HASH ("id")"#));

        // orders_2024_shard_0 foreign table
        assert!(result.statements[2].contains(
            r#"CREATE FOREIGN TABLE "public"."orders_2024_shard_0" PARTITION OF "public"."orders_2024""#
        ));
        assert!(result.statements[2].contains("FOR VALUES WITH (MODULUS 2, REMAINDER 0)"));
        assert!(result.statements[2].contains(r#"SERVER "shard_0""#));

        // orders_2024_shard_1 foreign table
        assert!(result.statements[3].contains(
            r#"CREATE FOREIGN TABLE "public"."orders_2024_shard_1" PARTITION OF "public"."orders_2024""#
        ));
        assert!(result.statements[3].contains("FOR VALUES WITH (MODULUS 2, REMAINDER 1)"));
        assert!(result.statements[3].contains(r#"SERVER "shard_1""#));

        // orders_2025 intermediate partition
        assert!(result.statements[4]
            .contains(r#"CREATE TABLE "public"."orders_2025" PARTITION OF "public"."test_table""#));
        assert!(result.statements[4].contains("FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')"));
        assert!(result.statements[4].contains(r#"PARTITION BY HASH ("id")"#));

        // orders_2025_shard_0 foreign table
        assert!(result.statements[5].contains(
            r#"CREATE FOREIGN TABLE "public"."orders_2025_shard_0" PARTITION OF "public"."orders_2025""#
        ));

        // orders_2025_shard_1 foreign table
        assert!(result.statements[6].contains(
            r#"CREATE FOREIGN TABLE "public"."orders_2025_shard_1" PARTITION OF "public"."orders_2025""#
        ));
    }
}
