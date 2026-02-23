//! Test helpers for postgres_fdw integration tests.

use std::collections::HashSet;

use crate::backend::pool::ShardingSchema;
use crate::backend::replication::ShardedTables;
use crate::backend::schema::postgres_fdw::{
    CreateForeignTableResult, ForeignTableBuilder, ForeignTableColumn, ForeignTableSchema,
};
use crate::backend::Server;
use crate::config::{DataType, FlexibleType, ShardedMapping, ShardedMappingKind, ShardedTable};
use crate::frontend::router::sharding::Mapping;

/// Data type configuration for test tables.
#[derive(Debug, Clone, Copy)]
pub enum TestDataType {
    Bigint,
    Varchar,
    Uuid,
}

impl TestDataType {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Bigint => "bigint",
            Self::Varchar => "varchar",
            Self::Uuid => "uuid",
        }
    }

    pub fn sql_type(&self) -> &'static str {
        match self {
            Self::Bigint => "BIGINT",
            Self::Varchar => "VARCHAR(100)",
            Self::Uuid => "UUID",
        }
    }

    pub fn config_type(&self) -> DataType {
        match self {
            Self::Bigint => DataType::Bigint,
            Self::Varchar => DataType::Varchar,
            Self::Uuid => DataType::Uuid,
        }
    }

    pub fn flexible_values(&self) -> Vec<FlexibleType> {
        match self {
            Self::Bigint => vec![
                FlexibleType::Integer(1),
                FlexibleType::Integer(2),
                FlexibleType::Integer(3),
            ],
            Self::Varchar => vec![
                FlexibleType::String("us".into()),
                FlexibleType::String("eu".into()),
                FlexibleType::String("asia".into()),
            ],
            Self::Uuid => vec![
                FlexibleType::Uuid("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11".parse().unwrap()),
                FlexibleType::Uuid("b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a22".parse().unwrap()),
                FlexibleType::Uuid("c0eebc99-9c0b-4ef8-bb6d-6bb9bd380a33".parse().unwrap()),
            ],
        }
    }
}

/// Partition strategy for tests.
#[derive(Debug, Clone, Copy)]
pub enum TestPartitionStrategy {
    Hash,
    List,
    Range,
}

impl TestPartitionStrategy {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Hash => "HASH",
            Self::List => "LIST",
            Self::Range => "RANGE",
        }
    }
}

/// Test fixture for FDW statement generation tests.
pub struct FdwTestFixture {
    pub table_name: String,
    pub shard_column: String,
    pub data_type: TestDataType,
    pub strategy: TestPartitionStrategy,
    pub num_shards: usize,
}

impl FdwTestFixture {
    pub fn new(
        table_name: &str,
        shard_column: &str,
        data_type: TestDataType,
        strategy: TestPartitionStrategy,
    ) -> Self {
        Self {
            table_name: table_name.into(),
            shard_column: shard_column.into(),
            data_type,
            strategy,
            num_shards: 2,
        }
    }

    pub async fn create_table(&self, server: &mut Server) -> Result<(), crate::backend::Error> {
        self.cleanup(server).await?;

        let sql = format!(
            "CREATE TABLE {} (
                id BIGINT NOT NULL,
                {} {} NOT NULL,
                data TEXT
            )",
            self.table_name,
            self.shard_column,
            self.data_type.sql_type()
        );
        server.execute(&sql).await?;
        Ok(())
    }

    pub async fn cleanup(&self, server: &mut Server) -> Result<(), crate::backend::Error> {
        server
            .execute(&format!("DROP TABLE IF EXISTS {} CASCADE", self.table_name))
            .await?;
        server
            .execute(&format!(
                "DROP TABLE IF EXISTS {}_fdw CASCADE",
                self.table_name
            ))
            .await?;
        Ok(())
    }

    pub fn sharding_schema(&self, schema_name: &str) -> ShardingSchema {
        let mapping = self.create_mapping();
        let sharded_table = ShardedTable {
            database: "test".into(),
            name: Some(self.table_name.clone()),
            schema: Some(schema_name.into()),
            column: self.shard_column.clone(),
            data_type: self.data_type.config_type(),
            mapping,
            ..Default::default()
        };

        let tables: ShardedTables = [sharded_table].as_slice().into();
        ShardingSchema {
            shards: self.num_shards,
            tables,
            ..Default::default()
        }
    }

    fn create_mapping(&self) -> Option<Mapping> {
        match self.strategy {
            TestPartitionStrategy::Hash => None,
            TestPartitionStrategy::List => {
                let values = self.data_type.flexible_values();
                let mappings: Vec<_> = values
                    .into_iter()
                    .enumerate()
                    .map(|(i, v)| ShardedMapping {
                        database: "test".into(),
                        column: self.shard_column.clone(),
                        table: Some(self.table_name.clone()),
                        kind: ShardedMappingKind::List,
                        values: HashSet::from([v]),
                        shard: i % self.num_shards,
                        ..Default::default()
                    })
                    .collect();
                Mapping::new(&mappings)
            }
            TestPartitionStrategy::Range => {
                let values = self.data_type.flexible_values();
                let mappings: Vec<_> = (0..self.num_shards)
                    .map(|shard| {
                        let (start, end) = if shard == 0 {
                            (None, Some(values[1].clone()))
                        } else {
                            (Some(values[1].clone()), None)
                        };
                        ShardedMapping {
                            database: "test".into(),
                            column: self.shard_column.clone(),
                            table: Some(self.table_name.clone()),
                            kind: ShardedMappingKind::Range,
                            start,
                            end,
                            shard,
                            ..Default::default()
                        }
                    })
                    .collect();
                Mapping::new(&mappings)
            }
        }
    }

    pub fn generate_statements(
        &self,
        columns: &[ForeignTableColumn],
        sharding_schema: &ShardingSchema,
    ) -> CreateForeignTableResult {
        ForeignTableBuilder::new(columns, sharding_schema)
            .build()
            .expect("Statement generation should succeed")
    }

    fn expected_statement_count(&self) -> usize {
        1 + self.num_shards
    }

    pub fn verify_statements(&self, statements: &[String]) {
        assert_eq!(
            statements.len(),
            self.expected_statement_count(),
            "Expected {} statements for {}, got {}",
            self.expected_statement_count(),
            self.table_name,
            statements.len()
        );

        assert!(
            statements[0].contains("CREATE TABLE"),
            "First statement should be CREATE TABLE: {}",
            statements[0]
        );
        assert!(
            statements[0].contains(&format!("PARTITION BY {}", self.strategy.as_str())),
            "Parent should use {} partitioning: {}",
            self.strategy.as_str(),
            statements[0]
        );

        for (i, stmt) in statements.iter().skip(1).enumerate() {
            assert!(
                stmt.contains("CREATE FOREIGN TABLE"),
                "Statement {} should be CREATE FOREIGN TABLE: {}",
                i + 1,
                stmt
            );
            assert!(
                stmt.contains(&format!("shard_{}", i)),
                "Partition {} should reference shard_{}: {}",
                i,
                i,
                stmt
            );
        }
    }

    pub async fn execute_parent_statement(
        &self,
        server: &mut Server,
        statements: &[String],
    ) -> Result<(), crate::backend::Error> {
        let parent_stmt =
            statements[0].replace(&self.table_name, &format!("{}_fdw", self.table_name));
        server.execute(&parent_stmt).await?;
        Ok(())
    }
}

pub fn find_table_columns<'a>(
    schema: &'a ForeignTableSchema,
    table_name: &str,
) -> Option<&'a Vec<ForeignTableColumn>> {
    schema
        .tables()
        .get(&("public".into(), table_name.into()))
        .or_else(|| schema.tables().get(&("pgdog".into(), table_name.into())))
}
