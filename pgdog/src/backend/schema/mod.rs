//! Schema operations.
pub mod columns;
pub mod relation;

use std::collections::HashMap;
use tracing::debug;

pub use relation::Relation;

use super::{pool::Request, Cluster, Error, Server};

static SETUP: &str = include_str!("setup.sql");

/// Load schema from database.
#[derive(Debug, Clone, Default)]
pub struct Schema {
    relations: HashMap<(String, String), Relation>,
}

impl Schema {
    /// Load schema from a server connection.
    pub async fn load(server: &mut Server) -> Result<Self, Error> {
        let relations = Relation::load(server)
            .await?
            .into_iter()
            .map(|relation| {
                (
                    (relation.schema().to_owned(), relation.name.clone()),
                    relation,
                )
            })
            .collect();

        Ok(Self { relations })
    }

    /// Install PgDog functions and schema.
    pub async fn setup(server: &mut Server) -> Result<(), Error> {
        server.execute_checked(SETUP).await?;
        Ok(())
    }

    /// Install PgDog-specific functions and triggers.
    pub async fn install(cluster: &Cluster) -> Result<(), Error> {
        let shards = cluster.shards();
        let sharded_tables = cluster.sharded_tables();

        if shards.len() < 2 || sharded_tables.is_empty() {
            return Ok(());
        }

        for (shard_number, shard) in shards.iter().enumerate() {
            let mut server = shard.primary(&Request::default()).await?;
            Self::setup(&mut server).await?;
            let schema = Self::load(&mut server).await?;

            debug!("[{}] {:#?}", server.addr(), schema);

            for table in sharded_tables {
                for schema_table in schema
                    .tables()
                    .iter()
                    .filter(|table| table.schema() != "pgdog")
                {
                    let column_match = schema_table.columns.iter().find(|column| {
                        column.column_name == table.column && column.data_type == "bigint"
                    });
                    if let Some(column_match) = column_match {
                        if table.name.is_none()
                            || table.name == Some(column_match.table_name.clone())
                        {
                            if table.primary {
                                let query = format!(
                                    "SELECT pgdog.install_next_id('{}', '{}', '{}', {}, {})",
                                    schema_table.schema(),
                                    schema_table.name,
                                    column_match.column_name,
                                    shards.len(),
                                    shard_number
                                );

                                server.execute(&query).await?;
                            }

                            let query = format!(
                                "SELECT pgdog.install_trigger('{}', '{}', '{}', {}, {})",
                                schema_table.schema(),
                                schema_table.name,
                                column_match.column_name,
                                shards.len(),
                                shard_number
                            );

                            server.execute(&query).await?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Get table by name.
    pub fn table(&self, name: &str, schema: Option<&str>) -> Option<&Relation> {
        let schema = schema.unwrap_or("public");
        self.relations.get(&(name.to_string(), schema.to_string()))
    }

    /// Get all indices.
    pub fn tables(&self) -> Vec<&Relation> {
        self.relations
            .values()
            .filter(|value| value.is_table())
            .collect()
    }

    /// Get all sequences.
    pub fn sequences(&self) -> Vec<&Relation> {
        self.relations
            .values()
            .filter(|value| value.is_sequence())
            .collect()
    }
}

#[cfg(test)]
mod test {
    use crate::backend::pool::Request;

    use super::super::pool::test::pool;
    use super::Schema;

    #[tokio::test]
    async fn test_schema() {
        let pool = pool();
        let mut conn = pool.get(&Request::default()).await.unwrap();
        conn.execute("DROP SCHEMA pgdog CASCADE").await.unwrap();
        let _schema = Schema::load(&mut conn).await.unwrap();
        Schema::setup(&mut conn).await.unwrap();
        let schema = Schema::load(&mut conn).await.unwrap();
        let seq = schema
            .sequences()
            .into_iter()
            .find(|seq| seq.schema() == "pgdog")
            .cloned()
            .unwrap();
        assert_eq!(seq.name, "validator_bigint_id_seq");

        let server_ok = conn.fetch_all::<i32>("SELECT 1 AS one").await.unwrap();
        assert_eq!(server_ok.first().unwrap().clone(), 1);

        let debug = conn
            .fetch_all::<String>("SELECT pgdog.debug()")
            .await
            .unwrap();
        assert!(debug.first().unwrap().contains("PgDog Debug"));
    }
}
