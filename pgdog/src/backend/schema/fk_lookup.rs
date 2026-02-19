//! Foreign key lookup for sharding.
//!
//! Used during COPY when a table is sharded but doesn't have the sharding key directly.
//! Queries the cluster via FK joins to find the sharding key value.

use std::collections::HashMap;
use tracing::debug;

use crate::{
    backend::{
        pool::{Guard, Request},
        Cluster, Error,
    },
    config::ShardedTable,
    frontend::router::{
        parser::Shard,
        sharding::{ContextBuilder, Data as ShardingData},
    },
    net::{
        messages::{Bind, DataRow, Format, FromBytes, Message, Parameter, Protocol, ToBytes},
        Execute, Parse, ProtocolMessage, Sync,
    },
};

use super::Join;

/// FK lookup state for a single shard connection.
#[derive(Debug)]
struct ShardConnection {
    server: Guard,
    prepared: bool,
}

/// Foreign key lookup for resolving sharding keys via FK relationships.
///
/// Keeps replica connections to all shards open and uses prepared statements
/// to minimize query overhead (Parse once, then Bind/Execute/Sync for each lookup).
#[derive(Debug)]
pub struct FkLookup {
    /// The join query and sharding info.
    join: Join,
    /// Unique name for the prepared statement.
    prepared_name: String,
    /// Number of shards.
    num_shards: usize,
    /// Connections to each shard (lazily initialized).
    connections: HashMap<usize, ShardConnection>,
    /// The cluster for connections.
    cluster: Cluster,
}

impl FkLookup {
    /// Create a new FK lookup from a Join.
    pub fn new(join: Join, cluster: Cluster) -> Self {
        let prepared_name = format!("__pgdog_fk_{}", uuid::Uuid::new_v4());
        let num_shards = cluster.shards().len();
        Self {
            join,
            prepared_name,
            num_shards,
            connections: HashMap::new(),
            cluster,
        }
    }

    /// Get or create a connection to a shard replica.
    async fn ensure_connection(&mut self, shard: usize) -> Result<(), Error> {
        if !self.connections.contains_key(&shard) {
            let server = self.cluster.replica(shard, &Request::default()).await?;
            self.connections.insert(
                shard,
                ShardConnection {
                    server,
                    prepared: false,
                },
            );
        }
        Ok(())
    }

    /// Prepare the statement on a shard if not already prepared.
    async fn ensure_prepared(&mut self, shard: usize) -> Result<(), Error> {
        self.ensure_connection(shard).await?;

        // Check if already prepared
        if self
            .connections
            .get(&shard)
            .map(|c| c.prepared)
            .unwrap_or(false)
        {
            return Ok(());
        }

        // Clone values needed for the Parse message before getting mutable borrow
        let prepared_name = self.prepared_name.clone();
        let query = self.join.query.clone();

        let parse = Parse::named(&prepared_name, &query);
        let messages = vec![ProtocolMessage::from(parse), Sync.into()];

        let conn = self.connections.get_mut(&shard).unwrap();
        conn.server.send(&messages.into()).await?;

        // Read responses until ReadyForQuery
        loop {
            let msg: Message = conn.server.read().await?;
            match msg.code() {
                'Z' => break,
                'E' => {
                    let err = crate::net::messages::ErrorResponse::from_bytes(msg.to_bytes()?)?;
                    return Err(Error::ConnectionError(Box::new(err)));
                }
                _ => continue,
            }
        }

        conn.prepared = true;
        Ok(())
    }

    /// Look up the sharding key for a given primary key value.
    ///
    /// Queries all shards in parallel to find the row,
    /// then applies the sharding function to determine the target shard.
    /// Accepts both text and binary formats via ShardingData.
    pub async fn lookup(&mut self, pk_value: ShardingData<'_>) -> Result<Shard, Error> {
        // Ensure all connections are prepared first
        for shard in 0..self.num_shards {
            self.ensure_prepared(shard).await?;
        }

        let (param, format) = pk_value.parameter_with_format();

        // Take connections out to allow parallel querying
        let mut connections = std::mem::take(&mut self.connections);
        let prepared_name = self.prepared_name.clone();

        // Create futures for all shards
        let futures: Vec<_> = (0..self.num_shards)
            .filter_map(|shard| {
                connections.remove(&shard).map(|conn| {
                    Self::query_connection(
                        conn,
                        prepared_name.clone(),
                        param.clone(),
                        format,
                        shard,
                    )
                })
            })
            .collect();

        // Run all queries in parallel
        let results = futures::future::join_all(futures).await;

        // Put connections back and find result
        let mut sharding_key = None;
        for (shard, conn, result) in results {
            self.connections.insert(shard, conn);
            if let Ok(Some(key)) = result {
                sharding_key = Some(key);
            }
        }

        // Apply sharding
        let shard = if let Some(key) = sharding_key {
            self.apply_sharding(&key)?
        } else {
            Shard::All
        };

        debug!(
            "sharding key via foreign key lookup resolved to shard={}",
            shard
        );

        Ok(shard)
    }

    /// Query a single connection for the sharding key.
    async fn query_connection(
        mut conn: ShardConnection,
        prepared_name: String,
        param: Parameter,
        format: Format,
        shard: usize,
    ) -> (usize, ShardConnection, Result<Option<String>, Error>) {
        let result = Self::do_query(&mut conn, &prepared_name, param, format).await;
        (shard, conn, result)
    }

    /// Execute the actual query on a connection.
    async fn do_query(
        conn: &mut ShardConnection,
        prepared_name: &str,
        param: Parameter,
        format: Format,
    ) -> Result<Option<String>, Error> {
        // Request text format (0) for results so we can parse as string
        let bind = Bind::new_params_codes_results(prepared_name, &[param], &[format], &[0]);
        let execute = Execute::new();

        let messages = vec![ProtocolMessage::from(bind), execute.into(), Sync.into()];
        conn.server.send(&messages.into()).await?;

        let mut result: Option<String> = None;

        // Read responses until ReadyForQuery
        loop {
            let msg: Message = conn.server.read().await?;
            match msg.code() {
                'D' => {
                    let data_row = DataRow::from_bytes(msg.to_bytes()?)?;
                    if let Some(value) = data_row.get_text(0) {
                        result = Some(value);
                    }
                }
                'Z' => break,
                'E' => {
                    let err = crate::net::messages::ErrorResponse::from_bytes(msg.to_bytes()?)?;
                    return Err(Error::ConnectionError(Box::new(err)));
                }
                _ => continue,
            }
        }

        Ok(result)
    }

    /// Apply the sharding function to the key value.
    fn apply_sharding(&self, key: &str) -> Result<Shard, Error> {
        let table = &self.join.sharded_table;
        let ctx = ContextBuilder::new(table)
            .data(key)
            .shards(self.num_shards)
            .build()
            .map_err(|e| Error::Sharding(e.to_string()))?;

        ctx.apply().map_err(|e| Error::Sharding(e.to_string()))
    }

    /// Get a reference to the join configuration.
    pub fn join(&self) -> &Join {
        &self.join
    }

    /// Get the sharded table configuration.
    pub fn sharded_table(&self) -> &ShardedTable {
        &self.join.sharded_table
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::backend::schema::test_helpers::prelude::*;
    use crate::config::config;

    #[test]
    fn test_fk_lookup_new() {
        let sharding_schema = sharding().sharded_table("users", "user_id").build();

        let db_schema = schema()
            .relation(
                table("users")
                    .oid(1001)
                    .column(pk("id"))
                    .column(col("user_id")),
            )
            .relation(
                table("orders")
                    .oid(1002)
                    .column(pk("id"))
                    .column(fk("user_id", "users", "id")),
            )
            .build();

        let relation = db_schema.inner.get("public", "orders").unwrap();
        let join = db_schema
            .construct_join(relation, &sharding_schema)
            .unwrap();

        let lookup = FkLookup::new(join.clone(), Cluster::default());

        assert!(lookup.prepared_name.starts_with("__pgdog_fk_"));
        assert_eq!(lookup.join().sharding_column.name, "user_id");
        assert_eq!(lookup.sharded_table().column, "user_id");
    }

    #[tokio::test]
    async fn test_fk_lookup_query() {
        crate::logger();

        let cluster = Cluster::new_test(&config());
        cluster.launch();

        // Use a single connection for setup (both shards point to same DB in tests)
        let mut server = cluster.primary(0, &Request::default()).await.unwrap();

        // Drop and recreate tables with FK relationship
        server
            .execute("DROP TABLE IF EXISTS pgdog.fk_orders, pgdog.fk_users")
            .await
            .unwrap();

        server
            .execute(
                "CREATE TABLE pgdog.fk_users (
                    id BIGINT PRIMARY KEY,
                    user_id BIGINT NOT NULL
                )",
            )
            .await
            .unwrap();

        server
            .execute(
                "CREATE TABLE pgdog.fk_orders (
                    id BIGINT PRIMARY KEY,
                    user_id BIGINT REFERENCES pgdog.fk_users(id)
                )",
            )
            .await
            .unwrap();

        // Insert 1000 users and orders
        for i in 1i64..=1000 {
            server
                .execute(&format!(
                    "INSERT INTO pgdog.fk_users (id, user_id) VALUES ({}, {})",
                    i,
                    i * 100 + i % 17 // Add some variation to user_id
                ))
                .await
                .unwrap();

            server
                .execute(&format!(
                    "INSERT INTO pgdog.fk_orders (id, user_id) VALUES ({}, {})",
                    i * 10, // order id
                    i       // references user id
                ))
                .await
                .unwrap();
        }

        // Build schema with FK relationships
        let sharding_schema = sharding()
            .sharded_table("fk_users", "user_id")
            .database("pgdog")
            .build();

        let db_schema = schema()
            .relation(
                table("fk_users")
                    .schema("pgdog")
                    .oid(1001)
                    .column(pk("id"))
                    .column(col("user_id")),
            )
            .relation(
                table("fk_orders")
                    .schema("pgdog")
                    .oid(1002)
                    .column(pk("id"))
                    .column(ColumnBuilder::new("user_id").foreign_key("pgdog", "fk_users", "id")),
            )
            .build();

        let relation = db_schema.inner.get("pgdog", "fk_orders").unwrap();
        let join = db_schema
            .construct_join(relation, &sharding_schema)
            .unwrap();

        // Query directly looks up sharding key from fk_users
        assert!(join.query.contains("user_id"));
        assert!(join.query.contains("fk_users"));

        let mut lookup = FkLookup::new(join, cluster.clone());

        // Test all 1000 FK values - pass the FK value (user_id which references fk_users.id)
        for i in 1i64..=1000 {
            // The FK value is `i` (references fk_users.id)
            let fk_value = i;
            let fk_value_str = fk_value.to_string();

            let text_result = lookup
                .lookup(ShardingData::Text(&fk_value_str))
                .await
                .unwrap();
            let binary_result = lookup
                .lookup(ShardingData::Binary(&fk_value.to_be_bytes()))
                .await
                .unwrap();

            assert!(
                matches!(text_result, Shard::Direct(_)),
                "FK value {} should route to a shard",
                fk_value
            );
            assert_eq!(
                text_result, binary_result,
                "text and binary should match for FK value {}",
                fk_value
            );
        }

        // Look up non-existent FK values -> Shard::All
        for non_existent in [99999i64, 123456, 999999] {
            let text_result = lookup
                .lookup(ShardingData::Text(&non_existent.to_string()))
                .await
                .unwrap();
            let binary_result = lookup
                .lookup(ShardingData::Binary(&non_existent.to_be_bytes()))
                .await
                .unwrap();

            assert_eq!(text_result, Shard::All);
            assert_eq!(text_result, binary_result);
        }

        // Clean up
        cluster
            .execute("DROP TABLE IF EXISTS pgdog.fk_orders, pgdog.fk_users")
            .await
            .unwrap();

        cluster.shutdown();
    }
}
