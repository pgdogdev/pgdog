//! Move a single table between databases.
//!
//! Reuses the resharding machinery: the table is wrapped in a publication
//! that PgDog creates and drops, then copied and replicated until the task
//! is stopped. There is no cutover; the application switches databases itself.

use std::collections::HashSet;

use tracing::{info, warn};

use super::Error;
use super::resharding_state::ReshardingState;
use crate::backend::Cluster;
use crate::backend::pool::Request;
use crate::net::DataRow;
use crate::util::sql::quote_literal;
use crate::util::{escape_identifier, random_string};

/// A single table moved between databases.
#[derive(Debug, Clone)]
pub(crate) struct TableMove {
    /// As the user wrote it; Postgres resolves it like any table name.
    table: String,
    publication: String,
}

/// The moved table as resolved on the source.
struct SourceTable {
    /// Quoted `schema.table`.
    name: String,
    /// Quoted schema.
    schema: String,
    /// Quoted names of the tables it references with a foreign key.
    referenced: Vec<String>,
}

impl TableMove {
    pub(crate) fn new(table: String) -> Self {
        Self {
            table,
            publication: format!("__pgdog_move_{}", random_string(19).to_lowercase()),
        }
    }

    pub(crate) fn publication(&self) -> &str {
        &self.publication
    }

    /// Validate the move, create the table's schema on the destination
    /// and the publication on every source shard.
    pub(crate) async fn prepare(&self, state: &ReshardingState) -> Result<(), Error> {
        let source = &state.source.identifier().database;
        if *source == state.destination.identifier().database {
            return Err(Error::SameDatabase(source.clone()));
        }

        let table = self.resolve(&state.source).await?;
        if let Some((shard, missing)) =
            missing_tables(&state.destination, &table.referenced).await?
        {
            return Err(Error::ReferencedTablesMissing {
                table: table.name,
                shard,
                missing: missing.join(", "),
            });
        }

        // `pg_dump --table` doesn't dump the schema the table is in.
        let create_schema = format!("CREATE SCHEMA IF NOT EXISTS {}", table.schema);
        for shard in state.destination.shards() {
            let mut server = shard.primary(&Request::default()).await?;
            server.execute_checked(create_schema.as_str()).await?;
        }

        self.create_publication(&state.source, &table.name).await?;

        info!(
            "moving table {} [{}], publication \"{}\"",
            table.name, state, self.publication
        );

        Ok(())
    }

    /// Drop the publication. Never touches the source table itself.
    pub(crate) async fn finish(&self, state: &ReshardingState, copied: bool) {
        if copied {
            info!(
                "table \"{}\" is left in place on \"{}\"; drop it once the application uses \"{}\"",
                self.table,
                state.source.identifier().database,
                state.destination.identifier().database
            );
        }

        self.drop_publication(&state.source).await;
    }

    async fn resolve(&self, source: &Cluster) -> Result<SourceTable, Error> {
        let mut server = source.primary(0, &Request::default()).await?;
        let rows: Vec<DataRow> = server
            .fetch_all(format!(
                "SELECT DISTINCT format('%I.%I', n.nspname, c.relname), format('%I', n.nspname), \
                 CASE WHEN r.oid IS NULL THEN '' ELSE format('%I.%I', rn.nspname, r.relname) END \
                 FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 LEFT JOIN pg_constraint con \
                   ON con.conrelid = c.oid AND con.contype = 'f' AND con.confrelid <> c.oid \
                 LEFT JOIN pg_class r ON r.oid = con.confrelid \
                 LEFT JOIN pg_namespace rn ON rn.oid = r.relnamespace \
                 WHERE c.oid = {}::regclass",
                quote_literal(&self.table)
            ))
            .await?;

        let first = rows.first().ok_or(Error::MissingData)?;
        Ok(SourceTable {
            name: first.get_text(0).ok_or(Error::MissingData)?,
            schema: first.get_text(1).ok_or(Error::MissingData)?,
            referenced: rows
                .iter()
                .filter_map(|row| row.get_text(2))
                .filter(|name| !name.is_empty())
                .collect(),
        })
    }

    async fn create_publication(&self, source: &Cluster, table: &str) -> Result<(), Error> {
        let sql = format!(
            "CREATE PUBLICATION \"{}\" FOR TABLE {table}",
            escape_identifier(&self.publication),
        );

        // Publications are per database, and shards can share one.
        let mut created = HashSet::new();
        for shard in source.shards() {
            let mut server = shard.primary(&Request::default()).await?;
            let addr = server.addr();
            if created.insert((addr.host.clone(), addr.port, addr.database_name.clone())) {
                server.execute_checked(sql.as_str()).await?;
            }
        }

        Ok(())
    }

    async fn drop_publication(&self, source: &Cluster) {
        let sql = format!(
            "DROP PUBLICATION IF EXISTS \"{}\"",
            escape_identifier(&self.publication)
        );

        for shard in source.shards() {
            let result: Result<(), Error> = async {
                let mut server = shard.primary(&Request::default()).await?;
                server.execute_checked(sql.as_str()).await?;
                Ok(())
            }
            .await;

            if let Err(err) = result {
                warn!(
                    "failed to drop publication \"{}\" on shard {}: {err}",
                    self.publication,
                    shard.number()
                );
            }
        }
    }
}

/// The first destination shard missing any of `tables`, with the missing ones.
async fn missing_tables(
    dest: &Cluster,
    tables: &[String],
) -> Result<Option<(usize, Vec<String>)>, Error> {
    if tables.is_empty() {
        return Ok(None);
    }

    let names = tables
        .iter()
        .map(|name| quote_literal(name))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT name FROM unnest(ARRAY[{names}]::text[]) AS name WHERE to_regclass(name) IS NULL"
    );

    for shard in dest.shards() {
        let mut server = shard.primary(&Request::default()).await?;
        let missing: Vec<String> = server.fetch_all(sql.as_str()).await?;
        if !missing.is_empty() {
            return Ok(Some((shard.number(), missing)));
        }
    }

    Ok(None)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::backend::server::test::test_server;
    use crate::config::config;

    #[tokio::test]
    async fn resolves_the_table_and_finds_missing_referenced_tables() {
        crate::logger();
        let mut server = test_server().await;
        for ddl in [
            "DROP TABLE IF EXISTS pgdog.table_move_child, pgdog.table_move_parent",
            "CREATE TABLE pgdog.table_move_parent (id BIGINT PRIMARY KEY)",
            "CREATE TABLE pgdog.table_move_child (id BIGINT PRIMARY KEY, \
             parent_id BIGINT REFERENCES pgdog.table_move_parent(id), \
             self_id BIGINT REFERENCES pgdog.table_move_child(id))",
        ] {
            server.execute_checked(ddl).await.unwrap();
        }

        let cluster = Cluster::new_test(&config());
        cluster.launch();

        // Unquoted names fold to lowercase, as in SQL.
        let child = TableMove::new("PGDOG.Table_Move_Child".into())
            .resolve(&cluster)
            .await
            .unwrap();
        assert_eq!(child.name, "pgdog.table_move_child");
        assert_eq!(child.schema, "pgdog");
        assert_eq!(child.referenced, ["pgdog.table_move_parent"]);
        assert_eq!(
            missing_tables(&cluster, &child.referenced).await.unwrap(),
            None
        );
        assert_eq!(
            missing_tables(
                &cluster,
                &[
                    "pgdog.table_move_parent".to_owned(),
                    "pgdog.table_move_absent".to_owned()
                ]
            )
            .await
            .unwrap(),
            Some((0, vec!["pgdog.table_move_absent".to_owned()]))
        );

        let err = TableMove::new("pgdog.table_move_absent".into())
            .resolve(&cluster)
            .await
            .err()
            .expect("table is missing on the source");
        assert!(err.to_string().contains("does not exist"), "{err}");

        cluster.shutdown();
        server
            .execute_checked("DROP TABLE pgdog.table_move_child, pgdog.table_move_parent")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn publication_is_created_once_when_shards_share_a_database() {
        crate::logger();
        let mut server = test_server().await;
        server
            .execute_checked(
                "CREATE TABLE IF NOT EXISTS pgdog.table_move_publication (id BIGINT PRIMARY KEY)",
            )
            .await
            .unwrap();

        // Both test shards point at the same database.
        let cluster = Cluster::new_test(&config());
        cluster.launch();

        let table_move = TableMove::new("pgdog.table_move_publication".into());
        table_move
            .create_publication(&cluster, "pgdog.table_move_publication")
            .await
            .unwrap();
        table_move.drop_publication(&cluster).await;

        cluster.shutdown();
        server
            .execute_checked("DROP TABLE pgdog.table_move_publication")
            .await
            .unwrap();
    }
}
