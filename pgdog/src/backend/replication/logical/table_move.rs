//! Move a single table between databases.
//!
//! Reuses the resharding machinery: the table is wrapped in a publication
//! that PgDog creates and drops, then copied and replicated until the task
//! is stopped. There is no cutover; the application switches databases itself.

use std::collections::HashSet;
use std::fmt::{self, Display};
use std::str::FromStr;

use tracing::{info, warn};

use super::Error;
use super::resharding_state::ReshardingState;
use crate::backend::Cluster;
use crate::backend::pool::Request;
use crate::util::sql::quote_literal;
use crate::util::{escape_identifier, random_string};

/// Table name that isn't `table` or `schema.table`.
#[derive(Debug, thiserror::Error)]
#[error("invalid table name \"{0}\", expected \"table\" or \"schema.table\"")]
pub(crate) struct InvalidTableName(String);

/// Schema-qualified table name. As in Postgres, unquoted parts are folded
/// to lowercase. A missing schema means `public`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QualifiedTable {
    pub(crate) schema: String,
    pub(crate) name: String,
}

impl QualifiedTable {
    fn regclass(&self) -> String {
        format!("{}::regclass", quote_literal(&self.to_string()))
    }
}

impl Display for QualifiedTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "\"{}\".\"{}\"",
            escape_identifier(&self.schema),
            escape_identifier(&self.name)
        )
    }
}

impl FromStr for QualifiedTable {
    type Err = InvalidTableName;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let invalid = || InvalidTableName(input.to_owned());
        let mut parts = identifiers(input).ok_or_else(invalid)?;
        let name = parts.pop().ok_or_else(invalid)?;
        let schema = parts.pop().unwrap_or_else(|| "public".to_owned());

        if !parts.is_empty() {
            return Err(invalid());
        }

        Ok(Self { schema, name })
    }
}

/// Split a dotted name into identifiers, unquoting `"..."` parts.
fn identifiers(input: &str) -> Option<Vec<String>> {
    let mut parts = vec![];
    let mut chars = input.chars().peekable();

    loop {
        let mut part = String::new();

        if chars.next_if_eq(&'"').is_some() {
            loop {
                match chars.next()? {
                    '"' if chars.next_if_eq(&'"').is_some() => part.push('"'),
                    '"' => break,
                    c => part.push(c),
                }
            }
        } else {
            while let Some(c) = chars.next_if(|c| *c != '.') {
                if c == '"' || c.is_whitespace() {
                    return None;
                }
                part.push(c.to_ascii_lowercase());
            }
        }

        if part.is_empty() {
            return None;
        }
        parts.push(part);

        match chars.next() {
            None => return Some(parts),
            Some('.') => continue,
            Some(_) => return None,
        }
    }
}

/// A single table moved between databases.
#[derive(Debug, Clone)]
pub(crate) struct TableMove {
    table: QualifiedTable,
    publication: String,
}

impl TableMove {
    pub(crate) fn new(table: QualifiedTable) -> Self {
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

        let referenced = self.referenced_tables(&state.source).await?;
        if let Some((shard, missing)) = missing_tables(&state.destination, &referenced).await? {
            return Err(Error::ReferencedTablesMissing {
                table: self.table.to_string(),
                shard,
                missing: missing.join(", "),
            });
        }

        // `pg_dump --table` doesn't dump the schema the table is in.
        let create_schema = format!(
            "CREATE SCHEMA IF NOT EXISTS \"{}\"",
            escape_identifier(&self.table.schema)
        );
        for shard in state.destination.shards() {
            let mut server = shard.primary(&Request::default()).await?;
            server.execute_checked(create_schema.as_str()).await?;
        }

        if let Err(err) = self.create_publication(&state.source).await {
            self.drop_publication(&state.source).await;
            return Err(err);
        }

        info!(
            "moving table {} [{}], publication \"{}\"",
            self.table, state, self.publication
        );

        Ok(())
    }

    /// Drop the publication. Never touches the source table itself.
    pub(crate) async fn finish(&self, state: &ReshardingState, copied: bool) {
        if copied {
            info!(
                "table {} is left in place on \"{}\"; drop it once the application uses \"{}\"",
                self.table,
                state.source.identifier().database,
                state.destination.identifier().database
            );
        }

        self.drop_publication(&state.source).await;
    }

    /// Tables the moved table references with a foreign key, other than itself.
    async fn referenced_tables(&self, source: &Cluster) -> Result<Vec<String>, Error> {
        let mut server = source.primary(0, &Request::default()).await?;
        Ok(server
            .fetch_all(format!(
                "SELECT DISTINCT format('%I.%I', n.nspname, c.relname) \
                 FROM pg_constraint con \
                 JOIN pg_class c ON c.oid = con.confrelid \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE con.contype = 'f' AND con.conrelid = {} AND con.confrelid <> con.conrelid",
                self.table.regclass()
            ))
            .await?)
    }

    async fn create_publication(&self, source: &Cluster) -> Result<(), Error> {
        let sql = format!(
            "CREATE PUBLICATION \"{}\" FOR TABLE {}",
            escape_identifier(&self.publication),
            self.table
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

    fn table(schema: &str, name: &str) -> QualifiedTable {
        QualifiedTable {
            schema: schema.into(),
            name: name.into(),
        }
    }

    #[test]
    fn parses_table_names() {
        for (input, expected) in [
            ("orders", table("public", "orders")),
            ("Orders", table("public", "orders")),
            ("sales.orders", table("sales", "orders")),
            ("\"Sales\".\"Orders\"", table("Sales", "Orders")),
            ("sales.\"my.table\"", table("sales", "my.table")),
            ("\"a\"\"b\".c", table("a\"b", "c")),
        ] {
            assert_eq!(
                input.parse::<QualifiedTable>().unwrap(),
                expected,
                "{input}"
            );
        }
    }

    #[test]
    fn rejects_invalid_table_names() {
        for input in [
            "",
            ".",
            "a.",
            ".a",
            "a..b",
            "db.sales.orders",
            "\"unterminated",
            "\"\"",
            "a\"b",
            "\"a\"b",
            "my table",
        ] {
            assert!(input.parse::<QualifiedTable>().is_err(), "{input:?}");
        }
    }

    #[test]
    fn displays_quoted() {
        assert_eq!(table("a\"b", "c").to_string(), "\"a\"\"b\".\"c\"");
    }

    #[tokio::test]
    async fn destination_checks_find_missing_tables() {
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

        let child = TableMove::new(table("pgdog", "table_move_child"));
        let referenced = child.referenced_tables(&cluster).await.unwrap();
        assert_eq!(referenced, vec!["pgdog.table_move_parent".to_owned()]);
        assert_eq!(missing_tables(&cluster, &referenced).await.unwrap(), None);
        assert_eq!(missing_tables(&cluster, &[]).await.unwrap(), None);
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

        let err = TableMove::new(table("pgdog", "table_move_absent"))
            .referenced_tables(&cluster)
            .await
            .expect_err("table is missing on the source");
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

        let table_move = TableMove::new(table("pgdog", "table_move_publication"));
        table_move.create_publication(&cluster).await.unwrap();
        table_move.drop_publication(&cluster).await;

        cluster.shutdown();
        server
            .execute_checked("DROP TABLE pgdog.table_move_publication")
            .await
            .unwrap();
    }
}
