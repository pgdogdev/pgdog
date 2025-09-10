//! Wrapper around pg_dump.

use std::{ops::Deref, str::from_utf8};

use lazy_static::lazy_static;
use pg_query::{
    protobuf::{AlterTableType, ConstrType, ParseResult},
    NodeEnum,
};
use regex::Regex;
use tracing::{info, trace, warn};

use super::{progress::Progress, Error};
use crate::{
    backend::{
        pool::{Address, Request},
        replication::publisher::PublicationTable,
        Cluster,
    },
    config::config,
    frontend::router::parser::{sequence::Sequence, Column, Table},
};

use tokio::process::Command;

#[derive(Debug, Clone)]
pub struct PgDump {
    source: Cluster,
    publication: String,
}

impl PgDump {
    pub fn new(source: &Cluster, publication: &str) -> Self {
        Self {
            source: source.clone(),
            publication: publication.to_string(),
        }
    }

    /// Dump schema from source cluster.
    pub async fn dump(&self) -> Result<Vec<PgDumpOutput>, Error> {
        let mut comparison: Vec<PublicationTable> = vec![];
        let addr = self
            .source
            .shards()
            .first()
            .ok_or(Error::NoDatabases)?
            .primary_or_replica(&Request::default())
            .await?
            .addr()
            .clone();

        info!(
            "loading tables from publication \"{}\" on {} shards [{}]",
            self.publication,
            self.source.shards().len(),
            self.source.name(),
        );

        for (num, shard) in self.source.shards().iter().enumerate() {
            let mut server = shard.primary_or_replica(&Request::default()).await?;
            let tables = PublicationTable::load(&self.publication, &mut server).await?;
            if comparison.is_empty() {
                comparison.extend(tables);
            } else if comparison != tables {
                warn!(
                    "shard {} tables are different [{}, {}]",
                    num,
                    server.addr(),
                    self.source.name()
                );
                continue;
            }
        }

        let mut result = vec![];
        info!(
            "dumping schema for {} tables [{}, {}]",
            comparison.len(),
            addr,
            self.source.name()
        );

        for table in comparison {
            let cmd = PgDumpCommand {
                table: table.name.clone(),
                schema: table.schema.clone(),
                address: addr.clone(),
            };

            let dump = cmd.execute().await?;
            result.push(dump);
        }

        Ok(result)
    }
}

struct PgDumpCommand {
    table: String,
    schema: String,
    address: Address,
}

impl PgDumpCommand {
    fn clean(source: &str) -> String {
        lazy_static! {
            static ref CLEANUP_RE: Regex = Regex::new(r"(?m)^\\(?:un)?restrict.*\n?").unwrap();
        }
        let cleaned = CLEANUP_RE.replace_all(source, "");

        cleaned.to_string()
    }

    async fn execute(&self) -> Result<PgDumpOutput, Error> {
        let config = config();
        let pg_dump_path = config
            .config
            .replication
            .pg_dump_path
            .to_str()
            .unwrap_or("pg_dump");
        let output = Command::new(pg_dump_path)
            .arg("-t")
            .arg(&self.table)
            .arg("-n")
            .arg(&self.schema)
            .arg("--schema-only")
            .arg("-h")
            .arg(&self.address.host)
            .arg("-p")
            .arg(self.address.port.to_string())
            .arg("-U")
            .arg(&self.address.user)
            .env("PGPASSWORD", &self.address.password)
            .arg("-d")
            .arg(&self.address.database_name)
            .output()
            .await?;

        if !output.status.success() {
            let err = from_utf8(&output.stderr)?;
            return Err(Error::PgDump(err.to_string()));
        }

        let original = from_utf8(&output.stdout)?.to_string();
        trace!("[pg_dump (original)] {}", original);

        let cleaned = Self::clean(&original);
        trace!("[pg_dump (clean)] {}", cleaned);

        let stmts = pg_query::parse(&cleaned)?.protobuf;

        Ok(PgDumpOutput {
            stmts,
            original: cleaned,
            table: self.table.clone(),
            schema: self.schema.clone(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct PgDumpOutput {
    stmts: ParseResult,
    original: String,
    pub table: String,
    pub schema: String,
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum SyncState {
    PreData,
    PostData,
    Cutover,
}

#[derive(Debug)]
pub enum Statement<'a> {
    Index {
        table: Table<'a>,
        name: &'a str,
        sql: &'a str,
    },

    Table {
        table: Table<'a>,
        sql: &'a str,
    },

    Other {
        sql: &'a str,
    },

    SequenceOwner {
        column: Column<'a>,
        sequence: Sequence<'a>,
        sql: &'a str,
    },

    SequenceSetMax {
        sequence: Sequence<'a>,
        sql: String,
    },
}

impl<'a> Deref for Statement<'a> {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Index { sql, .. } => sql,
            Self::Table { sql, .. } => sql,
            Self::SequenceOwner { sql, .. } => sql,
            Self::Other { sql } => sql,
            Self::SequenceSetMax { sql, .. } => sql.as_str(),
        }
    }
}

impl<'a> From<&'a str> for Statement<'a> {
    fn from(value: &'a str) -> Self {
        Self::Other { sql: value }
    }
}

impl PgDumpOutput {
    /// Get schema statements to execute before data sync,
    /// e.g., CREATE TABLE, primary key.
    pub fn statements(&self, state: SyncState) -> Result<Vec<Statement<'_>>, Error> {
        let mut result = vec![];

        for stmt in &self.stmts.stmts {
            let (_, original_start) = self
                .original
                .split_at_checked(stmt.stmt_location as usize)
                .ok_or(Error::StmtOutOfBounds)?;
            let (original, _) = original_start
                .split_at_checked(stmt.stmt_len as usize)
                .ok_or(Error::StmtOutOfBounds)?;

            if let Some(ref node) = stmt.stmt {
                if let Some(ref node) = node.node {
                    match node {
                        NodeEnum::CreateStmt(stmt) => {
                            if state == SyncState::PreData {
                                // CREATE TABLE is always good.
                                let table =
                                    stmt.relation.as_ref().map(Table::from).unwrap_or_default();
                                result.push(Statement::Table {
                                    table,
                                    sql: original,
                                });
                            }
                        }

                        NodeEnum::CreateSeqStmt(_) => {
                            if state == SyncState::PreData {
                                // Bring sequences over.
                                result.push(original.into());
                            }
                        }

                        NodeEnum::AlterTableStmt(stmt) => {
                            for cmd in &stmt.cmds {
                                if let Some(NodeEnum::AlterTableCmd(ref cmd)) = cmd.node {
                                    match cmd.subtype() {
                                        AlterTableType::AtAddConstraint => {
                                            if let Some(ref def) = cmd.def {
                                                if let Some(NodeEnum::Constraint(ref cons)) =
                                                    def.node
                                                {
                                                    // Only allow primary key constraints.
                                                    if matches!(
                                                        cons.contype(),
                                                        ConstrType::ConstrPrimary
                                                            | ConstrType::ConstrNotnull
                                                            | ConstrType::ConstrNull
                                                    ) {
                                                        if state == SyncState::PreData {
                                                            result.push(original.into());
                                                        }
                                                    } else if state == SyncState::PostData {
                                                        result.push(original.into());
                                                    }
                                                }
                                            }
                                        }
                                        AlterTableType::AtColumnDefault => {
                                            if state == SyncState::PreData {
                                                result.push(original.into())
                                            }
                                        }
                                        AlterTableType::AtChangeOwner => {
                                            continue; // Don't change owners, for now.
                                        }
                                        _ => {
                                            if state == SyncState::PostData {
                                                result.push(original.into());
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        NodeEnum::AlterSeqStmt(stmt) => {
                            if matches!(state, SyncState::PreData | SyncState::Cutover) {
                                let sequence = stmt
                                    .sequence
                                    .as_ref()
                                    .map(Table::from)
                                    .ok_or(Error::MissingEntity)?;
                                let sequence = Sequence::from(sequence);
                                let column = stmt.options.first().ok_or(Error::MissingEntity)?;
                                let column =
                                    Column::try_from(column).map_err(|_| Error::MissingEntity)?;

                                if state == SyncState::PreData {
                                    result.push(Statement::SequenceOwner {
                                        column,
                                        sequence,
                                        sql: original,
                                    });
                                } else {
                                    let sql = sequence
                                        .setval_from_column(&column)
                                        .map_err(|_| Error::MissingEntity)?;
                                    result.push(Statement::SequenceSetMax { sequence, sql })
                                }
                            }
                        }

                        NodeEnum::IndexStmt(stmt) => {
                            if state == SyncState::PostData {
                                let table =
                                    stmt.relation.as_ref().map(Table::from).unwrap_or_default();
                                result.push(Statement::Index {
                                    table,
                                    name: stmt.idxname.as_str(),
                                    sql: original,
                                });
                            }
                        }

                        NodeEnum::VariableSetStmt(_) => continue,
                        NodeEnum::SelectStmt(_) => continue,

                        _ => {
                            if state == SyncState::PostData {
                                result.push(original.into());
                            }
                        }
                    }
                }
            }
        }

        Ok(result)
    }

    /// Create objects in destination cluster.
    pub async fn restore(
        &self,
        dest: &Cluster,
        ignore_errors: bool,
        state: SyncState,
    ) -> Result<(), Error> {
        let stmts = self.statements(state)?;

        for (num, shard) in dest.shards().iter().enumerate() {
            let mut primary = shard.primary(&Request::default()).await?;

            info!(
                "syncing schema for \"{}\".\"{}\" into shard {} [{}, {}]",
                self.schema,
                self.table,
                num,
                primary.addr(),
                dest.name()
            );

            let progress = Progress::new(stmts.len());

            for stmt in &stmts {
                progress.next(stmt);
                if let Err(err) = primary.execute(stmt.deref()).await {
                    if ignore_errors {
                        warn!("skipping: {}", err);
                    } else {
                        return Err(err.into());
                    }
                }
                progress.done();
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::backend::server::test::test_server;

    use super::*;

    #[tokio::test]
    async fn test_pg_dump_execute() {
        let mut server = test_server().await;

        let queries = vec![
            "DROP PUBLICATION IF EXISTS test_pg_dump_execute",
            "CREATE TABLE IF NOT EXISTS test_pg_dump_execute(id BIGSERIAL PRIMARY KEY, email VARCHAR UNIQUE, created_at TIMESTAMPTZ)",
            "CREATE INDEX ON test_pg_dump_execute USING btree(created_at)",
            "CREATE TABLE IF NOT EXISTS test_pg_dump_execute_fk(fk BIGINT NOT NULL REFERENCES test_pg_dump_execute(id), meta JSONB)",
            "CREATE PUBLICATION test_pg_dump_execute FOR TABLE test_pg_dump_execute, test_pg_dump_execute_fk"
        ];

        for query in queries {
            server.execute(query).await.unwrap();
        }

        let output = PgDumpCommand {
            table: "test_pg_dump_execute".into(),
            schema: "pgdog".into(),
            address: server.addr().clone(),
        }
        .execute()
        .await
        .unwrap();

        let output_pre = output.statements(SyncState::PreData).unwrap();
        let output_post = output.statements(SyncState::PostData).unwrap();
        let output_cutover = output.statements(SyncState::Cutover).unwrap();

        let mut dest = test_server().await;
        dest.execute("DROP SCHEMA IF EXISTS test_pg_dump_execute_dest CASCADE")
            .await
            .unwrap();

        dest.execute("CREATE SCHEMA test_pg_dump_execute_dest")
            .await
            .unwrap();
        dest.execute("SET search_path TO test_pg_dump_execute_dest, public")
            .await
            .unwrap();

        for stmt in output_pre {
            // Hack around us using the same database as destination.
            // I know, not very elegant.
            let stmt = stmt.replace("pgdog.", "test_pg_dump_execute_dest.");
            dest.execute(stmt).await.unwrap();
        }

        for i in 0..5 {
            let id = dest.fetch_all::<i64>("INSERT INTO test_pg_dump_execute_dest.test_pg_dump_execute VALUES (DEFAULT, 'test@test', NOW()) RETURNING id")
                .await
                .unwrap();
            assert_eq!(id[0], i + 1); // Sequence has made it over.

            // Unique index didn't make it over.
        }

        dest.execute("DELETE FROM test_pg_dump_execute_dest.test_pg_dump_execute")
            .await
            .unwrap();

        for stmt in output_post {
            let stmt = stmt.replace("pgdog.", "test_pg_dump_execute_dest.");
            dest.execute(stmt).await.unwrap();
        }

        let q = "INSERT INTO test_pg_dump_execute_dest.test_pg_dump_execute VALUES (DEFAULT, 'test@test', NOW()) RETURNING id";
        assert!(dest.execute(q).await.is_ok());
        let err = dest.execute(q).await.err().unwrap();
        assert!(err.to_string().contains(
            r#"duplicate key value violates unique constraint "test_pg_dump_execute_email_key""#
        )); // Unique index made it over.

        assert_eq!(output_cutover.len(), 1);
        for stmt in output_cutover {
            let stmt = stmt.replace("pgdog.", "test_pg_dump_execute_dest.");
            assert!(stmt.starts_with("SELECT setval('"));
            dest.execute(stmt).await.unwrap();
        }

        dest.execute("DROP SCHEMA test_pg_dump_execute_dest CASCADE")
            .await
            .unwrap();

        server
            .execute("DROP TABLE test_pg_dump_execute CASCADE")
            .await
            .unwrap();
    }

    #[test]
    fn test_specific_dump() {
        let dump = r#"
-- PostgreSQL database dump
--

\restrict nu6jB5ogH2xGMn2dB3dMyMbSZ2PsVDqB2IaWK6zZVjngeba0UrnmxMy6s63SwzR

-- Dumped from database version 16.6
-- Dumped by pg_dump version 16.10 (Ubuntu 16.10-0ubuntu0.24.04.1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: users; Type: TABLE; Schema: public; Owner: pgdog-4
--

CREATE TABLE public.users (
    id bigint NOT NULL,
    email character varying NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.users OWNER TO "pgdog-4";

--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: pgdog-4
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- PostgreSQL database dump complete
--

\unrestrict nu6jB5ogH2xGMn2dB3dMyMbSZ2PsVDqB2IaWK6zZVjngeba0UrnmxMy6s63SwzR
"#;
        let clean = PgDumpCommand::clean(&dump);
        let _parse = pg_query::parse(&PgDumpCommand::clean(&dump)).unwrap();
    }
}
