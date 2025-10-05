//! Wrapper around pg_dump.

use std::{ops::Deref, str::from_utf8};

use lazy_static::lazy_static;
use pg_query::{
    protobuf::{AlterTableType, ConstrType, ObjectType, ParseResult},
    NodeEnum,
};
use regex::Regex;
use tracing::{info, trace, warn};

use super::{progress::Progress, Error};
use crate::{
    backend::{self, pool::Request, replication::publisher::PublicationTable, Cluster},
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

    fn clean(source: &str) -> String {
        lazy_static! {
            static ref CLEANUP_RE: Regex = Regex::new(r"(?m)^\\(?:un)?restrict.*\n?").unwrap();
        }
        let cleaned = CLEANUP_RE.replace_all(source, "");

        cleaned.to_string()
    }

    /// Dump schema from source cluster.
    pub async fn dump(&self) -> Result<PgDumpOutput, Error> {
        let mut comparison: Vec<PublicationTable> = vec![];
        let addr = self
            .source
            .shards()
            .first()
            .ok_or(Error::NoDatabases)?
            .pools()
            .iter()
            .next()
            .ok_or(Error::NoDatabases)?
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
            }
        }

        if comparison.is_empty() {
            return Err(Error::PublicationNoTables(self.publication.clone()));
        }

        info!("dumping schema [{}, {}]", comparison.len(), addr,);

        let config = config();
        let pg_dump_path = config
            .config
            .replication
            .pg_dump_path
            .to_str()
            .unwrap_or("pg_dump");

        let output = Command::new(pg_dump_path)
            .arg("--schema-only")
            .arg("-h")
            .arg(&addr.host)
            .arg("-p")
            .arg(addr.port.to_string())
            .arg("-U")
            .arg(&addr.user)
            .env("PGPASSWORD", &addr.password)
            .arg("-d")
            .arg(&addr.database_name)
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
        })
    }
}

#[derive(Debug, Clone)]
pub struct PgDumpOutput {
    stmts: ParseResult,
    original: String,
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
        sql: String,
    },

    Table {
        table: Table<'a>,
        sql: String,
    },

    Other {
        sql: String,
        idempotent: bool,
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
            Self::Other { sql, .. } => sql,
            Self::SequenceSetMax { sql, .. } => sql.as_str(),
        }
    }
}

impl<'a> From<&'a str> for Statement<'a> {
    fn from(value: &'a str) -> Self {
        Self::Other {
            sql: value.to_string(),
            idempotent: true,
        }
    }
}

impl<'a> From<String> for Statement<'a> {
    fn from(value: String) -> Self {
        Self::Other {
            sql: value,
            idempotent: true,
        }
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
                            let sql = {
                                let mut stmt = stmt.clone();
                                stmt.if_not_exists = true;
                                NodeEnum::CreateStmt(stmt).deparse()?
                            };
                            if state == SyncState::PreData {
                                // CREATE TABLE is always good.
                                let table =
                                    stmt.relation.as_ref().map(Table::from).unwrap_or_default();
                                result.push(Statement::Table { table, sql });
                            }
                        }

                        NodeEnum::CreateSeqStmt(stmt) => {
                            let mut stmt = stmt.clone();
                            stmt.if_not_exists = true;
                            let sql = NodeEnum::CreateSeqStmt(stmt).deparse()?;
                            if state == SyncState::PreData {
                                // Bring sequences over.
                                result.push(sql.into());
                            }
                        }

                        NodeEnum::CreateExtensionStmt(stmt) => {
                            let mut stmt = stmt.clone();
                            stmt.if_not_exists = true;
                            let sql = NodeEnum::CreateExtensionStmt(stmt).deparse()?;
                            if state == SyncState::PreData {
                                result.push(sql.into());
                            }
                        }

                        NodeEnum::CreateSchemaStmt(stmt) => {
                            let mut stmt = stmt.clone();
                            stmt.if_not_exists = true;
                            let sql = NodeEnum::CreateSchemaStmt(stmt).deparse()?;
                            if state == SyncState::PreData {
                                result.push(sql.into());
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
                                                            result.push(Statement::Other {
                                                                sql: original.to_string(),
                                                                idempotent: false,
                                                            });
                                                        }
                                                    } else if state == SyncState::PostData {
                                                        result.push(Statement::Other {
                                                            sql: original.to_string(),
                                                            idempotent: false,
                                                        });
                                                    }
                                                }
                                            }
                                        }
                                        AlterTableType::AtAttachPartition => {
                                            // Index partitions need to be attached to indexes,
                                            // which we create in the post-data step.
                                            match stmt.objtype() {
                                                ObjectType::ObjectIndex => {
                                                    if state == SyncState::PostData {
                                                        result.push(Statement::Other {
                                                            sql: original.to_string(),
                                                            idempotent: false,
                                                        });
                                                    }
                                                }

                                                _ => {
                                                    if state == SyncState::PreData {
                                                        result.push(Statement::Other {
                                                            sql: original.to_string(),
                                                            idempotent: false,
                                                        });
                                                    }
                                                }
                                            }
                                        }

                                        AlterTableType::AtColumnDefault => {
                                            if state == SyncState::PreData {
                                                result.push(original.into())
                                            }
                                        }
                                        // AlterTableType::AtChangeOwner => {
                                        //     continue; // Don't change owners, for now.
                                        // }
                                        _ => {
                                            if state == SyncState::PreData {
                                                result.push(original.into());
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        NodeEnum::CreateTrigStmt(stmt) => {
                            let mut stmt = stmt.clone();
                            stmt.replace = true;

                            if state == SyncState::PreData {
                                result.push(NodeEnum::CreateTrigStmt(stmt).deparse()?.into());
                            }
                        }

                        // Skip these.
                        NodeEnum::CreatePublicationStmt(_)
                        | NodeEnum::CreateSubscriptionStmt(_)
                        | NodeEnum::AlterPublicationStmt(_)
                        | NodeEnum::AlterSubscriptionStmt(_) => (),

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
                                let sql = {
                                    let mut stmt = stmt.clone();
                                    stmt.concurrent = stmt
                                        .relation
                                        .as_ref()
                                        .map(|relation| relation.inh) // ONLY used for partitioned tables, which can't be created concurrently.
                                        .unwrap_or(false);
                                    stmt.if_not_exists = true;
                                    NodeEnum::IndexStmt(stmt).deparse()?
                                };

                                let table =
                                    stmt.relation.as_ref().map(Table::from).unwrap_or_default();

                                result.push(Statement::Index {
                                    table,
                                    name: stmt.idxname.as_str(),
                                    sql,
                                });
                            }
                        }

                        NodeEnum::ViewStmt(stmt) => {
                            let mut stmt = stmt.clone();
                            stmt.replace = true;

                            if state == SyncState::PreData {
                                result.push(Statement::Other {
                                    sql: NodeEnum::ViewStmt(stmt).deparse()?,
                                    idempotent: true,
                                });
                            }
                        }

                        NodeEnum::CreateTableAsStmt(stmt) => {
                            let mut stmt = stmt.clone();
                            stmt.if_not_exists = true;

                            if state == SyncState::PreData {
                                result.push(Statement::Other {
                                    sql: NodeEnum::CreateTableAsStmt(stmt).deparse()?,
                                    idempotent: true,
                                });
                            }
                        }

                        NodeEnum::CreateFunctionStmt(stmt) => {
                            let mut stmt = stmt.clone();
                            stmt.replace = true;

                            if state == SyncState::PreData {
                                result.push(Statement::Other {
                                    sql: NodeEnum::CreateFunctionStmt(stmt).deparse()?,
                                    idempotent: true,
                                });
                            }
                        }

                        NodeEnum::CreateEnumStmt(_)
                        | NodeEnum::CreateDomainStmt(_)
                        | NodeEnum::CompositeTypeStmt(_) => {
                            if state == SyncState::PreData {
                                result.push(Statement::Other {
                                    sql: original.to_owned(),
                                    idempotent: false,
                                });
                            }
                        }

                        NodeEnum::VariableSetStmt(_) => continue,
                        NodeEnum::SelectStmt(_) => continue,
                        _ => {
                            if state == SyncState::PreData {
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
                "syncing schema into shard {} [{}, {}]",
                num,
                primary.addr(),
                dest.name()
            );

            let mut progress = Progress::new(stmts.len());

            for stmt in &stmts {
                progress.next(stmt);
                if let Err(err) = primary.execute(stmt.deref()).await {
                    if let backend::Error::ExecutionError(ref err) = err {
                        let code = &err.code;

                        if let Statement::Other { idempotent, .. } = stmt {
                            if !idempotent {
                                if matches!(code.as_str(), "42P16" | "42710" | "42809" | "42P07") {
                                    warn!("entity already exists, skipping");
                                    continue;
                                } else if !ignore_errors {
                                    return Err(Error::Backend(backend::Error::ExecutionError(
                                        err.clone(),
                                    )));
                                } else {
                                    warn!("skipping: {}", err);
                                }
                            }
                        }
                    } else {
                        return Err(err.into());
                    }
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
    use super::*;

    #[tokio::test]
    async fn test_pg_dump_execute() {
        let cluster = Cluster::new_test_single_shard();
        let _pg_dump = PgDump::new(&cluster, "test_pg_dump_execute");
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
        let _parse = pg_query::parse(&PgDump::clean(&dump)).unwrap();
    }
}
