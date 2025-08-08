//! Wrapper around pg_dump.

use std::str::{from_utf8, from_utf8_unchecked};

use pg_query::{
    protobuf::{ConstrType, ParseResult},
    NodeEnum,
};
use tracing::warn;

use super::Error;
use crate::backend::{
    pool::{Address, Request},
    replication::publisher::PublicationTable,
    Cluster,
};

use tokio::process::Command;

#[derive(Debug, Clone)]
pub struct PgDump {
    source: Cluster,
    publication: String,
}

impl PgDump {
    pub async fn new(source: &Cluster, publication: &str) -> Self {
        Self {
            source: source.clone(),
            publication: publication.to_string(),
        }
    }

    pub async fn dump(&self) -> Result<(), Error> {
        let mut comparison: Vec<PublicationTable> = vec![];
        for (num, shard) in self.source.shards().iter().enumerate() {
            let mut server = shard.primary_or_replica(&Request::default()).await?;
            let tables = PublicationTable::load(&self.publication, &mut server).await?;
            if comparison.is_empty() {
                comparison.extend(tables);
            } else {
                if comparison != tables {
                    warn!("shard {} tables are different [{}]", num, server.addr());
                    continue;
                }
            }
        }
        todo!()
    }
}

struct PgDumpCommand {
    table: String,
    schema: String,
    address: Address,
}

impl PgDumpCommand {
    async fn execute(&self) -> Result<PgDumpOutput, Error> {
        let output = Command::new("pg_dump")
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
        let stmts = pg_query::parse(&original)?.protobuf;

        Ok(PgDumpOutput { stmts, original })
    }
}

#[derive(Debug, Clone)]
pub struct PgDumpOutput {
    stmts: ParseResult,
    original: String,
}

impl PgDumpOutput {
    /// Get schema statements to execute before data sync,
    /// e.g., CREATE TABLE, primary key.
    pub fn pre_data_sync(&self) -> Result<Vec<&str>, Error> {
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
                        NodeEnum::CreateStmt(_) => {
                            // CREATE TABLE is always good.
                            result.push(original);
                        }

                        NodeEnum::AlterTableStmt(stmt) => {
                            for cmd in &stmt.cmds {
                                if let Some(ref node) = cmd.node {
                                    if let NodeEnum::AlterTableCmd(cmd) = node {
                                        if let Some(ref def) = cmd.def {
                                            if let Some(ref node) = def.node {
                                                // Only allow primary key constraints.
                                                if let NodeEnum::Constraint(cons) = node {
                                                    if cons.contype() == ConstrType::ConstrPrimary {
                                                        result.push(original);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        NodeEnum::IndexStmt(_) => {
                            continue;
                        }

                        _ => (),
                    }
                }
            }
        }

        Ok(result)
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
            "CREATE TABLE IF NOT EXISTS test_pg_dump_execute(id BIGINT PRIMARY KEY, email VARCHAR UNIQUE, created_at TIMESTAMPTZ)",
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

        let output = output.pre_data_sync().unwrap();

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

        for stmt in output {
            // Hack around us using the same database as destination.
            // I know, not very elegant.
            let stmt = stmt.replace("pgdog.", "test_pg_dump_execute_dest.");
            dest.execute(stmt).await.unwrap();
        }

        dest.execute("SELECT * FROM test_pg_dump_execute_dest.test_pg_dump_execute")
            .await
            .unwrap();
        dest.execute("DROP SCHEMA test_pg_dump_execute_dest CASCADE")
            .await
            .unwrap();

        server
            .execute("DROP TABLE test_pg_dump_execute CASCADE")
            .await
            .unwrap();
    }
}
