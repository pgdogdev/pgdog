use std::{
    collections::HashMap,
    sync::atomic::{AtomicUsize, Ordering},
};

use once_cell::sync::Lazy;
use pg_query::{
    protobuf::{InsertStmt, ParseResult},
    NodeEnum,
};
use tracing::trace;

use super::super::{publisher::Table, Error};
use crate::{
    backend::{Cluster, Server, ShardingSchema},
    config::Role,
    frontend::router::parser::{Insert, Shard},
    net::{
        replication::{
            xlog_data::XLogPayload, Commit as XLogCommit, Insert as XLogInsert, Relation,
            StatusUpdate,
        },
        Bind, CopyData, ErrorResponse, Execute, Flush, FromBytes, Parse, Protocol, Sync, ToBytes,
    },
    util::postgres_now,
};

// Unique prepared statement counter.
static STATEMENT_COUNTER: Lazy<AtomicUsize> = Lazy::new(|| AtomicUsize::new(1));
fn statement_name() -> String {
    format!(
        "__pgdog_repl_{}",
        STATEMENT_COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

// Unique identifier for a table in Postgres.
#[derive(Debug, Hash, Clone, PartialEq, Eq)]
struct Key {
    schema: String,
    name: String,
}

#[derive(Default, Debug, Clone)]
struct Statements {
    #[allow(dead_code)]
    insert: Statement,
    upsert: Statement,
    #[allow(dead_code)]
    update: Statement,
}

#[derive(Default, Debug, Clone)]
struct Statement {
    name: String,
    query: String,
    ast: ParseResult,
}

impl Statement {
    fn parse(&self) -> Parse {
        Parse::named(&self.name, &self.query)
    }

    fn new(query: &str) -> Result<Self, Error> {
        let ast = pg_query::parse(query)?.protobuf;
        Ok(Self {
            name: statement_name(),
            query: query.to_string(),
            ast,
        })
    }

    fn insert(&self) -> Option<&Box<InsertStmt>> {
        self.ast
            .stmts
            .first()
            .map(|stmt| {
                stmt.stmt.as_ref().map(|stmt| {
                    stmt.node.as_ref().map(|node| {
                        if let NodeEnum::InsertStmt(ref insert) = node {
                            Some(insert)
                        } else {
                            None
                        }
                    })
                })
            })
            .flatten()
            .flatten()
            .flatten()
    }
}

#[derive(Debug)]
pub struct StreamSubscriber {
    /// Destination cluster.
    cluster: Cluster,

    /// Sharding schema.
    sharding_schema: ShardingSchema,

    // Relation markers sent by the publisher.
    // Happens once per connection.
    relations: HashMap<i32, Relation>,

    // Tables in the publication on the publisher.
    tables: HashMap<Key, Table>,

    // Statements
    statements: HashMap<i32, Statements>,

    // Connections to shards.
    connections: Vec<Server>,

    // Position in the WAL we have flushed successfully.
    lsn: i64,
}

impl StreamSubscriber {
    pub fn new(cluster: &Cluster, tables: &[Table]) -> Self {
        Self {
            cluster: cluster.clone(),
            sharding_schema: cluster.sharding_schema(),
            relations: HashMap::new(),
            statements: HashMap::new(),
            tables: tables
                .into_iter()
                .map(|table| {
                    (
                        Key {
                            schema: table.table.schema.clone(),
                            name: table.table.name.clone(),
                        },
                        table.clone(),
                    )
                })
                .collect(),
            connections: vec![],
            lsn: 0, // Unknown,
        }
    }

    // Connect to all the shards.
    pub async fn connect(&mut self) -> Result<(), Error> {
        let mut conns = vec![];

        for shard in self.cluster.shards() {
            let primary = shard
                .pools_with_roles()
                .iter()
                .filter(|(r, _)| r == &Role::Primary)
                .next()
                .ok_or(Error::NoPrimary)?
                .1
                .standalone()
                .await?;
            conns.push(primary);
        }

        self.connections = conns;

        // Transaction control statements.
        //
        // TODO: Figure out if we need to use them?
        for server in &mut self.connections {
            let begin = Parse::named("__pgdog_repl_begin", "BEGIN");
            let commit = Parse::named("__pgdog_repl_commit", "COMMIT");

            server
                .send(&vec![begin.clone().into(), commit.clone().into(), Sync.into()].into())
                .await?;
            for _ in 0..3 {
                let msg = server.read().await?;
                trace!("[{}] --> {:?}", server.addr(), msg);
                match msg.code() {
                    '1' | 'C' | 'Z' => (),
                    'E' => return Err(Error::PgError(ErrorResponse::from_bytes(msg.to_bytes()?)?)),
                    c => return Err(Error::OutOfSync(c)),
                }
            }
        }

        Ok(())
    }

    // Send an upsert to one or more shards.
    async fn send(&mut self, val: &Shard, bind: &Bind) -> Result<(), Error> {
        for (shard, conn) in self.connections.iter_mut().enumerate() {
            match val {
                Shard::Direct(direct) => {
                    if shard != *direct {
                        continue;
                    }
                }
                Shard::Multi(multi) => {
                    if multi.contains(&shard) {
                        continue;
                    }
                }
                _ => (),
            }

            conn.send(&vec![bind.clone().into(), Execute::new().into(), Flush.into()].into())
                .await?;
        }

        for (shard, conn) in self.connections.iter_mut().enumerate() {
            match val {
                Shard::Direct(direct) => {
                    if shard != *direct {
                        continue;
                    }
                }
                Shard::Multi(multi) => {
                    if multi.contains(&shard) {
                        continue;
                    }
                }
                _ => (),
            }

            Self::read(conn, 2, &['2', 'C']).await?;
        }

        Ok(())
    }

    // Read a set of messages from the server connection. If we receive something
    // we didn't expect, error out.
    async fn read(server: &mut Server, messages: usize, codes: &[char]) -> Result<(), Error> {
        for _ in 0..messages {
            let msg = server.read().await?;

            trace!("[{}] --> {:?}", server.addr(), msg);

            let code = msg.code();
            if code == 'E' {
                return Err(Error::PgError(ErrorResponse::from_bytes(msg.to_bytes()?)?));
            }
            if codes.contains(&code) {
                continue;
            } else {
                return Err(Error::OutOfSync(code));
            }
        }
        Ok(())
    }

    // Handle Insert message.
    //
    // Convert Insert into an idempotent "upsert" and apply it to
    // the right shard(s).
    async fn insert(&mut self, insert: XLogInsert) -> Result<(), Error> {
        let statements = self.statements.get(&insert.oid).ok_or(Error::MissingData)?;

        // Convert TupleData into a Bind message. We can now insert that tuple
        // using a prepared statement.
        let bind = insert.tuple_data.to_bind(&statements.upsert.name);

        // Upserts are idempotent. Even if we rewind the stream,
        // we are able to replay changes we already applied safely.
        if let Some(upsert) = statements.upsert.insert() {
            let upsert = Insert::new(upsert);
            let val = upsert.shard(&self.sharding_schema, Some(&bind))?;
            self.send(&val, &bind).await?;
        }

        Ok(())
    }

    // Handle Commit message.
    //
    // Send Sync to all shards, ensuring they close the transaction.
    async fn commit(&mut self, commit: XLogCommit) -> Result<(), Error> {
        for server in &mut self.connections {
            server.send_one(&Sync.into()).await?;
            server.flush().await?;
        }
        for server in &mut self.connections {
            Self::read(server, 1, &['Z']).await?;
        }

        self.lsn = commit.end_lsn;

        Ok(())
    }

    /// Handle Relation message.
    ///
    /// Prepare upsert statement and record table info for future use
    /// by Insert, Update and Delete messages.
    async fn relation(&mut self, relation: Relation) -> Result<(), Error> {
        let table = self.tables.get(&Key {
            schema: relation.namespace.clone(),
            name: relation.name.clone(),
        });

        if let Some(table) = table {
            // Prepare queries for this table. Prepared statements
            // are much faster.
            let insert = Statement::new(&table.insert(false))?;
            let upsert = Statement::new(&table.insert(true))?;

            for server in &mut self.connections {
                server
                    .send(&vec![insert.parse().into(), upsert.parse().into(), Sync.into()].into())
                    .await?;
            }

            for server in &mut self.connections {
                Self::read(server, 3, &['1', 'Z']).await?;
            }

            self.statements.insert(
                relation.oid,
                Statements {
                    insert,
                    upsert,
                    update: Statement::default(),
                },
            );
        }

        self.relations.insert(relation.oid, relation);

        Ok(())
    }

    /// Handle replication stream message.
    pub async fn handle(&mut self, data: CopyData) -> Result<(), Error> {
        // Lazily connect to all shards.
        if self.connections.is_empty() {
            self.connect().await?;
        }

        if let Some(xlog) = data.xlog_data() {
            if let Some(payload) = xlog.payload() {
                match payload {
                    XLogPayload::Insert(insert) => self.insert(insert).await?,
                    XLogPayload::Commit(commit) => self.commit(commit).await?,
                    XLogPayload::Relation(relation) => self.relation(relation).await?,
                    _ => (),
                }
            }
        }

        Ok(())
    }

    /// Get latest LSN we flushed to replicas.
    pub fn status_update(&self) -> StatusUpdate {
        StatusUpdate {
            last_applied: self.lsn,
            last_flushed: self.lsn, // We use transactions which are flushed.
            last_written: self.lsn,
            system_clock: postgres_now(),
            reply: 0,
        }
    }
}
