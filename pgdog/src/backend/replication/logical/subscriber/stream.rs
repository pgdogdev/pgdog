//! Handle logical replication stream.
//!
//! Encodes Insert, Update and Delete messages
//! into idempotent prepared statements.
//!
use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    sync::atomic::{AtomicUsize, Ordering},
};

use once_cell::sync::Lazy;
use pg_query::{
    protobuf::{InsertStmt, ParseResult},
    NodeEnum,
};
use tracing::{debug, trace};

use super::super::{publisher::Table, Error};
use super::StreamContext;
use crate::{
    backend::{Cluster, ConnectReason, Server},
    config::Role,
    frontend::router::parser::Shard,
    net::{
        replication::{
            xlog_data::XLogPayload, Commit as XLogCommit, Delete as XLogDelete,
            Insert as XLogInsert, Relation, StatusUpdate, Update as XLogUpdate,
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

impl Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, r#""{}"."{}""#, self.schema, self.name)
    }
}

#[derive(Default, Debug, Clone)]
struct Statements {
    insert: Statement,
    #[allow(dead_code)]
    upsert: Statement,
    update: Statement,
    delete: Statement,
}

#[derive(Default, Debug, Clone)]
struct Statement {
    ast: ParseResult,
    parse: Parse,
}

impl Statement {
    fn parse(&self) -> &Parse {
        &self.parse
    }

    fn new(query: &str) -> Result<Self, Error> {
        let ast = pg_query::parse(query)?.protobuf;
        let name = statement_name();
        Ok(Self {
            ast,
            parse: Parse::named(name, query.to_string()),
        })
    }

    #[allow(clippy::borrowed_box)]
    #[allow(dead_code)]
    fn insert(&self) -> Option<&Box<InsertStmt>> {
        self.ast
            .stmts
            .first()
            .and_then(|stmt| {
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
    }

    fn query(&self) -> &str {
        self.parse.query()
    }
}
#[derive(Debug)]
pub struct StreamSubscriber {
    /// Destination cluster.
    cluster: Cluster,

    // Relation markers sent by the publisher.
    // Happens once per connection.
    relations: HashMap<i32, Relation>,

    // Tables in the publication on the publisher.
    tables: HashMap<Key, Table>,

    // Statements
    statements: HashMap<i32, Statements>,

    // Partitioned tables dedup.
    partitioned_dedup: HashSet<Key>,

    // LSNs for each table
    table_lsns: HashMap<i32, i64>,

    // Connections to shards.
    connections: Vec<Server>,

    // Position in the WAL we have flushed successfully.
    lsn: i64,
    lsn_changed: bool,

    // Bytes sharded
    bytes_sharded: usize,
}

impl StreamSubscriber {
    pub fn new(cluster: &Cluster, tables: &[Table]) -> Self {
        let cluster = cluster.logical_stream();
        Self {
            cluster,
            relations: HashMap::new(),
            statements: HashMap::new(),
            partitioned_dedup: HashSet::new(),
            table_lsns: HashMap::new(),
            tables: tables
                .iter()
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
            bytes_sharded: 0,
            lsn_changed: true,
        }
    }

    // Connect to all the shards.
    pub async fn connect(&mut self) -> Result<(), Error> {
        let mut conns = vec![];

        for shard in self.cluster.shards() {
            let primary = shard
                .pools_with_roles()
                .iter()
                .find(|(r, _)| r == &Role::Primary)
                .ok_or(Error::NoPrimary)?
                .1
                .standalone(ConnectReason::Replication)
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
                    'E' => {
                        return Err(Error::PgError(Box::new(ErrorResponse::from_bytes(
                            msg.to_bytes()?,
                        )?)))
                    }
                    c => return Err(Error::OutOfSync(c)),
                }
            }
        }

        Ok(())
    }

    // Send a statement to one or more shards.
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

        Ok(())
    }

    // Handle Insert message.
    //
    // Convert Insert into an idempotent "upsert" and apply it to
    // the right shard(s).
    async fn insert(&mut self, insert: XLogInsert) -> Result<(), Error> {
        if self.lsn_applied(&insert.oid) {
            return Ok(());
        }

        if let Some(statements) = self.statements.get(&insert.oid) {
            // Convert TupleData into a Bind message. We can now insert that tuple
            // using a prepared statement.
            let mut context =
                StreamContext::new(&self.cluster, &insert.tuple_data, statements.insert.parse());
            let bind = context.bind().clone();
            let shard = context.shard()?;

            self.send(&shard, &bind).await?;
        }

        // Update table LSN.
        self.table_lsns.insert(insert.oid, self.lsn);

        Ok(())
    }

    async fn update(&mut self, update: XLogUpdate) -> Result<(), Error> {
        if self.lsn_applied(&update.oid) {
            return Ok(());
        }

        if let Some(statements) = self.statements.get(&update.oid) {
            // Primary key update.
            //
            // Convert it into a delete of the old row
            // and an insert with the new row.
            if let Some(key) = update.key {
                let delete = XLogDelete {
                    key: Some(key),
                    oid: update.oid,
                    old: None,
                };
                let insert = XLogInsert {
                    xid: None,
                    oid: update.oid,
                    tuple_data: update.new,
                };
                self.delete(delete).await?;
                self.insert(insert).await?;
            } else {
                let mut context =
                    StreamContext::new(&self.cluster, &update.new, statements.update.parse());
                let bind = context.bind().clone();
                let shard = context.shard()?;

                self.send(&shard, &bind).await?;
            }
        }

        // Update table LSN.
        self.table_lsns.insert(update.oid, self.lsn);

        Ok(())
    }

    async fn delete(&mut self, delete: XLogDelete) -> Result<(), Error> {
        if self.lsn_applied(&delete.oid) {
            return Ok(());
        }

        if let Some(statements) = self.statements.get(&delete.oid) {
            // Convert TupleData into a Bind message. We can now insert that tuple
            // using a prepared statement.
            if let Some(key) = delete.key_non_null() {
                let mut context =
                    StreamContext::new(&self.cluster, &key, statements.delete.parse());
                let bind = context.bind().clone();
                let shard = context.shard()?;

                self.send(&shard, &bind).await?;
            }
        }

        // Update table LSN.
        self.table_lsns.insert(delete.oid, self.lsn);

        Ok(())
    }

    fn lsn_applied(&self, oid: &i32) -> bool {
        if let Some(table_lsn) = self.table_lsns.get(oid) {
            // Don't apply change if table is ahead.
            if self.lsn < *table_lsn {
                return true;
            }
        }

        false
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
            // Drain responses from server.
            loop {
                let msg = server.read().await?;
                trace!("[{}] --> {:?}", server.addr(), msg);

                match msg.code() {
                    'E' => {
                        return Err(Error::PgError(Box::new(ErrorResponse::from_bytes(
                            msg.to_bytes()?,
                        )?)))
                    }
                    'Z' => break,
                    '2' | 'C' => continue,
                    c => return Err(Error::CommitOutOfSync(c)),
                }
            }
        }

        self.set_current_lsn(commit.end_lsn);

        Ok(())
    }

    // Handle Relation message.
    //
    // Prepare upsert statement and record table info for future use
    // by Insert, Update and Delete messages.
    async fn relation(&mut self, relation: Relation) -> Result<(), Error> {
        let table = self.tables.get(&Key {
            schema: relation.namespace.clone(),
            name: relation.name.clone(),
        });

        if let Some(table) = table {
            // Prepare queries for this table. Prepared statements
            // are much faster.

            table.valid()?;

            let dest_key = Key {
                schema: table.table.destination_schema().to_string(),
                name: table.table.destination_name().to_string(),
            };

            if self.partitioned_dedup.contains(&dest_key) {
                debug!("queries for table {} already prepared", dest_key);
                return Ok(());
            }

            let insert = Statement::new(&table.insert(false))?;
            let upsert = Statement::new(&table.insert(true))?;
            let update = Statement::new(&table.update())?;
            let delete = Statement::new(&table.delete())?;

            for server in &mut self.connections {
                for stmt in &[&insert, &upsert, &update, &delete] {
                    debug!("preparing \"{}\" [{}]", stmt.query(), server.addr());
                }

                server
                    .send(
                        &vec![
                            insert.parse().clone().into(),
                            upsert.parse().clone().into(),
                            update.parse().clone().into(),
                            delete.parse().clone().into(),
                            Sync.into(),
                        ]
                        .into(),
                    )
                    .await?;
            }

            for server in &mut self.connections {
                loop {
                    let msg = server.read().await?;
                    trace!("[{}] --> {:?}", server.addr(), msg);

                    match msg.code() {
                        'E' => {
                            return Err(Error::PgError(Box::new(ErrorResponse::from_bytes(
                                msg.to_bytes()?,
                            )?)))
                        }
                        'Z' => break,
                        '1' => continue,
                        c => return Err(Error::RelationOutOfSync(c)),
                    }
                }
            }

            self.statements.insert(
                relation.oid,
                Statements {
                    insert,
                    upsert,
                    update,
                    delete,
                },
            );

            // Only record tables we expect to stream changes for.
            self.table_lsns.insert(relation.oid, table.lsn.lsn);
            self.relations.insert(relation.oid, relation);
            self.partitioned_dedup.insert(dest_key);
        }

        Ok(())
    }

    /// Handle replication stream message.
    ///
    /// Return true if stream is done, false otherwise.
    pub async fn handle(&mut self, data: CopyData) -> Result<Option<StatusUpdate>, Error> {
        // Lazily connect to all shards.
        if self.connections.is_empty() {
            self.connect().await?;
        }

        let mut status_update = None;

        if let Some(xlog) = data.xlog_data() {
            if let Some(payload) = xlog.payload() {
                match payload {
                    XLogPayload::Insert(insert) => self.insert(insert).await?,
                    XLogPayload::Update(update) => self.update(update).await?,
                    XLogPayload::Delete(delete) => self.delete(delete).await?,
                    XLogPayload::Commit(commit) => {
                        self.commit(commit).await?;
                        status_update = Some(self.status_update());
                    }
                    XLogPayload::Relation(relation) => self.relation(relation).await?,
                    XLogPayload::Begin(begin) => {
                        self.set_current_lsn(begin.final_transaction_lsn);
                    }
                    _ => (),
                }
                self.bytes_sharded += xlog.len();
            }
        }

        Ok(status_update)
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

    /// Number of bytes processed.
    pub fn bytes_sharded(&self) -> usize {
        self.bytes_sharded
    }

    /// Set stream start at this LSN.
    ///
    /// Return true if LSN has been updated to a new value,
    /// i.e., the stream is moving forward.
    pub fn set_current_lsn(&mut self, lsn: i64) -> bool {
        self.lsn_changed = lsn != self.lsn;
        self.lsn = lsn;
        self.lsn_changed
    }

    /// Get current LSN.
    pub fn lsn(&self) -> i64 {
        self.lsn
    }

    /// Lsn changed since the last time we updated it.
    pub fn lsn_changed(&self) -> bool {
        self.lsn_changed
    }
}
