//! Handle logical replication stream.
//!
//! Encodes Insert, Update and Delete messages
//! into idempotent prepared statements.
//!
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
    frontend::router::parser::{self, Insert, InsertRouting, Shard},
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

    #[allow(clippy::borrowed_box)]
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
        Self {
            cluster: cluster.clone(),
            sharding_schema: cluster.sharding_schema(),
            relations: HashMap::new(),
            statements: HashMap::new(),
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
        if let Some(table_lsn) = self.table_lsns.get(&insert.oid) {
            // Don't apply change if table is ahead.
            if self.lsn < *table_lsn {
                return Ok(());
            }
        }

        if let Some(statements) = self.statements.get(&insert.oid) {
            // Convert TupleData into a Bind message. We can now insert that tuple
            // using a prepared statement.
            let bind = insert.tuple_data.to_bind(&statements.upsert.name);

            // Upserts are idempotent. Even if we rewind the stream,
            // we are able to replay changes we already applied safely.
            if let Some(upsert) = statements.upsert.insert() {
                let upsert = Insert::new(upsert);
                let cfg = crate::config::config();
                let rewrite_enabled = cfg.config.rewrite.enabled;
                let split_mode = cfg.config.rewrite.split_inserts;
                let routing = upsert.shard(
                    &self.sharding_schema,
                    Some(&bind),
                    rewrite_enabled,
                    split_mode,
                )?;
                match routing {
                    InsertRouting::Routed(shard) => self.send(&shard, &bind).await?,
                    InsertRouting::Split(plan) => {
                        return Err(Error::Parser(parser::Error::SplitInsertNotSupported {
                            table: plan.table().to_string(),
                            reason: "logical replication does not support split inserts".into(),
                        }))
                    }
                }
            }

            // Update table LSN.
            self.table_lsns.insert(insert.oid, self.lsn);
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
                    c => return Err(Error::OutOfSync(c)),
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
            let insert = Statement::new(&table.insert(false))?;
            let upsert = Statement::new(&table.insert(true))?;

            for server in &mut self.connections {
                server
                    .send(&vec![insert.parse().into(), upsert.parse().into(), Sync.into()].into())
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
                        c => return Err(Error::OutOfSync(c)),
                    }
                }
            }

            self.statements.insert(
                relation.oid,
                Statements {
                    insert,
                    upsert,
                    update: Statement::default(),
                },
            );

            // Only record tables we expect to stream changes for.
            self.table_lsns.insert(relation.oid, table.lsn.lsn);
            self.relations.insert(relation.oid, relation);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{self, config, ConfigAndUsers, RewriteMode},
        net::messages::replication::logical::tuple_data::{Column, Identifier, TupleData},
    };
    use bytes::Bytes;
    use std::ops::Deref;

    struct RewriteGuard {
        original: ConfigAndUsers,
    }

    impl RewriteGuard {
        fn enable_split_rewrite() -> Self {
            let original = config().deref().clone();
            let mut updated = original.clone();
            updated.config.rewrite.enabled = true;
            updated.config.rewrite.split_inserts = RewriteMode::Rewrite;
            config::set(updated).unwrap();
            Self { original }
        }
    }

    impl Drop for RewriteGuard {
        fn drop(&mut self) {
            config::set(self.original.clone()).unwrap();
        }
    }

    fn text_column(value: &str) -> Column {
        Column {
            identifier: Identifier::Format(crate::net::messages::bind::Format::Text),
            len: value.len() as i32,
            data: Bytes::copy_from_slice(value.as_bytes()),
        }
    }

    #[tokio::test]
    async fn split_insert_rejected_during_replication() {
        config::load_test();
        let _guard = RewriteGuard::enable_split_rewrite();

        let cluster = Cluster::new_test();
        let mut subscriber = StreamSubscriber::new(&cluster, &[]);

        let upsert =
            Statement::new("INSERT INTO sharded (id, value) VALUES (1, 'one'), (11, 'eleven')")
                .unwrap();

        subscriber.statements.insert(
            42,
            Statements {
                insert: Statement::default(),
                upsert: upsert.clone(),
                update: Statement::default(),
            },
        );

        let insert = XLogInsert {
            xid: None,
            oid: 42,
            tuple_data: TupleData {
                columns: vec![text_column("1"), text_column("one")],
            },
        };

        let err = subscriber
            .insert(insert)
            .await
            .expect_err("expected split reject");
        match err {
            Error::Parser(parser::Error::SplitInsertNotSupported { table, reason }) => {
                assert_eq!(table, "sharded");
                assert!(reason.contains("logical replication does not support"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
