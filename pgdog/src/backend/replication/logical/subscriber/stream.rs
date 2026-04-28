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
use pgdog_postgres_types::Oid;
use tracing::{debug, trace};

use super::super::publisher::NonIdentityColumnsPresence;
use super::super::{publisher::Table, Error};
use super::StreamContext;
use crate::net::messages::replication::logical::tuple_data::{Identifier, TupleData};
use crate::{
    backend::{Cluster, ConnectReason, Server},
    config::Role,
    frontend::router::parser::Shard,
    net::{
        replication::{
            xlog_data::XLogPayload, Commit as XLogCommit, Delete as XLogDelete,
            Insert as XLogInsert, Relation, StatusUpdate, Update as XLogUpdate,
        },
        Bind, CommandComplete, CopyData, ErrorResponse, Execute, Flush, FromBytes, Parse, Protocol,
        Sync, ToBytes,
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
    upsert: Statement,
    update: Statement,
    delete: Statement,
    omni: bool,
    /// Cached UPDATE statements keyed by the observed `NonIdentityColumnsPresence` shape —
    /// one entry per distinct set of unchanged-TOAST columns. Populated lazily
    /// by [`StreamSubscriber::ensure_update_shape`].
    update_shapes: HashMap<NonIdentityColumnsPresence, Statement>,
}

#[derive(Default, Debug, Clone)]
struct Statement {
    parse: Parse,
}

impl Statement {
    fn parse(&self) -> &Parse {
        &self.parse
    }

    fn new(query: &str) -> Result<Self, Error> {
        let name = statement_name();
        Ok(Self {
            parse: Parse::named(name, query.to_string()),
        })
    }
}
#[derive(Debug, Default)]
pub struct StreamSubscriber {
    /// Destination cluster.
    cluster: Cluster,

    // Relation markers sent by the publisher.
    // Happens once per connection.
    relations: HashMap<Oid, Relation>,

    // Tables in the publication on the publisher.
    tables: HashMap<Key, Table>,

    // Statements
    statements: HashMap<Oid, Statements>,
    // Mapping of table keys to their oid.
    keys: HashMap<Key, Oid>,

    // LSNs for each table
    table_lsns: HashMap<Oid, i64>,

    // Tables changed in the current transaction. We advance their replay
    // watermark on commit so equal-LSN rows in the same transaction are not skipped.
    changed_tables: HashSet<Oid>,

    // Connections to shards.
    connections: Vec<Server>,

    // Position in the WAL we have flushed successfully.
    lsn: i64,
    lsn_changed: bool,
    in_transaction: bool,

    // Bytes sharded
    bytes_sharded: usize,

    // Missed rows.
    missed_rows: MissedRows,
}

impl StreamSubscriber {
    pub fn new(cluster: &Cluster, tables: &[Table]) -> Self {
        let cluster = cluster.logical_stream();
        Self {
            cluster,
            relations: HashMap::new(),
            statements: HashMap::new(),
            table_lsns: HashMap::new(),
            changed_tables: HashSet::new(),
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
            in_transaction: false,
            missed_rows: MissedRows::default(),
            keys: HashMap::default(),
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

    // Send a statement to one or more matching shards.
    async fn send(&mut self, val: &Shard, bind: &Bind) -> Result<(), Error> {
        let mut conns: Vec<_> = self
            .connections
            .iter_mut()
            .enumerate()
            .filter(|(shard, _)| match val {
                Shard::Direct(direct) => *shard == *direct,
                Shard::Multi(multi) => multi.contains(shard),
                _ => true,
            })
            .map(|(_, server)| server)
            .collect();

        for conn in &mut conns {
            conn.send(&vec![bind.clone().into(), Execute::new().into(), Flush.into()].into())
                .await?;
        }

        for conn in &mut conns {
            conn.flush().await?;
        }

        for conn in &mut conns {
            // Keep server connections always synchronized.
            for _ in 0..2 {
                let msg = conn.read().await?;
                match msg.code() {
                    'C' => {
                        let cmd = CommandComplete::try_from(msg)?;
                        let rows = cmd
                            .rows()?
                            .ok_or(Error::CommandCompleteNoRows(cmd.clone()))?;
                        // A direct-to-shard update indicates a row has changed on source.
                        // This row must exist on the destination, or we missed some data during sync.
                        if rows == 0 && val.is_direct() {
                            match cmd.tag() {
                                "UPDATE" => self.missed_rows.update += 1,
                                "DELETE" => self.missed_rows.delete += 1,
                                "INSERT" => self.missed_rows.insert += 1,
                                _ => (),
                            }
                        }
                    }
                    '2' => (),
                    'E' => {
                        return Err(Error::PgError(Box::new(ErrorResponse::from_bytes(
                            msg.to_bytes()?,
                        )?)))
                    }
                    c => return Err(Error::SendOutOfSync(c)),
                }
            }
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
            let mut context = StreamContext::new(
                &self.cluster,
                &insert.tuple_data,
                if statements.omni {
                    statements.upsert.parse()
                } else {
                    statements.insert.parse()
                },
            );
            let bind = context.bind().clone();
            let shard = context.shard()?;

            self.send(&shard, &bind).await?;
        }

        self.mark_table_changed(insert.oid);

        Ok(())
    }

    async fn update(&mut self, update: XLogUpdate) -> Result<(), Error> {
        if self.lsn_applied(&update.oid) {
            return Ok(());
        }

        if !self.statements.contains_key(&update.oid) {
            self.mark_table_changed(update.oid);
            return Ok(());
        }

        self.check_toasted_identity(&update)?;

        // PK changed: delete old row by key, insert new row.
        // Toasted column in new tuple means incomplete data — fail.
        if let Some(key) = update.key {
            if update.new.has_toasted() {
                let table = self.get_table(update.oid)?;

                return Err(Error::ToastedRowMigration {
                    table: table.publication,
                    oid: update.oid,
                });
            }
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
            return Ok(());
        }

        if !update.new.has_toasted() {
            return self.update_full(update.oid, &update.new).await;
        }

        self.update_with_toasted(update.oid, update).await
    }

    /// Resolve the `Table` for a relation OID.
    fn get_table(&self, oid: Oid) -> Result<Table, Error> {
        let key = self
            .relations
            .get(&oid)
            .map(|r| Key {
                schema: r.namespace.clone(),
                name: r.name.clone(),
            })
            .ok_or(Error::MissingKey)?;
        self.tables.get(&key).cloned().ok_or(Error::MissingKey)
    }

    /// Fast-path UPDATE: no unchanged-TOAST columns — bind every column in
    /// tuple order and reuse the pre-prepared `update` statement.
    async fn update_full(&mut self, oid: Oid, new: &TupleData) -> Result<(), Error> {
        let statements = self
            .statements
            .get(&oid)
            .expect("statements entry checked before dispatch");
        let mut context = StreamContext::new(&self.cluster, new, statements.update.parse());
        let bind = context.bind().clone();
        let shard = context.shard()?;
        self.send(&shard, &bind).await?;
        self.mark_table_changed(oid);
        Ok(())
    }

    /// Slow-path UPDATE: at least one unchanged-TOAST column. Build a shape
    /// bitmask, look up or prepare the matching partial UPDATE statement, then
    /// bind and execute it.
    async fn update_with_toasted(&mut self, oid: Oid, update: XLogUpdate) -> Result<(), Error> {
        let table = self.get_table(update.oid)?;
        let present = NonIdentityColumnsPresence::from_tuple(&update.new, &table)?;

        if present.no_non_identity_present() {
            // All non-identity columns are unchanged-TOAST and identity didn't
            // change — the destination row already has every value we would
            // write. No-op, but still advance the watermark so equal-LSN replay
            // after commit is gated correctly.
            self.mark_table_changed(oid);
            return Ok(());
        }

        let shape_stmt = self.ensure_update_shape(oid, &table, &present).await?;
        let partial_new = update.partial_new();
        let shape_parse = shape_stmt.parse();

        // Route via the shape's AST/bind — param positions match by construction.
        let mut context = StreamContext::new(&self.cluster, &partial_new, shape_parse);
        let bind = context.bind().clone(); // clone before mutable shard() borrow
        let shard = context.shard()?;
        self.send(&shard, &bind).await?;
        self.mark_table_changed(oid);
        Ok(())
    }

    /// Return `Err(ToastedIdentityColumn)` if any identity column in the new tuple is `'u'`.
    fn check_toasted_identity(&self, update: &XLogUpdate) -> Result<(), Error> {
        if update.new.has_toasted() {
            let table = self.get_table(update.oid)?;

            let has_toasted_identity = update
                .new
                .columns
                .iter()
                .zip(table.columns.iter())
                .any(|(col, tcol)| tcol.identity && col.identifier == Identifier::Toasted);
            if has_toasted_identity {
                return Err(Error::ToastedIdentityColumn {
                    table: table.publication.clone(),
                    oid: update.oid,
                });
            }
        }
        Ok(())
    }

    /// Prepare the UPDATE statement matching `present` on every shard and
    /// cache it under `statements[oid].update_shapes[present]`.
    ///
    /// Returns the (possibly freshly prepared) `Statement` so the caller can
    /// use it directly without a second cache lookup.
    /// Send a batch of [`Parse`] messages to every server and drain the
    /// acknowledgment cycle (`ParseComplete` × N, then `ReadyForQuery` when
    /// not in a transaction).
    async fn prepare_statements(&mut self, parses: &[Parse]) -> Result<(), Error> {
        let in_txn = self.in_transaction;
        let mut msgs: Vec<_> = parses.iter().map(|p| p.clone().into()).collect();
        msgs.push(if in_txn { Flush.into() } else { Sync.into() });
        let payload = msgs.into();

        for server in &mut self.connections {
            for p in parses {
                debug!("preparing \"{}\" [{}]", p.query(), server.addr());
            }
            server.send(&payload).await?;
        }

        let num_acks = if in_txn {
            parses.len()
        } else {
            parses.len() + 1
        };
        for server in &mut self.connections {
            for _ in 0..num_acks {
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
        Ok(())
    }

    async fn ensure_update_shape(
        &mut self,
        oid: Oid,
        table: &Table,
        present: &NonIdentityColumnsPresence,
    ) -> Result<Statement, Error> {
        if let Some(stmt) = self
            .statements
            .get(&oid)
            .and_then(|s| s.update_shapes.get(present))
        {
            return Ok(stmt.clone());
        }

        let sql = table.update_partial(present);
        let stmt = Statement::new(&sql)?;
        self.prepare_statements(&[stmt.parse().clone()]).await?;

        self.statements
            .get_mut(&oid)
            .ok_or(Error::MissingKey)?
            .update_shapes
            .insert(present.clone(), stmt.clone());
        Ok(stmt)
    }

    async fn delete(&mut self, delete: XLogDelete) -> Result<(), Error> {
        if self.lsn_applied(&delete.oid) {
            return Ok(());
        }

        if let Some(statements) = self.statements.get(&delete.oid) {
            if let Some(key) = delete.key_non_null() {
                let mut context =
                    StreamContext::new(&self.cluster, &key, statements.delete.parse());
                let bind = context.bind().clone();
                let shard = context.shard()?;

                self.send(&shard, &bind).await?;
            }
        }

        self.mark_table_changed(delete.oid);

        Ok(())
    }

    pub(crate) fn lsn_applied(&self, oid: &Oid) -> bool {
        if let Some(table_lsn) = self.table_lsns.get(oid) {
            // Don't apply change if the table has already been copied or replayed
            // through this transaction boundary.
            if self.lsn <= *table_lsn {
                return true;
            }
        }

        false
    }

    fn mark_table_changed(&mut self, oid: Oid) {
        if self.in_transaction {
            self.changed_tables.insert(oid);
        } else {
            self.table_lsns.insert(oid, self.lsn);
        }
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
            let msg = server.read().await?;
            trace!("[{}] --> {:?}", server.addr(), msg);

            match msg.code() {
                'E' => {
                    return Err(Error::PgError(Box::new(ErrorResponse::from_bytes(
                        msg.to_bytes()?,
                    )?)))
                }
                'Z' => (),
                c => return Err(Error::CommitOutOfSync(c)),
            }
        }

        let transaction_lsn = self.lsn;
        for oid in self.changed_tables.drain() {
            self.table_lsns.insert(oid, transaction_lsn);
        }

        self.set_current_lsn(commit.end_lsn);

        Ok(())
    }

    // Handle Relation message.
    //
    // Prepare upsert statement and record table info for future use
    // by Insert, Update and Delete messages.
    async fn relation(&mut self, relation: Relation) -> Result<(), Error> {
        let table = self
            .tables
            .get(&Key {
                schema: relation.namespace.clone(),
                name: relation.name.clone(),
            })
            .cloned();

        if let Some(table) = table {
            // Prepare queries for this table. Prepared statements
            // are much faster.

            table.valid()?;

            let dest_key = Key {
                schema: table.table.destination_schema().to_string(),
                name: table.table.destination_name().to_string(),
            };

            // Partition child tables target the parent on the destination shard,
            // we don't need to prepare the same statement per child.
            if let Some(oid) = self.keys.get(&dest_key) {
                let statements = self.statements.get(oid).ok_or(Error::MissingKey)?;
                self.statements.insert(relation.oid, statements.clone());

                debug!("queries for table {} already prepared", dest_key);
            } else {
                let insert = Statement::new(&table.insert())?;
                let upsert = Statement::new(&table.upsert())?;
                let update = Statement::new(&table.update())?;
                let delete = Statement::new(&table.delete())?;
                let omni = !table.is_sharded(&self.cluster.sharding_schema().tables);

                self.prepare_statements(&[
                    insert.parse().clone(),
                    upsert.parse().clone(),
                    update.parse().clone(),
                    delete.parse().clone(),
                ])
                .await?;

                self.statements.insert(
                    relation.oid,
                    Statements {
                        insert,
                        upsert,
                        update,
                        delete,
                        omni,
                        update_shapes: HashMap::new(),
                    },
                );

                self.keys.insert(dest_key, relation.oid);
            }

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
                    XLogPayload::Update(update) => self.update(update).await?,
                    XLogPayload::Delete(delete) => self.delete(delete).await?,
                    XLogPayload::Commit(commit) => {
                        self.commit(commit).await?;
                        status_update = Some(self.status_update());
                        self.in_transaction = false;
                    }
                    XLogPayload::Relation(relation) => self.relation(relation).await?,
                    XLogPayload::Begin(begin) => {
                        self.changed_tables.clear();
                        self.set_current_lsn(begin.final_transaction_lsn);
                        self.in_transaction = true;
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

    /// Whether we are inside a transaction.
    #[cfg(test)]
    pub(crate) fn in_transaction(&self) -> bool {
        self.in_transaction
    }

    /// Get and reset missing rows.
    pub(crate) fn missed_rows(&mut self) -> MissedRows {
        std::mem::take(&mut self.missed_rows)
    }
}

#[derive(Debug, Default)]
pub(crate) struct MissedRows {
    insert: usize,
    delete: usize,
    update: usize,
}

impl MissedRows {
    pub(crate) fn non_zero(&self) -> bool {
        self.insert > 0 || self.delete > 0 || self.update > 0
    }
}

impl Display for MissedRows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut written = false;
        if self.insert > 0 {
            write!(f, "insert={}", self.insert)?;
            written = true;
        }
        if self.update > 0 {
            write!(
                f,
                "{}update={}",
                if written { " " } else { "" },
                self.update
            )?;
            written = true;
        }
        if self.delete > 0 {
            write!(
                f,
                "{}delete={}",
                if written { " " } else { "" },
                self.delete
            )?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::config;

    fn make_subscriber() -> StreamSubscriber {
        let cluster = Cluster::new_test(&config());
        StreamSubscriber::new(&cluster, &[])
    }

    #[test]
    fn lsn_gating_is_inclusive_at_copy_boundary() {
        let mut sub = make_subscriber();
        let oid = Oid(42);

        sub.table_lsns.insert(oid, 100);
        sub.set_current_lsn(100);

        assert!(sub.lsn_applied(&oid));
    }

    #[tokio::test]
    async fn table_watermarks_advance_on_commit() {
        let mut sub = make_subscriber();
        let oid = Oid(42);

        sub.table_lsns.insert(oid, 50);
        sub.in_transaction = true;
        sub.set_current_lsn(100);
        sub.mark_table_changed(oid);

        // Rows from the current transaction must remain eligible until commit.
        assert!(!sub.lsn_applied(&oid));

        sub.commit(XLogCommit {
            flags: 0,
            commit_lsn: 0,
            end_lsn: 200,
            commit_timestamp: 0,
        })
        .await
        .unwrap();

        assert_eq!(sub.table_lsns.get(&oid), Some(&100));
        assert_eq!(sub.lsn(), 200);
        assert!(sub.changed_tables.is_empty());
    }
}
