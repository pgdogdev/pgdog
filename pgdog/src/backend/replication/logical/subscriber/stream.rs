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
use tracing::{debug, trace, warn};

use super::super::publisher::{has_unique_index, NonIdentityColumnsPresence};
use super::super::{publisher::Table, Error, TableValidationError, TableValidationErrorKind};
use super::StreamContext;
use crate::net::messages::replication::logical::tuple_data::{Identifier, TupleData};
use crate::{
    backend::{Cluster, ConnectReason, Server},
    config::Role,
    frontend::router::parser::Shard,
    net::{
        replication::{
            xlog_data::XLogPayload, Commit as XLogCommit, Delete as XLogDelete,
            Insert as XLogInsert, Relation, StatusUpdate, Update as XLogUpdate, UpdateIdentity,
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
    /// `true` when the source table has `REPLICA IDENTITY FULL`.
    /// Controls INSERT/UPDATE/DELETE dispatch to FULL-mode handlers.
    full_identity: bool,
    /// Cached UPDATE statements keyed by the observed `NonIdentityColumnsPresence` shape —
    /// one entry per distinct set of unchanged-TOAST columns. Populated lazily by
    /// [`StreamSubscriber::ensure_update_shape_for`] (both DEFAULT/INDEX and FULL identity).
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

    // Dispatch a pre-built bind to the matching shard(s).
    //
    // Returns the maximum rows-affected count across all connections.
    async fn send(&mut self, val: &Shard, bind: &Bind) -> Result<usize, Error> {
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

        let mut max_rows: usize = 0;
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
                        max_rows = max_rows.max(rows);
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

        Ok(max_rows)
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
            let parse = if statements.omni {
                statements.upsert.parse()
            } else {
                statements.insert.parse()
            }
            .clone();
            let ctx = StreamContext::new(&self.cluster, &insert.tuple_data, &parse)?;
            self.send(ctx.shard(), ctx.bind()).await?;
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

        // FULL identity: dispatch before the DEFAULT/INDEX key-based match.
        // check_toasted_identity is a no-op for FULL tables (no identity columns)
        // so we skip it and go straight to the FULL handler.
        if self
            .statements
            .get(&update.oid)
            .is_some_and(|s| s.full_identity)
        {
            return self.update_full_identity(update.oid, update).await;
        }

        self.check_toasted_identity(&update)?;

        // Route by pre-image variant.
        match update.identity {
            UpdateIdentity::Key(key) => {
                // PK changed: delete old row by key, insert new row.
                // Toasted column in new tuple means incomplete data — fail.
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
                Ok(())
            }
            UpdateIdentity::Old(_) | UpdateIdentity::Nothing => {
                if !update.new.has_toasted() {
                    return self.update_full(update.oid, &update.new).await;
                }
                self.update_with_toasted(update.oid, update).await
            }
        }
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

    /// Fast-path UPDATE (DEFAULT/INDEX): no unchanged-TOAST columns — bind every
    /// column in tuple order and reuse the pre-prepared `update` statement.
    async fn update_full(&mut self, oid: Oid, new: &TupleData) -> Result<(), Error> {
        let parse = self
            .statements
            .get(&oid)
            .expect("statements entry checked before dispatch")
            .update
            .parse()
            .clone();
        let ctx = StreamContext::new(&self.cluster, new, &parse)?;
        self.send(ctx.shard(), ctx.bind()).await?;
        self.mark_table_changed(oid);
        Ok(())
    }

    /// Slow-path UPDATE (DEFAULT/INDEX): at least one unchanged-TOAST column.
    /// Build a shape bitmask, look up or prepare the matching partial UPDATE
    /// statement, then bind and execute it.
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

        let partial_new = update.partial_new();
        let shape_stmt = self
            .ensure_update_shape_for(oid, &table, &present, false)
            .await?;
        let ctx = StreamContext::new(&self.cluster, &partial_new, shape_stmt.parse())?;
        self.send(ctx.shard(), ctx.bind()).await?;
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

    // ── Routing helpers ────────────────────────────────────────────────────────────

    /// Resolve the shard for a tuple using `StreamContext` routing, without
    /// building a full `Bind` message. Used when the bind is constructed from
    /// multiple tuples (FULL identity UPDATE/DELETE).
    fn shard_for(&self, tuple: &TupleData, parse: &Parse) -> Result<Shard, Error> {
        Ok(StreamContext::new(&self.cluster, tuple, parse)?
            .shard()
            .clone())
    }

    /// Return `Err(FullIdentityAmbiguousMatch)` when more than one destination
    /// row was affected — the FULL-identity WHERE clause matched duplicate rows.
    fn check_ambiguous_match(&self, oid: Oid, op: &'static str, rows: usize) -> Result<(), Error> {
        if rows > 1 {
            let table = self.get_table(oid)?;
            return Err(Error::FullIdentityAmbiguousMatch {
                table: table.table.clone(),
                oid,
                op,
                rows: rows as u64,
            });
        }
        Ok(())
    }

    /// Concatenate two `TupleData` column lists in order: `a` first, then `b`.
    /// Used for FULL identity UPDATE binding where WHERE-predicate columns (`a`)
    /// occupy `$1..$k` and SET-value columns (`b`) occupy `$k+1..$n`.
    fn concat_tuples(a: &TupleData, b: &TupleData) -> TupleData {
        TupleData {
            columns: a.columns.iter().chain(b.columns.iter()).cloned().collect(),
        }
    }

    // ── Shape-cache helpers ──────────────────────────────────────────────────────

    /// Look up or prepare the UPDATE statement matching `present` on every shard
    /// and cache it under `statements[oid].update_shapes[present]`.
    ///
    /// `full_identity` selects the SQL generator on a cache miss:
    /// - `false` → `Table::update_partial` (DEFAULT/INDEX)
    /// - `true`  → `Table::update_full_identity` (FULL)
    ///
    /// Both modes share the same `update_shapes` map — `full_identity` is
    /// table-scoped so there is no key collision between modes.
    async fn ensure_update_shape_for(
        &mut self,
        oid: Oid,
        table: &Table,
        present: &NonIdentityColumnsPresence,
        full_identity: bool,
    ) -> Result<Statement, Error> {
        if let Some(stmt) = self
            .statements
            .get(&oid)
            .and_then(|s| s.update_shapes.get(present))
        {
            return Ok(stmt.clone());
        }

        let sql = if full_identity {
            table.update_full_identity(present)
        } else {
            table.update_partial(present)
        };
        let stmt = Statement::new(&sql)?;
        self.prepare_statements(&[stmt.parse().clone()]).await?;

        self.statements
            .get_mut(&oid)
            .ok_or(Error::MissingKey)?
            .update_shapes
            .insert(present.clone(), stmt.clone());
        Ok(stmt)
    }

    /// Select OLD columns at positions indicated by `present`, mirroring the WHERE layout
    /// of `Table::update_full_identity`. Used by the slow path because PG detoasts OLD
    /// before WAL emission — OLD has no 'u' markers to filter on.
    fn partial_old_for_present(
        old: &TupleData,
        table: &Table,
        present: &NonIdentityColumnsPresence,
    ) -> TupleData {
        let mut identity_count = 0usize;
        let columns = old
            .columns
            .iter()
            .zip(table.columns.iter())
            .enumerate()
            .filter_map(|(i, (col, tcol))| {
                if tcol.identity {
                    identity_count += 1;
                    None
                } else if present.is_set(i - identity_count) {
                    Some(col.clone())
                } else {
                    None
                }
            })
            .collect();
        TupleData { columns }
    }

    /// FULL identity UPDATE: WHERE on old-row values (`$1..$k`), SET on new-row values (`$k+1..$n`).
    /// On shard-key change fans out DELETE+INSERT across shards.
    async fn update_full_identity(&mut self, oid: Oid, update: XLogUpdate) -> Result<(), Error> {
        let old_full = match &update.identity {
            UpdateIdentity::Old(old) => old.clone(),
            _ => {
                let table = self.get_table(oid)?;
                return Err(Error::FullIdentityMissingOld {
                    table: table.table.clone(),
                    oid,
                    op: "UPDATE",
                });
            }
        };

        let table = self.get_table(oid)?;
        let (update_parse, delete_parse, insert_parse) = {
            let stmts = self.statements.get(&oid).ok_or(Error::MissingKey)?;
            (
                stmts.update.parse().clone(),
                stmts.delete.parse().clone(),
                stmts.insert.parse().clone(),
            )
        };

        let new_shard = self.shard_for(&update.new, &update_parse)?;
        let old_shard = self.shard_for(&old_full, &update_parse)?;

        if new_shard != old_shard {
            // Shard key changed: DELETE on old shard, INSERT on new.
            if update.new.has_toasted() {
                return Err(Error::FullIdentityCrossShardToasted {
                    table: table.table.clone(),
                    oid,
                });
            }
            let delete_bind = old_full.to_bind(delete_parse.name());
            let rows = self.send(&old_shard, &delete_bind).await?;
            self.check_ambiguous_match(oid, "UPDATE", rows)?;

            let insert_bind = update.new.to_bind(insert_parse.name());
            self.send(&new_shard, &insert_bind).await?;
            self.mark_table_changed(oid);
            return Ok(());
        }

        let (parse, set_tuple, where_tuple) = if !update.new.has_toasted() {
            // Fast path: all columns present — use the pre-prepared statement.
            (update_parse, update.new, old_full)
        } else {
            // Slow path: at least one unchanged-TOAST column — use shape cache.
            let present = NonIdentityColumnsPresence::from_tuple(&update.new, &table)?;
            if present.no_non_identity_present() {
                self.mark_table_changed(oid);
                return Ok(());
            }
            let partial_old = Self::partial_old_for_present(&old_full, &table, &present);
            let partial_new = update.partial_new();
            let stmt = self
                .ensure_update_shape_for(oid, &table, &present, true)
                .await?;
            (stmt.parse().clone(), partial_new, partial_old)
        };

        // WHERE occupies $1..$k (where_tuple), SET occupies $k+1..$n (set_tuple).
        let bind = Self::concat_tuples(&where_tuple, &set_tuple).to_bind(parse.name());
        let rows = self.send(&new_shard, &bind).await?;
        self.check_ambiguous_match(oid, "UPDATE", rows)?;
        self.mark_table_changed(oid);
        Ok(())
    }

    async fn delete(&mut self, delete: XLogDelete) -> Result<(), Error> {
        if self.lsn_applied(&delete.oid) {
            return Ok(());
        }

        // Extract statement info upfront to release the shared borrow before
        // async calls and the subsequent &mut self borrows in send().
        let stmt_data = self
            .statements
            .get(&delete.oid)
            .map(|s| (s.full_identity, s.delete.parse().clone()));

        if let Some((full_identity, delete_parse)) = stmt_data {
            if full_identity {
                // FULL identity: match on all old-row columns via IS NOT DISTINCT FROM.
                // PostgreSQL fetches TOAST values before writing DELETE WAL records,
                // so delete.old never contains Toasted markers.
                let Some(old) = delete.old.as_ref() else {
                    let table = self.get_table(delete.oid)?;
                    return Err(Error::FullIdentityMissingOld {
                        table: table.table.clone(),
                        oid: delete.oid,
                        op: "DELETE",
                    });
                };
                let shard = self.shard_for(old, &delete_parse)?;
                let bind = old.to_bind(delete_parse.name());
                let rows = self.send(&shard, &bind).await?;
                self.check_ambiguous_match(delete.oid, "DELETE", rows)?;
            } else if let Some(key) = delete.key_non_null() {
                // DEFAULT/INDEX path: bind the key columns.
                let shard = self.shard_for(&key, &delete_parse)?;
                let bind = key.to_bind(delete_parse.name());
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
                let omni = !table.is_sharded(&self.cluster.sharding_schema().tables);

                let statements = if table.identity.identity == "f" {
                    // ── FULL identity path ──────────────────────────────────────────────
                    let all_present = NonIdentityColumnsPresence::all(&table);
                    let insert = Statement::new(&table.insert())?;
                    let update = Statement::new(&table.update_full_identity(&all_present))?;
                    let delete = Statement::new(&table.delete_full_identity())?;

                    // Omni FULL tables require a unique index on the destination for
                    // ON CONFLICT DO NOTHING deduplication during the copy–replication
                    // overlap window. Sharded tables route each row to exactly one shard
                    // so no upsert dedup is needed; warn instead that secondary indexes
                    // must be present before catch-up replication begins.
                    let upsert = if omni {
                        let schema = table.table.destination_schema().to_string();
                        let name_str = table.table.destination_name().to_string();
                        // Probe every shard — schema can drift, causing FullIdentityAmbiguousMatch.
                        for dest_server in self.connections.iter_mut() {
                            let unique = has_unique_index(&schema, &name_str, dest_server).await?;
                            if !unique {
                                return Err(TableValidationError {
                                    table: table.table.clone(),
                                    kind: TableValidationErrorKind::FullIdentityOmniNoUniqueIndex,
                                }
                                .into());
                            }
                        }
                        Statement::new(&table.upsert_full_identity())?
                    } else {
                        warn!(
                            "table {} has REPLICA IDENTITY FULL and is sharded; \
                            ensure secondary indexes are present on the destination \
                            before catch-up replication or every UPDATE/DELETE will \
                            be a sequential scan and lag will compound",
                            dest_key
                        );
                        // Upsert slot is unused for sharded tables (omni == false).
                        Statement::default()
                    };

                    let mut parses = vec![
                        insert.parse().clone(),
                        update.parse().clone(),
                        delete.parse().clone(),
                    ];
                    if omni {
                        parses.push(upsert.parse().clone());
                    }
                    self.prepare_statements(&parses).await?;

                    Statements {
                        insert,
                        upsert,
                        update,
                        delete,
                        omni,
                        full_identity: true,
                        update_shapes: HashMap::new(),
                    }
                } else {
                    // ── DEFAULT / INDEX path (unchanged) ────────────────────────────────
                    let insert = Statement::new(&table.insert())?;
                    let upsert = Statement::new(&table.upsert())?;
                    let update = Statement::new(&table.update())?;
                    let delete = Statement::new(&table.delete())?;

                    self.prepare_statements(&[
                        insert.parse().clone(),
                        upsert.parse().clone(),
                        update.parse().clone(),
                        delete.parse().clone(),
                    ])
                    .await?;

                    Statements {
                        insert,
                        upsert,
                        update,
                        delete,
                        omni,
                        full_identity: false,
                        update_shapes: HashMap::new(),
                    }
                };

                self.statements.insert(relation.oid, statements);
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
    /// On any error, drain and `Sync` every shard connection so the implicit
    /// transaction left open by `Bind/Execute/Flush` is rolled back. Without
    /// this cleanup, a `FullIdentityAmbiguousMatch` (or any Postgres-level
    /// error mid-batch) would leave shards holding row locks until the
    /// connection is dropped — blocking the next operation on that shard.
    pub async fn handle(&mut self, data: CopyData) -> Result<Option<StatusUpdate>, Error> {
        match self.handle_inner(data).await {
            Ok(status) => Ok(status),
            Err(err) => {
                if let Err(abort_err) = self.abort_pending_transaction().await {
                    // Abort cleanup is best-effort; log so the original error isn't masked.
                    warn!("abort_pending_transaction failed after stream error: {abort_err:?}");
                }
                Err(err)
            }
        }
    }

    async fn handle_inner(&mut self, data: CopyData) -> Result<Option<StatusUpdate>, Error> {
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

    /// Send `Sync` to every shard and drain to `ReadyForQuery`.
    ///
    /// Postgres puts a connection in implicit-transaction mode when it receives
    /// `Bind/Execute/Flush` without a `Sync`. After an error — either a Postgres-
    /// level `ErrorResponse` or a client-side abort like `FullIdentityAmbiguousMatch`
    /// — every connection that participated in the failed dispatch is still
    /// holding that implicit transaction open (and its row locks). `Sync` causes
    /// Postgres to roll back the implicit transaction (or commit it if no error
    /// occurred) and reply with `ReadyForQuery`. Best-effort: each connection's
    /// drain is independent so one stuck conn does not poison the rest.
    async fn abort_pending_transaction(&mut self) -> Result<(), Error> {
        let mut first_err: Option<Error> = None;
        for server in &mut self.connections {
            let drained = async {
                server.send_one(&Sync.into()).await?;
                server.flush().await?;
                loop {
                    let msg = server.read().await?;
                    if msg.code() == 'Z' {
                        return Ok::<(), Error>(());
                    }
                }
            }
            .await;
            if let Err(e) = drained {
                debug!("[{}] abort drain failed: {:?}", server.addr(), e);
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }
        self.in_transaction = false;
        self.changed_tables.clear();
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
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
