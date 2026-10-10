//! Handle logical replication stream.
//!
//! Encodes Insert, Update and Delete messages
//! into idempotent prepared statements.
//!
use std::{
    collections::HashMap,
    fmt::Display,
    sync::atomic::{AtomicUsize, Ordering},
};

use futures::future::try_join_all;
use once_cell::sync::Lazy;
use pgdog_postgres_types::Oid;
use pgdog_stats::MissedRows;
use tracing::{debug, trace, warn};

use super::super::publisher::{NonIdentityColumnsPresence, tables_missing_unique_index};
use super::super::{
    Error, TableValidationError, TableValidationErrorKind, ensure_validation, publisher::Table,
};
use super::StreamContext;
use super::{
    PipelinedConnection, connect_primary, pipeline::ProgressCheck,
    replication_origin::ReplicationOrigin,
};
use crate::net::messages::replication::logical::tuple_data::{Identifier, TupleData};
use crate::net::messages::replication::logical::update::Update as XLogUpdate;
use crate::{
    backend::{Cluster, Server},
    frontend::router::parser::Shard,
    net::{
        Bind, CopyData, ErrorResponse, FromBytes, Parse, Protocol, Sync, ToBytes,
        replication::{
            Commit as XLogCommit, Delete as XLogDelete, Insert as XLogInsert, Relation,
            StatusUpdate, UpdateIdentity, xlog_data::XLogPayload,
        },
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
    /// UPDATE statements keyed by `NonIdentityColumnsPresence` — one per
    /// distinct TOAST-column shape. Shared by DEFAULT/INDEX and FULL identity.
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
            parse: Parse::named(name, query),
        })
    }
}

#[derive(Debug)]
pub(crate) struct StreamSubscriber {
    /// Destination cluster.
    cluster: Cluster,

    /// Origins on the destination shards, one per shard, created by the
    /// caller and bound again on every connect.
    origins: Vec<ReplicationOrigin>,

    // Relation markers sent by the publisher.
    // Happens once per connection.
    relations: HashMap<Oid, Relation>,

    // Tables in the publication on the publisher.
    tables: HashMap<Key, Table>,

    // Statements
    statements: HashMap<Oid, Statements>,
    // Mapping of table keys to their oid.
    keys: HashMap<Key, Oid>,

    // Watermark of each table: changes of transactions that begin at or before
    // it are skipped. Starts at the table copy point and is raised to every
    // confirmed commit, so an already applied transaction is not applied again.
    table_lsns: HashMap<Oid, i64>,

    // Pipelined connections to shards. DML is pushed without waiting per event;
    // a background task per connection reads responses and reconciles them.
    connections: Vec<PipelinedConnection>,
    /// Shards that received changes or statements in the current transaction.
    /// Only they commit it and record it in their replication origin.
    changed_shards: Vec<bool>,
    /// Source LSN recorded in the origin of each destination shard and
    /// flushed to disk.
    applied_lsns: Vec<i64>,
    /// Source LSN of the last commit sent to the shards. Commits up to it
    /// that are not confirmed yet are in flight.
    sent_lsn: i64,
    // Last commit LSN acked to Postgres. Reported in status updates; never
    // advances mid-transaction so KeepAlive replies can't skip an open transaction.
    committed_lsn: i64,
    // Working position in the stream (advances on Begin for deduplication).
    lsn: i64,
    lsn_changed: bool,
    in_transaction: bool,

    // Bytes sharded
    bytes_sharded: usize,
    rows_sharded: usize,

    missed_rows: MissedRows,
}

impl StreamSubscriber {
    pub(crate) fn new(
        cluster: &Cluster,
        tables: Vec<Table>,
        origins: Vec<ReplicationOrigin>,
    ) -> Self {
        let cluster = cluster.logical_stream();
        let applied_lsns = vec![0; origins.len()];
        Self {
            cluster,
            origins,
            relations: HashMap::new(),
            statements: HashMap::new(),
            table_lsns: HashMap::new(),
            sent_lsn: 0,
            tables: tables
                .into_iter()
                .map(|table| {
                    (
                        Key {
                            schema: table.table.schema.clone(),
                            name: table.table.name.clone(),
                        },
                        table,
                    )
                })
                .collect(),
            connections: vec![],
            changed_shards: vec![],
            applied_lsns,
            committed_lsn: 0,
            lsn: 0, // Unknown,
            bytes_sharded: 0,
            rows_sharded: 0,
            lsn_changed: true,
            in_transaction: false,
            keys: HashMap::default(),
            missed_rows: MissedRows::default(),
        }
    }

    /// Are there commits sent to the shards but not confirmed to the source yet?
    pub(crate) fn has_in_flight(&self) -> bool {
        self.committed_lsn < self.sent_lsn
    }

    /// Bind the origin of `shard` to `server` and read what it already has.
    async fn setup_origin(&mut self, shard: usize, server: &mut Server) -> Result<(), Error> {
        let origin = &self.origins[shard];
        origin.setup_session(server).await?;
        self.applied_lsns[shard] = origin.progress(server, true).await?.lsn;
        Ok(())
    }

    // Connect to all the shards.
    //
    // The transaction-control prepare and the omni FULL-identity validation run
    // synchronously on the raw `Server` connections (both are one-shot,
    // request/response query flows). Only once they succeed are the connections
    // moved into their per-shard pipelined tasks for the streaming apply path.
    pub(crate) async fn connect(&mut self) -> Result<(), Error> {
        let mut conns: Vec<Server> = vec![];

        for shard in self.cluster.shards() {
            conns.push(connect_primary(shard).await?);
        }
        for (shard, server) in conns.iter_mut().enumerate() {
            self.setup_origin(shard, server).await?;
        }

        // Transaction control statements, and the statement that records
        // the source commit in the replication origin.
        //
        // TODO: Figure out if we need to use them?
        for server in &mut conns {
            let begin = Parse::named("__pgdog_repl_begin", "BEGIN");
            let commit = Parse::named("__pgdog_repl_commit", "COMMIT");

            server
                .send(
                    &vec![
                        begin.into(),
                        commit.into(),
                        ReplicationOrigin::xact_setup_parse().into(),
                        Sync.into(),
                    ]
                    .into(),
                )
                .await?;
            for _ in 0..4 {
                let msg = server.read().await?;
                trace!("[{}] --> {:?}", server.addr(), msg);
                match msg.code() {
                    '1' | 'C' | 'Z' => (),
                    'E' => {
                        return Err(Error::PgError(Box::new(ErrorResponse::from_bytes(
                            msg.to_bytes(),
                        )?)));
                    }
                    c => return Err(Error::OutOfSync(c)),
                }
            }
        }

        // Validate omni FULL-identity tables have a unique index on every destination shard.
        let omni_full: Vec<Table> = self
            .tables
            .values()
            .filter(|t| {
                t.is_identity_full() && !t.is_sharded(&self.cluster.sharding_schema().tables)
            })
            .cloned()
            .collect();
        if !omni_full.is_empty() {
            self.validate_full_identity_omni_has_unique_index(&mut conns, &omni_full)
                .await?;
        }

        // Hand each connection to its background pipelining task.
        self.changed_shards = vec![false; conns.len()];
        let check = ProgressCheck::builder()
            .retry_attempts(self.cluster.resharding_replication_retry_max_attempts())
            .retry_delay(self.cluster.resharding_replication_retry_min_delay())
            .build();
        self.connections = conns
            .into_iter()
            .zip(&self.origins)
            .map(|(server, origin)| PipelinedConnection::new(server, origin.clone(), check))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(())
    }

    // Dispatch a pre-built bind to the matching shard(s).
    async fn send(&mut self, val: &Shard, bind: Bind) -> Result<(), Error> {
        // Fail-fast: the replicated transaction spans every shard, so an error
        // latched on any connection means whole transaction is aborted.
        for conn in &self.connections {
            if let Some(err) = conn.take_error() {
                return Err(err);
            }
        }

        let n_conns = self.connections.len();
        let is_direct = val.is_direct();

        // Runs per replicated change: the common single-target case
        // hands the `Bind` over without cloning it, and multi-target
        // writes clone once per extra target.
        let mut pending: Option<usize> = None;
        for shard in 0..n_conns {
            let target = match val {
                Shard::Direct(direct) => shard == *direct,
                Shard::Multi(multi) => multi.contains(&shard),
                _ => true,
            };
            // The transaction is already on this shard: its commit, at
            // `self.lsn`, comes before the last commit the origin recorded.
            if !target || self.lsn < self.applied_lsns[shard] {
                continue;
            }
            self.changed_shards[shard] = true;
            if let Some(previous) = pending.replace(shard) {
                self.connections[previous]
                    .execute(bind.clone(), is_direct)
                    .await?;
            }
        }
        if let Some(last) = pending {
            self.connections[last].execute(bind, is_direct).await?;
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
            let parse = if statements.omni {
                statements.upsert.parse()
            } else {
                statements.insert.parse()
            };
            let ctx = StreamContext::new(&self.cluster, &insert.tuple_data, parse).await?;
            {
                let (shard, bind) = ctx.into_parts();
                self.send(&shard, bind).await?;
            }
        }

        Ok(())
    }

    async fn update(&mut self, update: XLogUpdate) -> Result<(), Error> {
        if self.lsn_applied(&update.oid) {
            return Ok(());
        }

        if !self.statements.contains_key(&update.oid) {
            return Ok(());
        }

        // Route by pre-image variant — the WAL byte encodes replica identity:
        //   Key     →  DEFAULT/INDEX, identity column(s) changed
        //   Old     →  REPLICA IDENTITY FULL (always)
        //   Nothing →  DEFAULT/INDEX, identity column(s) unchanged
        match update.identity {
            UpdateIdentity::Key(ref key) => {
                // PK changed: delete old row by key, insert new row.
                // Identity columns must not be toasted — we need them to route the delete.
                self.check_toasted_identity(&update)?;
                if update.new.has_toasted() {
                    let table = self.get_table(update.oid)?;
                    return Err(Error::ToastedRowMigration {
                        table: table.publication,
                        oid: update.oid,
                    });
                }
                let delete = XLogDelete {
                    key: Some(key.clone()),
                    oid: update.oid,
                    old: None,
                };
                let insert = XLogInsert {
                    oid: update.oid,
                    tuple_data: update.new,
                };
                self.delete(delete).await?;
                self.insert(insert).await?;
                Ok(())
            }
            UpdateIdentity::Old(_) => {
                // REPLICA IDENTITY FULL: old row is fully materialised.
                // If every NEW column is unchanged-TOAST there is nothing to do.
                if update.new.all_toasted() {
                    return Ok(());
                }
                self.update_full_identity(update.oid, update).await
            }
            UpdateIdentity::Nothing => {
                // Identity columns unchanged; none may be toasted (routing needs them).
                self.check_toasted_identity(&update)?;
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
        let ctx = StreamContext::new(&self.cluster, new, &parse).await?;
        {
            let (shard, bind) = ctx.into_parts();
            self.send(&shard, bind).await?;
        }
        Ok(())
    }

    /// Slow-path UPDATE (DEFAULT/INDEX): at least one unchanged-TOAST column.
    /// Build a shape bitmask, look up or prepare the matching partial UPDATE
    /// statement, then bind and execute it.
    async fn update_with_toasted(&mut self, oid: Oid, update: XLogUpdate) -> Result<(), Error> {
        let table = self.get_table(update.oid)?;
        let present = NonIdentityColumnsPresence::from_tuple(&update.new, &table)?;

        if present.no_non_identity_present() {
            // All non-identity columns are unchanged-TOAST — destination already
            // has every value. No-op.
            return Ok(());
        }

        let partial_new = update.partial_new();
        let shape_stmt = self
            .ensure_update_shape_for(oid, &table, &present, false)
            .await?;
        let ctx = StreamContext::new(&self.cluster, &partial_new, shape_stmt.parse()).await?;
        {
            let (shard, bind) = ctx.into_parts();
            self.send(&shard, bind).await?;
        }
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
        if in_txn {
            // make sure during transaction we'll send the Sync message
            // to all shards.
            self.changed_shards.fill(true);
        }
        for server in &self.connections {
            for p in parses {
                debug!("preparing \"{}\" [{}]", p.query(), server.addr());
            }
        }
        // Each prepare targets an independent connection/task — run them
        // concurrently so the cost is one round-trip, not one per shard.
        try_join_all(
            self.connections
                .iter()
                .map(|server| server.prepare(parses, in_txn)),
        )
        .await?;
        Ok(())
    }

    // ── Routing helpers ────────────────────────────────────────────────────────────

    /// Route a tuple to its shard without constructing a `Bind`.
    /// Used when the bind merges multiple tuples (FULL identity UPDATE/DELETE).
    async fn shard_for(&self, tuple: &TupleData, parse: &Parse) -> Result<Shard, Error> {
        Ok(StreamContext::new(&self.cluster, tuple, parse)
            .await?
            .shard()
            .clone())
    }

    // ── Shape-cache helpers ──────────────────────────────────────────────────────

    /// Look up or prepare the UPDATE statement for `present`, cached under
    /// `statements[oid].update_shapes[present]`.
    ///
    /// `full_identity` selects the SQL generator on a cache miss:
    /// - `false` → `Table::update_partial` (DEFAULT/INDEX)
    /// - `true`  → `Table::update_full_identity_partial_set` (FULL)
    ///
    /// Both modes share `update_shapes`; no collision since `full_identity` is table-scoped.
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
            table.update_full_identity_partial_set(present)
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

    /// FULL identity UPDATE: WHERE on old-row values (`$1..$k`), SET on new-row values (`$k+1..$n`).
    /// On shard-key change fans out DELETE+INSERT across shards.
    async fn update_full_identity(&mut self, oid: Oid, update: XLogUpdate) -> Result<(), Error> {
        let table = self.get_table(oid)?;

        let old_full = match &update.identity {
            UpdateIdentity::Old(old) => old,
            _ => {
                return Err(Error::FullIdentityMissingOld {
                    table: table.table.to_string(),
                    oid,
                    op: "UPDATE",
                });
            }
        };

        let (update_parse, delete_parse, insert_parse) = {
            let stmts = self.statements.get(&oid).ok_or(Error::MissingKey)?;
            (
                stmts.update.parse().clone(),
                stmts.delete.parse().clone(),
                stmts.insert.parse().clone(),
            )
        };

        // Fill any 'u' (unchanged-TOAST) columns from old_full before routing.
        // FULL identity guarantees old_full is fully materialised; 'u' columns in
        // update.new carry the same value as the corresponding column in old_full.
        // Routing from a raw 'u' column yields empty bytes → wrong shard.
        let complete_new = update.new.fill_toasted_from(old_full)?;
        let new_shard = self.shard_for(&complete_new, &update_parse).await?;
        let old_shard = self.shard_for(old_full, &update_parse).await?;

        if new_shard != old_shard {
            // Shard key changed: DELETE on old shard, INSERT on new shard.
            let delete_bind = old_full.to_bind(delete_parse.name());
            self.send(&old_shard, delete_bind).await?;

            let insert_bind = complete_new.to_bind(insert_parse.name());
            self.send(&new_shard, insert_bind).await?;
            return Ok(());
        }

        let (parse, set_tuple, where_tuple) = if !update.new.has_toasted() {
            // Fast path: all columns present — use the pre-prepared statement.
            (update_parse, update.new, old_full.clone())
        } else {
            // Slow path: at least one unchanged-TOAST (`'u'`) column in new.
            let present = NonIdentityColumnsPresence::from_tuple(&update.new, &table)?;
            if present.no_non_identity_present() {
                return Ok(());
            }
            let partial_new = update.partial_new();
            let stmt = self
                .ensure_update_shape_for(oid, &table, &present, true)
                .await?;
            (stmt.parse().clone(), partial_new, old_full.clone())
        };

        // Fast path: WHERE $1..$n (where_tuple=old_full), SET $n+1..$2n (set_tuple=update.new).
        // Slow path: WHERE $1..$n (where_tuple=old_full), SET $n+1..$n+k (set_tuple=partial_new).
        let bind =
            XLogUpdate::full_identity_bind_tuple(&where_tuple, &set_tuple).to_bind(parse.name());
        self.send(&new_shard, bind).await?;
        Ok(())
    }

    async fn delete(&mut self, delete: XLogDelete) -> Result<(), Error> {
        if self.lsn_applied(&delete.oid) {
            return Ok(());
        }

        // Extract statement info upfront to release the shared borrow before
        // async calls and the subsequent &mut self borrows in send().
        let Some(stmts) = self.statements.get(&delete.oid) else {
            return Ok(());
        };
        let full_identity = stmts.full_identity;
        let delete_parse = stmts.delete.parse().clone();
        let oid = delete.oid;

        // Resolve the tuple used for both shard routing and the WHERE bind.
        // FULL identity matches on the full old row; DEFAULT/INDEX on key columns only.
        let tuple = if full_identity {
            // Postgres materialises all TOAST values before writing DELETE WAL records,
            // so old never contains 'u' markers.
            let Some(old) = delete.old else {
                let table = self.get_table(oid)?;
                return Err(Error::FullIdentityMissingOld {
                    table: table.table.to_string(),
                    oid,
                    op: "DELETE",
                });
            };
            old
        } else {
            let Some(key) = delete.key_non_null() else {
                // No key columns present — nothing to send.
                return Ok(());
            };
            key
        };

        let shard = self.shard_for(&tuple, &delete_parse).await?;
        let bind = tuple.to_bind(delete_parse.name());

        self.send(&shard, bind).await?;
        Ok(())
    }

    pub(crate) fn lsn_applied(&self, oid: &Oid) -> bool {
        if let Some(table_lsn) = self.table_lsns.get(oid) {
            // Don't apply change if the table copy or a confirmed commit already has it.
            if self.lsn <= *table_lsn {
                return true;
            }
        }

        false
    }

    // Handle Commit message.
    async fn commit(&mut self, commit: XLogCommit) -> Result<(), Error> {
        for (shard, conn) in self.connections.iter().enumerate() {
            // commit only shards that were changed
            if self.changed_shards[shard] {
                conn.commit(ReplicationOrigin::xact_setup_bind(&commit), commit.end_lsn)
                    .await?;
            }
        }
        self.changed_shards.fill(false);
        self.sent_lsn = commit.end_lsn;

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

                let statements = if table.is_identity_full() {
                    // ── FULL identity path ──────────────────────────────────────────────
                    let insert = Statement::new(&table.insert())?;
                    let update = Statement::new(&table.update_full_identity())?;
                    let delete = Statement::new(&table.delete_full_identity())?;

                    // Omni FULL: upsert dedup requires a unique constraint on the destination.
                    // Sharded FULL: each row routes to one shard — no upsert needed.
                    // Validated at connect() time.
                    let upsert = if omni {
                        Statement::new(&table.upsert_full_identity())?
                    } else {
                        warn!(
                            "table {} has REPLICA IDENTITY FULL and no primary key; \
                            replication performance will be degraded without an index \
                            on the destination table.",
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
                    // ── DEFAULT / INDEX path ────────────────────────────────────────────
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
            //
            // table.lsn represents every change UP TO the table LSN (not AT)
            // E.g., if it's at 100, we have 1-99
            //
            // This `table_lsns` map is what we check in the `lsn_applied()` method, where we
            // SKIP changes AT or below. If we stored 100, it would skip 100 (losing the change)
            // So, we store one less (99)
            let copied = table.lsn.lsn - 1;
            let watermark = self.table_lsns.entry(relation.oid).or_insert(copied);
            *watermark = (*watermark).max(copied);
            self.relations.insert(relation.oid, relation);
        }

        Ok(())
    }

    /// Reset destination connections and state, rolling back any open implicit
    /// transaction on each shard. Caches are repopulated from Relation messages on re-delivery.
    pub(crate) async fn reconnect(&mut self) -> Result<(), Error> {
        self.connections.clear();
        self.sent_lsn = self.committed_lsn;
        self.relations.clear();
        self.statements.clear();
        self.keys.clear();
        self.in_transaction = false;
        self.connect().await
    }

    /// Clear destination connections so the next `handle` call forces a fresh
    /// `connect()`. Use after a failed reconnect to avoid reusing connections
    /// that may have buffered stale handshake responses.
    pub(crate) fn reset_connections(&mut self) {
        self.connections.clear();
        self.sent_lsn = self.committed_lsn;
    }

    /// `docs/REPLICATION.md` → "Error rollback".
    pub(crate) async fn handle(&mut self, data: CopyData) -> Result<Option<StatusUpdate>, Error> {
        match self.handle_inner(data).await {
            Ok(status) => Ok(status),
            Err(err) => {
                // Drop sockets → backend FATAL → implicit transaction rolled back.
                // `Sync` would commit Rust-side errors. See docs/REPLICATION.md.
                self.connections.clear();
                self.sent_lsn = self.committed_lsn;
                // Per-session state — repopulated from Relation messages on reconnect.
                self.relations.clear();
                self.statements.clear();
                self.keys.clear();
                self.in_transaction = false;
                Err(err)
            }
        }
    }

    /// Check if the lsn was flushed on all destination shards to later point
    /// than [`Self::sent_lsn`]. Returns true if all the seen updates are
    /// durable, false otherwise.
    pub(crate) async fn check_confirmed_sent_lsn(&mut self) -> Result<bool, Error> {
        for conn in &self.connections {
            if let Some(err) = conn.take_error() {
                return Err(err);
            }
        }

        let confirmed = self
            .connections
            .iter()
            .filter_map(PipelinedConnection::durable_lsn_progress)
            .fold(self.sent_lsn, i64::min);

        if confirmed <= self.committed_lsn {
            return Ok(false);
        }

        for conn in &self.connections {
            self.missed_rows
                .merge(conn.take_confirmed_missed(confirmed));
        }
        // Update watermark on every table - the table is either before
        // shared watermark and it will be updated, or after
        // and it will use the current value.
        // After all tables synchronized all the tables will be updated
        // here at once.
        // PERF: we probably don't need to update it after all tables synced..
        for watermark in self.table_lsns.values_mut() {
            *watermark = (*watermark).max(confirmed - 1);
        }
        self.set_committed_lsn(confirmed);

        Ok(true)
    }

    async fn handle_inner(&mut self, data: CopyData) -> Result<Option<StatusUpdate>, Error> {
        // Lazily connect to all shards.
        if self.connections.is_empty() {
            self.connect().await?;
        }

        let mut status_update = None;

        if let Some(xlog) = data.xlog_data()
            && let Some(payload) = xlog.payload()
        {
            match payload {
                XLogPayload::Insert(insert) => {
                    self.insert(insert).await?;
                    self.rows_sharded += 1;
                }
                XLogPayload::Update(update) => {
                    self.update(update).await?;
                    self.rows_sharded += 1;
                }
                XLogPayload::Delete(delete) => {
                    self.delete(delete).await?;
                    self.rows_sharded += 1;
                }
                XLogPayload::Commit(commit) => {
                    self.commit(commit).await?;
                    status_update = Some(self.status_update());
                    self.in_transaction = false;
                }
                XLogPayload::Relation(relation) => self.relation(relation).await?,
                XLogPayload::Begin(begin) => {
                    self.changed_shards.fill(false);
                    self.set_working_lsn(begin.final_transaction_lsn);
                    self.in_transaction = true;
                }
                _ => (),
            }
            self.bytes_sharded += xlog.len();
        }

        Ok(status_update)
    }

    /// LSN of the last transaction committed to all destination shards.
    pub(crate) fn status_update(&self) -> StatusUpdate {
        StatusUpdate {
            last_applied: self.committed_lsn,
            last_flushed: self.committed_lsn,
            last_written: self.committed_lsn,
            system_clock: postgres_now(),
            reply: 0,
        }
    }

    /// Number of bytes processed.
    pub(crate) fn bytes_sharded(&self) -> usize {
        self.bytes_sharded
    }

    /// Number of rows applied.
    pub(crate) fn rows_sharded(&self) -> usize {
        self.rows_sharded
    }

    /// Advance both LSN fields. Call after commit and on publisher init.
    pub(crate) fn set_current_lsn(&mut self, lsn: i64) -> bool {
        self.lsn_changed = lsn != self.lsn;
        self.lsn = lsn;
        self.committed_lsn = lsn;
        self.lsn_changed
    }

    fn set_committed_lsn(&mut self, lsn: i64) {
        self.committed_lsn = lsn;
        if lsn > self.lsn {
            self.lsn_changed = true;
            self.lsn = lsn;
        }
    }

    /// Advance working LSN only. Used on Begin; does not move the ack pointer.
    fn set_working_lsn(&mut self, lsn: i64) {
        self.lsn_changed = lsn != self.lsn;
        self.lsn = lsn;
    }

    /// Get current LSN.
    pub(crate) fn lsn(&self) -> i64 {
        self.lsn
    }

    pub(crate) fn committed_lsn(&self) -> i64 {
        self.committed_lsn
    }

    /// Whether we are inside a transaction.
    pub(crate) fn in_transaction(&self) -> bool {
        self.in_transaction
    }

    /// Missed rows of all transactions committed so far. Resets on read.
    /// Rows of a transaction that failed are never counted, because the
    /// source sends that transaction again after a reconnect.
    pub(crate) fn missed_rows(&mut self) -> MissedRows {
        std::mem::take(&mut self.missed_rows)
    }

    /// Verify every destination shard has a qualifying unique index for all `tables`.
    /// FULL-identity omni tables use `ON CONFLICT DO NOTHING` during the
    /// copy-replication overlap window, which requires a unique constraint.
    /// Queries all shards in parallel (one bulk query per shard) then surfaces
    /// the complete set of missing indexes across the cluster in a single error.
    async fn validate_full_identity_omni_has_unique_index(
        &self,
        servers: &mut [Server],
        tables: &[Table],
    ) -> Result<(), Error> {
        // Fan out to all shards concurrently; each gets one IN-list query.
        let per_shard: Vec<Vec<String>> = try_join_all(servers.iter_mut().map(|dest_server| {
            tables_missing_unique_index(tables.iter().map(|t| &t.table), dest_server)
        }))
        .await?;

        // Flatten; ensure_validation! deduplicates and sorts before reporting.
        let errors: Vec<TableValidationError> = per_shard
            .into_iter()
            .flatten()
            .map(|table_name| TableValidationError {
                table_name,
                kind: TableValidationErrorKind::FullIdentityOmniNoUniqueIndex,
            })
            .collect();
        ensure_validation!(errors);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{begin_copy_data, commit_copy_data, test_origins};
    use super::*;
    use crate::config::config;

    async fn make_subscriber() -> StreamSubscriber {
        let cluster = Cluster::new_test(&config());
        StreamSubscriber::new(&cluster, vec![], test_origins(&cluster).await)
    }

    #[tokio::test]
    async fn apply_begin_no_status_update() {
        let cluster = Cluster::new_test(&config());
        cluster.launch();
        let mut stream = StreamSubscriber::new(&cluster, vec![], test_origins(&cluster).await);
        stream.connect().await.unwrap();

        let result = stream.handle(begin_copy_data(1)).await;

        assert!(
            result.unwrap().is_none(),
            "Begin event must not emit a status update"
        );
        cluster.shutdown();
    }

    #[tokio::test]
    async fn apply_commit_emits_status_update() {
        let cluster = Cluster::new_test(&config());
        cluster.launch();
        let mut stream = StreamSubscriber::new(&cluster, vec![], test_origins(&cluster).await);
        stream.connect().await.unwrap();

        let result = stream.handle(commit_copy_data(1)).await;

        assert!(
            result.unwrap().is_some(),
            "commit should produce a status update"
        );
        cluster.shutdown();
    }

    #[tokio::test]
    async fn table_watermarks_advance_on_commit() {
        let mut sub = make_subscriber().await;
        sub.connect().await.unwrap();
        let oid = Oid(42);

        sub.table_lsns.insert(oid, 50);
        sub.handle(begin_copy_data(100)).await.unwrap();

        // Rows from the current transaction must remain eligible until commit.
        assert!(!sub.lsn_applied(&oid));

        sub.handle(commit_copy_data(200)).await.unwrap();
        assert_eq!(sub.table_lsns.get(&oid), Some(&50));

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while sub.committed_lsn() < 200 {
            assert!(std::time::Instant::now() < deadline);
            sub.check_confirmed_sent_lsn().await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        // The confirmed transaction is skipped if it arrives again.
        sub.handle(begin_copy_data(100)).await.unwrap();
        assert!(sub.lsn_applied(&oid));

        // A transaction that begins exactly at the confirmed LSN is not confirmed.
        sub.handle(begin_copy_data(200)).await.unwrap();
        assert!(!sub.lsn_applied(&oid));
    }

    /// Begin message sets in_transaction and records the LSN.
    #[tokio::test]
    async fn begin_sets_transaction_state() {
        let mut sub = make_subscriber().await;
        assert!(!sub.in_transaction());
        assert_eq!(sub.lsn(), 0);

        sub.connect().await.unwrap();
        sub.handle(begin_copy_data(100)).await.unwrap();

        assert!(sub.in_transaction());
        assert_eq!(sub.lsn(), 100);
        assert!(sub.lsn_changed);
    }

    /// set_current_lsn returns true only when the LSN changes.
    #[tokio::test]
    async fn lsn_changed_tracking() {
        let mut sub = make_subscriber().await;

        assert!(sub.set_current_lsn(100));
        assert!(sub.lsn_changed);

        assert!(!sub.set_current_lsn(100));
        assert!(!sub.lsn_changed);

        assert!(sub.set_current_lsn(200));
        assert!(sub.lsn_changed);
    }
}
