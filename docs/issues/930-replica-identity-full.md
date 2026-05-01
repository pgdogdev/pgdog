# REPLICA IDENTITY FULL support

[github.com/pgdogdev/pgdog/issues/930](https://github.com/pgdogdev/pgdog/issues/930)

Resharding tables that carry `REPLICA IDENTITY FULL` was previously blocked at validation
time (see issue #914). This document describes the design for correct support.

---

## Background: what REPLICA IDENTITY FULL does

PostgreSQL's logical replication protocol encodes a row-identity in WAL for `UPDATE` and
`DELETE` operations. The identity controls what columns are included in the "old" tuple:

| Setting | Old row in WAL | When useful |
|---------|---------------|-------------|
| `DEFAULT` | Only primary key columns | Table has a primary key |
| `INDEX` | Only the nominated unique index columns | No PK, but has a suitable unique index |
| `FULL` | All columns of the row | No PK, no unique index |
| `NOTHING` | Nothing — UPDATE/DELETE cannot be replicated | Unsupported |

With `FULL`, every `UPDATE` and `DELETE` WAL record carries the complete old row
(protocol marker `'O'`). This is the only mechanism PostgreSQL provides to identify
a row when no unique index exists.

---

## TOAST and the Toasted marker

PostgreSQL may store large column values (TEXT, BYTEA, JSONB, etc.) out-of-line in
TOAST storage. When an `UPDATE` does not modify a large column, PostgreSQL omits its
value from the WAL record and instead emits a `Toasted` marker (protocol byte `'u'`).

This has a direct consequence for replication:

- **`update.old` Toasted columns**: under `REPLICA IDENTITY FULL`, this cannot occur.
  PostgreSQL calls `toast_flatten_tuple` on OLD in `ExtractReplicaIdentity` before
  WAL-logging; OLD is always fully materialised. Toasted markers only appear in `update.new`.
- **`update.new` Toasted columns**: the column was not changed. It must not appear
  in the `SET` clause (we have no value to assign). The presence bitmask is derived
  from `update.new`; OLD is left intact and binds in full to the WHERE clause.

---

## Replica identity verification

`sync_tables()` reads two things from the source for each table:

1. `pg_class.relreplident` → stored as `ReplicaIdentity.identity`
   (`'d'` = DEFAULT, `'f'` = FULL, `'i'` = INDEX, `'n'` = NOTHING)
2. Column list via `pg_get_replica_identity_index(oid)` → for DEFAULT/INDEX this
   returns the index OID and marks those columns with `identity = true`. For FULL this
   function returns `NULL`, so the LEFT JOIN produces nothing and **every column gets
   `identity = false`**.

The column-level `identity` flag carries no information for FULL tables.
`ReplicaIdentity.identity == 'f'` is the sole authority for determining FULL mode.

**Pre-copy validation** (`valid()`):

| Value | Meaning | Action |
|-------|---------|--------|
| `'f'` | FULL | Accept — `valid()` matches `"f"` and returns `Ok(())`. FULL tables carry no identity columns in metadata; the subscriber routes through `update.old` / `delete.old` instead. |
| `'n'` | NOTHING | Reject — `ReplicaIdentityNothing` error |
| `'d'` / `'i'` | DEFAULT / INDEX | Accept only if at least one column has `identity = true` |

`Statements` carries `full_identity: bool`, set at `relation()` time from `ReplicaIdentity.identity == "f"`. Every INSERT, UPDATE, and DELETE dispatch branch checks this flag to route FULL-mode events to their own handlers.

**Additional check at `connect()` time for non-sharded (omni) FULL tables:**
query `pg_index` on the destination for any unique index whose every key column is `attnotnull`,
or whose `indnullsnotdistinct = true` (PG15+, NULLs treated as equal in the unique constraint).
PgDog enforces no schema-uniformity invariant across shards, so the probe runs on every
shard's primary connection — a single-shard probe could miss drift that surfaces later as a
non-retryable `FullIdentityAmbiguousMatch`. If any shard lacks a usable unique index, return
`TableValidationError::FullIdentityOmniNoUniqueIndex(table)` — the same error path as
`ReplicaIdentityNothing`, surfaced to the operator before streaming begins.

Error message: `"table {schema}.{name} has REPLICA IDENTITY FULL and is not sharded
(omni) but the destination has no unique index — add a unique index or use
REPLICA IDENTITY USING INDEX"`.

Why NOT NULL is required: standard PostgreSQL treats NULLs as distinct in unique indexes,
so a unique index over a nullable column does NOT prevent two NULL-keyed duplicates; that
would let `ON CONFLICT DO NOTHING` admit duplicates during the copy/replication overlap
window, then any FULL `UPDATE`/`DELETE` would match both copies and fail.
Note: a plain nullable unique index is rejected; the destination probe accepts an index
only if every key column has `attnotnull = true` or the index has `indnullsnotdistinct = true`
(PG15+).

**Runtime consistency check.** Once FULL identity is accepted, the subscriber detects
when the source table's replica identity changes mid-stream:

| Condition | Action |
|-----------|--------|
| `statements.full_identity` and `update.identity` is `Key` or `Nothing` | `Error::FullIdentityMissingOld { op: "UPDATE" }` |
| `statements.full_identity` and `delete.old.is_none()` | `Error::FullIdentityMissingOld { op: "DELETE" }` |
| Sharded FULL UPDATE crosses shards but `update.new.has_toasted()` | `Error::FullIdentityCrossShardToasted` (cannot rebuild destination row) |

---

## INSERT

The strategy differs between sharded and non-sharded (omni) tables, and for omni tables
also depends on whether the destination has a unique constraint.

**Sharded tables** — plain `INSERT`:

```sql
INSERT INTO "schema"."table" ("col1", "col2", ...) VALUES ($1, $2, ...)
```

Idempotency during the copy-to-replication overlap window is provided by the LSN gate
(`lsn_applied()`). Duplicate rows from different shards do not occur for sharded data.

**Non-sharded (omni) tables** — unique index required on destination:

At `connect()` time, query `pg_index` on the destination for any unique index
(`indisunique = true`). If none exists, fail with
`TableValidationError::FullIdentityOmniNoUniqueIndex` before preparing any statements.
If a unique index is found, use `INSERT … ON CONFLICT DO NOTHING`:

```sql
INSERT INTO "schema"."table" ("col1", "col2", ...) VALUES ($1, $2, ...) ON CONFLICT DO NOTHING
```

`ON CONFLICT DO NOTHING` with no conflict target covers all unique constraints at once.
Duplicate rows arriving during the copy-to-replication overlap are skipped silently.

This is stored in the `upsert` slot of `Statements` and selected by the existing
`omni == true` branch in the `insert()` handler. At `relation()` time, FULL tables
populate `upsert` with `ON CONFLICT DO NOTHING` instead of the DEFAULT/INDEX version
(`ON CONFLICT (identity_cols) DO UPDATE SET …`).

---

## UPDATE

A FULL identity `UPDATE` WAL record carries the full old row (`update.old`) in its
pre-image and the full new row (`update.new`) as the post-image. PostgreSQL passes
the OLD tuple through `ExtractReplicaIdentity` → `toast_flatten_tuple` before emitting
the WAL record, so OLD never carries `'u'` (unchanged-TOAST) markers; NEW still does
for any external on-disk TOAST value that did not change.

The correct replication operation is a **single `UPDATE` statement**:

```sql
UPDATE "schema"."table"
SET   "new_col_a" = $N+1, "new_col_b" = $N+2, ...   -- the K non-Toasted cols of update.new
WHERE "old_col_1" IS NOT DISTINCT FROM $1
  AND "old_col_2" IS NOT DISTINCT FROM $2          -- all N old columns, in table order
  AND ...
```

- **WHERE clause**: occupies parameter indices `$1..$N` and covers **all** `N` columns
  of `update.old`, in table order. PostgreSQL detoasts OLD before WAL-logging, so it is
  always fully materialised; we never slice it.
- **SET clause**: occupies parameter indices `$N+1..$N+K`. Columns from `update.new`
  whose `identifier != Toasted`, in table column order — `K ≤ N`.
- Bind order is `[where_tuple_full][set_tuple_partial]` for `N+K` parameters total.
- `IS NOT DISTINCT FROM` is required so that NULL-valued columns participate in the match.

Sharded shard-key change is detected by routing the OLD and NEW tuples through the
router. When they map to different shards, PgDog falls back to `DELETE` on the old
shard plus `INSERT` on the new shard via the table's pre-prepared statements. If the
new tuple has unchanged-TOAST columns the row cannot be rebuilt for INSERT, and the
event fails non-retryably with `Error::FullIdentityCrossShardToasted`.

Decomposing every UPDATE into DELETE + INSERT was rejected: the FULL WHERE clause
alone can match more than one logically-identical row, and we want the destination
to apply the change atomically and report the ambiguity as one error rather than
racing two separate operations.

| Edge case | Action |
|-----------|--------|
| All `update.new` columns Toasted | SET empty → nothing changed → skip silently |
| `update.identity` is `Key` or `Nothing` on a FULL table | Source replica identity changed mid-stream → `Error::FullIdentityMissingOld` |
| Sharded shard-key change with unchanged-TOAST in NEW | Cannot reconstruct destination row → `Error::FullIdentityCrossShardToasted` |
| Zero rows affected | Not an error — row may have been deleted concurrently |
| More than one row affected | `send()` returns the row count to `update()`; when `statements.full_identity`, `update()` raises `Err(FullIdentityAmbiguousMatch)` — task exits, connections drop, PostgreSQL implicitly rolls back the open transaction, operation fails non-retryably |

---

## DELETE

```sql
DELETE FROM "schema"."table"
WHERE "col_x" IS NOT DISTINCT FROM $1
  AND "col_y" IS NOT DISTINCT FROM $2
  AND ...
```

All non-Toasted columns from `delete.old`. Actual DELETE WAL records never contain
Toasted markers — PostgreSQL fetches TOAST values before writing the record.

| Edge case | Action |
|-----------|--------|
| `delete.old` absent on a FULL table | Source replica identity changed mid-stream → `Error::FullIdentityMissingOld` |
| All `delete.old` columns Toasted (would imply replica-identity drift) | WHERE empty → would delete all rows → log error, skip |
| Zero rows affected | Not an error — row may have been deleted concurrently |
| More than one row affected | `send()` returns the row count to `delete()`; when `statements.full_identity`, `delete()` raises `Err(FullIdentityAmbiguousMatch)` — task exits, connections drop, PostgreSQL implicitly rolls back the open transaction, operation fails non-retryably |

---

## Dynamic query generation

For tables with unchanged-TOAST columns in `update.new`, PgDog cannot reuse a single
pre-prepared UPDATE statement — the parameter count varies by which columns are Toasted
in each WAL event, and this is not known until the event arrives.

The subscriber maintains a single per-table shape cache inside `Statements`:

```
update_shapes: HashMap<NonIdentityColumnsPresence, Statement>
```

**`NonIdentityColumnsPresence`** is a `BitVec` bitmask (one bit per non-identity column,
in table order). Bit `i` is set when the `i`-th non-identity column is present (not
`Toasted`). For DEFAULT/INDEX tables identity columns are excluded from the mask; for
FULL tables every column has `identity = false` so the bit index equals the column
index directly.

A table is always either FULL or DEFAULT/INDEX — never both — so there is no key
collision between the two modes. The `full_identity` flag controls which SQL generator
is called on a cache miss; the cache structure is shared.

**Fast path** (`!update.new.has_toasted()`): all columns present — bind directly to the
pre-prepared `update` statement. No cache lookup, no `update_shapes` interaction.
- DEFAULT/INDEX: bind `update.new` columns (all); `$N` positions match `update()`'s
  non-identity-in-SET / identity-in-WHERE layout.
- FULL: bind `[update.old columns][update.new columns]` — all `N` old cols occupy
  `$1..$N` (WHERE), all `N` new cols occupy `$N+1..$2N` (SET). The `update` slot was
  prepared at `relation()` time via `Table::update_full_identity()` with all columns
  present; this exact shape never appears as an `update_shapes` key because the slow
  path only fires when `update.new.has_toasted()` is true.

**Slow path** (`update.new.has_toasted()`):

1. `NonIdentityColumnsPresence::from_tuple(&update.new, table)` builds the bitmask.
   The mask is **always derived from `update.new`**: real PostgreSQL detoasts the OLD
   tuple in `ExtractReplicaIdentity` before emitting the record, so `update.old` does
   not carry `'u'` markers and would always produce an all-set mask.
2. If `present.no_non_identity_present()` — every non-identity column is Toasted —
   skip silently. For DEFAULT/INDEX: identity columns are still the correct WHERE;
   for FULL: nothing to SET and the destination already has every value.
3. Look up `present` in `update_shapes`. On a **cache miss**:
   - DEFAULT/INDEX: `table.update_partial(&present)` — present non-identity cols in
     `SET`, identity cols in `WHERE` (`"col" = $N`).
   - FULL: `table.update_full_identity_partial_set(&present)` — **all** OLD columns in
     `WHERE` (`"col" IS NOT DISTINCT FROM $1..$N`), only the `K` present NEW columns
     in `SET` (`"col" = $N+1..$N+K`).
   - `Statement::new()` assigns a unique `__pgdog_repl_N` name and parses the SQL.
   - Sends `Parse + (Flush|Sync)` to every shard; reads `ParseComplete ('1')`.
     Inside a transaction: `Parse + Flush`, read 1 message. Outside: `Parse + Sync`,
     read 2 messages (ParseComplete + ReadyForQuery).
   - Inserts into `update_shapes`.
4. On a **cache hit** (including the just-prepared entry):
   - DEFAULT/INDEX: bind via `update.partial_new().to_bind(name)`.
   - FULL: build the bind tuple via
     `XLogUpdate::full_identity_bind_tuple(&old_full, &update.partial_new())`. OLD is
     never sliced — it occupies `$1..$N` (WHERE) in full because PostgreSQL detoasts
     OLD before WAL-logging. NEW is filtered by `partial_new()` to drop `'u'`-marked
     columns; the remaining `K` values occupy `$N+1..$N+K` (SET). Total bind size is
     `N+K` parameters, matching the SQL emitted by `update_full_identity_partial_set`.
5. Send `Bind + Execute + Flush`; read `BindComplete ('2') + CommandComplete ('C')`.
   Identical to the fast path — no extra round-trips after the first occurrence.

The Parse cost is paid **once per distinct Toasted-column pattern**, not once per event.
For most workloads only a small number of distinct patterns occur (e.g. one pattern where
a large JSONB column is always unchanged, another where it is modified).

**Statement preparation at `relation()` time**

| Statement | DEFAULT / INDEX | FULL |
|-----------|----------------|------|
| `insert` | Plain `INSERT` (sharded, `omni == false`) | Plain `INSERT` (sharded, `omni == false`) |
| `upsert` | `INSERT … ON CONFLICT (identity_cols) DO UPDATE SET …` (omni) | `INSERT … ON CONFLICT DO NOTHING` (omni) — no conflict target; covers any unique index on the destination |
| `update` (fast path) | Prepared at `relation()` time | Prepared at `relation()` time via `Table::update_full_identity()` (no `present` arg) — `WHERE col IS NOT DISTINCT FROM $1..$N; SET col=$N+1..$2N` |
| `update_shapes` | Populated lazily per `NonIdentityColumnsPresence` | Populated lazily per `NonIdentityColumnsPresence` — same map, different SQL generator |
| `delete` | `WHERE identity_col = $N` | `WHERE col IS NOT DISTINCT FROM $N` — all columns, via `Table::delete_full_identity()`, prepared at `relation()` time |

`Statements.full_identity = true` enables the FULL dispatch path across INSERT, UPDATE,
and DELETE handlers.

WAL tuple data carries values but not column names. `columns: Vec<PublicationTableColumn>`
on `Table` stores the metadata from `sync_tables()` so names are available when building
slow-path SQL via `update_partial()` and `update_full_identity()`.

---

## Execution flow and index availability

Understanding when indexes exist on the destination is critical for evaluating
performance. The two paths through the codebase differ:

**`replicate_and_cutover()` — RESHARD path** (`orchestrator.rs`):
```
schema_sync_pre()    -- tables, primary keys
data_sync()          -- COPY (bulk load into unindexed heap, intentionally fast)
schema_sync_post()   -- ALL secondary indexes, foreign keys  <-- before replication
replicate()          -- catch-up replication against a fully indexed destination
cutover()
```

**`COPY_DATA` admin command** (`copy_data.rs`):
```
schema_sync_pre()    -- tables, primary keys
data_sync()          -- COPY
replicate()          -- catch-up replication  <-- no schema_sync_post before this
```

In the RESHARD path, all secondary indexes — including the nominated replica identity
index for INDEX-mode tables and any secondary indexes on FULL-mode tables copied from
the source — are present before catch-up replication begins. UPDATE and DELETE lookups
in the subscriber use those indexes. No sequential scan problem.

In the COPY_DATA path, `schema_sync_post` is never called. The destination has only
the primary key index during catch-up. For FULL identity tables (which typically have
no primary key either), every UPDATE and DELETE is a full sequential scan for the
duration of catch-up. For INDEX-mode tables, the nominated replica identity index is
absent, causing the same degradation. This is a structural gap in the COPY_DATA path:
it was designed for cases where the destination schema already exists, but if used for
a fresh destination with FULL or INDEX identity tables receiving frequent writes,
replication lag will compound.

**Per-shape Parse overhead.** Each distinct Toasted-column pattern in UPDATE events incurs
one Parse round-trip on first occurrence; subsequent events with the same pattern pay only
the normal Bind + Execute cost. For append-only tables (no UPDATEs) this is zero.

---

## When to use REPLICA IDENTITY FULL

FULL identity is appropriate for:

| Table type | Reason |
|-----------|--------|
| Append-only (audit, event, log) | No UPDATE/DELETE WAL events; FULL costs nothing regardless of path |
| Small reference/lookup tables | Write rate low; per-event Parse overhead negligible |
| Configuration/metadata tables | Small, rarely modified |
| Tables with no natural key | No alternative exists |

FULL identity is **not appropriate** for:

| Table type | Reason |
|-----------|--------|
| Large write-heavy tables via COPY_DATA | No secondary indexes during catch-up; sequential scans compound lag |
| Tables with duplicate rows on non-key columns | FULL WHERE clause may match multiple destination rows; pgdog will fail the replication task when `rows_affected > 1` is detected |
| Tables where `ALTER TABLE t REPLICA IDENTITY USING INDEX` is possible | Use that instead; avoids slow-path shape-cache overhead entirely |

---

## Known limitations
**Duplicate rows — hard failure and implicit rollback.** When a FULL-mode `UPDATE` or
`DELETE` produces `rows_affected > 1`, the WHERE clause matched more than one row on
the destination. This means the destination already held logically duplicate rows that
REPLICA IDENTITY FULL cannot distinguish.

`CommandComplete` returns the affected-row count, which `send()` already parses for the
existing `rows == 0` check. `send()` plumbs `rows` out to its caller; `update()` and
`delete()` inspect it only when `statements.full_identity` is set, and raise
`Err(FullIdentityAmbiguousMatch { op, rows, oid })`. Keeping the variant construction in
the dispatch handlers (not in `send()`) preserves `send()` as a path-agnostic primitive.
The error propagates through `update()` / `delete()` → `handle()` → the replication loop
in `publisher_impl.rs`. The spawned task exits, dropping `StreamSubscriber` and all its
shard `Server` connections. PostgreSQL rolls back any open transaction implicitly when a
client disconnects mid-transaction.

**Invariant — failure must precede the destination COMMIT.** The implicit-rollback
mechanism is only safe because each source xact maps to exactly one destination
BEGIN → statements → COMMIT span, and the failing UPDATE/DELETE is dispatched *before*
the matching `XLogCommit` message produces the destination COMMIT. The current handler
ordering satisfies this: every `Update`/`Delete` WAL message is applied inside the open
transaction, and `Commit` is a separate dispatch that lands afterward. Any future
refactor that batches the COMMIT eagerly, applies multiple source xacts in one
destination xact, or autocommits per-statement breaks this invariant — the
ambiguous-match error must remain detectable while the destination transaction is still
open.

The failed task is **non-retryable** — `Error::is_retryable()` returns `false` for
`FullIdentityAmbiguousMatch`. The same source transaction would produce the same
`rows > 1` result on every retry, so automatic retry would loop forever. The
RESHARD or `COPY_DATA` operation fails and surfaces the error to the operator.

The operator sees: a failed reshard or copy-data command, an error message naming the
table, the operation (`UPDATE`/`DELETE`), and the number of rows matched. The
destination is consistent up to the last successfully committed replication transaction.
The fix is to resolve duplicate rows on the source before retrying, or to switch to
`REPLICA IDENTITY USING INDEX` if a suitable unique index exists.


**No reconnect on mid-copy failure.** `standalone()` connections have no reconnect.
A dropped connection fails the entire copy task with no retry. Resume-from-LSN is
structurally possible (the `lsn` field on `Table` records WAL position reached) but
not yet implemented.


---

## Implementation plan

Phases are ordered by dependency. Each phase is a self-contained commit that includes
its own unit tests.

### ✅ Phase 1 — Table SQL generation (`logical/publisher/table.rs`)

No dependencies. The new SQL generators are pure string-building with no imports from
other phases.

- ✅ Add `upsert_full_identity(&self) -> String` — `INSERT … ON CONFLICT DO NOTHING`
- ✅ Add `delete_full_identity(&self) -> String` — all columns in WHERE with `IS NOT DISTINCT FROM`
- ✅ Add `update_full_identity(&self) -> String` — fast-path SQL with **all** columns in
  both clauses: `WHERE col IS NOT DISTINCT FROM $1..$N; SET col = $N+1..$2N`. No `present`
  argument; the fast path only fires when every NEW column is non-Toasted.
- ✅ Add `update_full_identity_partial_set(&self, present: &NonIdentityColumnsPresence) -> String`
  — slow-path SQL with **all** OLD columns in `WHERE` ($1..$N) and only the `K` present NEW
  columns in `SET` ($N+1..$N+K).
- ✅ Add `NonIdentityColumnsPresence::count_present()` — count of set bits, used by
  `update_full_identity_partial_set` to size the SET clause.
- ✅ Add `Columns::with_offset(n)` — index-shift transformer composable with any terminal method
- ✅ Unit tests: all-present and partial-present cases for all three generators

### ✅ Phase 2 — Error variants + `valid()` fix (`logical/error.rs`, `logical/publisher/table.rs`)

No dependency on Phase 1 — only touches `error.rs` and the `valid()` switch in `table.rs`, neither of which imports the new SQL generators. Phases 1, 2, 3, and 4 may proceed in parallel; Phase 5 fans them in.

- `error.rs`: add `ReplicaIdentityNothing(PublicationTable)` to `TableValidationError`
- `error.rs`: add `FullIdentityOmniNoUniqueIndex(PublicationTable)` to `TableValidationError`
- `error.rs`: add `FullIdentityAmbiguousMatch { table: PublicationTable, oid: Oid, op: &'static str, rows: u64 }`
  to `Error` with `is_retryable() → false`. Field `table` is the offending table identifier,
  so the error message names the actual table rather than the publication.
- `error.rs`: add `FullIdentityMissingOld { table: PublicationTable, oid: Oid, op: &'static str }`
  for the case when a FULL-mode UPDATE/DELETE lacks the OLD pre-image (replica identity changed
  mid-stream). Non-retryable.
- `error.rs`: add `FullIdentityCrossShardToasted { table: PublicationTable, oid: Oid }` for
  sharded FULL UPDATEs that would cross shards but cannot reconstruct the destination row
  due to unchanged-TOAST in NEW. Non-retryable.
- `table.rs`: fix `valid()` — `"f"` → `Ok(())`, `"n"` → `ReplicaIdentityNothing`,
  `_` → existing identity-column check
- Unit tests: extend `not_retryable` for the new `Error` and `TableValidationError` variants;
  update `test_valid_replica_identity_full` (now `Ok`) and
  `test_valid_replica_identity_nothing` (now `ReplicaIdentityNothing`).

### ✅ Phase 3 — Protocol message (`net/messages/replication/logical/update.rs`)

No dependencies. Standalone addition to `impl Update`.

- ✅ Replace the prior `Option<TupleData>` pre-image field with
  `pub enum UpdateIdentity { Key(TupleData), Old(TupleData), Nothing }` and strict
  marker validation in `FromBytes`: any byte after the OID that is not `'K'`/`'O'`/`'N'`
  returns `Error::UnexpectedMessage` rather than silently falling through to `Nothing`.
- ✅ Add `Update::partial_new(&self) -> TupleData` filtering `Identifier::Toasted` from the
  NEW tuple. Identity columns never carry `'u'` markers (PG omits them from the unchanged-
  TOAST set), so this is the SET tuple for both DEFAULT/INDEX and FULL.
- ✅ Add `Update::full_identity_bind_tuple(where_cols, set_cols) -> TupleData` concatenating
  the two tuples in a single bind for FULL UPDATE: WHERE params at `$1..$N`, SET params at
  `$N+1..$N+K` (slow path) or `$N+1..$2N` (fast path).
- ✅ Add `TupleData::without_toasted()` and `TupleData::all_toasted()` so `partial_new` and
  the slow-path no-op short-circuit share one definition of "drop unchanged-TOAST sentinels".
- ✅ Unit tests: Toasted columns stripped; non-Toasted columns preserved;
  `to_bytes` ↔ `from_bytes` round-trip for `Key`, `Old`, and `Nothing` variants;
  malformed marker byte → `Error::UnexpectedMessage`.
- The slow-path subscriber does NOT slice OLD: PostgreSQL detoasts OLD via `toast_flatten_tuple`
  before WAL-logging, so OLD already has every column present. Derive `present` from
  `update.new` and bind `[old_full][partial_new]` of `N+K` parameters via
  `full_identity_bind_tuple`. The WHERE clause covers all `N` OLD columns at `$1..$N`.

### ✅ Phase 4 — Destination unique-index query (`logical/publisher/queries.rs`)

No code dependencies. Required before Phase 5.

- Add `has_unique_index(schema: &str, name: &str, server: &mut Server) -> Result<bool, Error>`
- SQL: `SELECT 1 FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON c.oid = i.indrelid`
  `JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace`
  `WHERE n.nspname = $schema AND c.relname = $name AND i.indisunique`
  `AND i.indisvalid AND i.indisready AND i.indislive`
  `AND i.indpred IS NULL AND i.indexprs IS NULL`
  `AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_attribute a`
  `WHERE a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) AND NOT a.attnotnull)`
  `LIMIT 1`
  - `indisvalid AND indisready AND indislive` filters out indexes mid-build via
    `CREATE UNIQUE INDEX CONCURRENTLY` and indexes being concurrently dropped, neither of
    which can be relied on for `ON CONFLICT DO NOTHING` deduplication.
  - `indpred IS NULL` excludes partial indexes — their unique constraint covers only the
    rows matching the predicate, so duplicates outside the predicate slip past `ON CONFLICT`.
  - `indexprs IS NULL` excludes expression indexes — their constraint is on a computed value,
    not the raw column tuple, so two distinct row values can map to the same indexed value.
- Probe **every shard's** connection: PgDog enforces no schema-uniformity invariant
  across shards, so a single-shard probe can hide drift that surfaces later as a
  non-retryable `FullIdentityAmbiguousMatch`.
- Unit tests: table without unique index → `false`; after `CREATE UNIQUE INDEX` → `true`.

### ✅ Phase 5 — Stream subscriber (`logical/subscriber/stream.rs`, `logical/subscriber/tests.rs`)

Depends on Phases 1–4.

- `Statements`: add `full_identity: bool`
- `connect()`: for every omni FULL table call `has_unique_index()` on every destination
  shard. If any shard lacks a usable unique index, return
  `TableValidationError::FullIdentityOmniNoUniqueIndex` before any streaming begins.
- `relation()`: after `table.valid()`, branch on `table.identity.identity == "f"`:
  - Prepare `insert` (plain INSERT), `update` (`update_full_identity()` all-columns fast path),
    `delete` (`delete_full_identity`)
  - If `omni`: prepare `upsert` (`INSERT … ON CONFLICT DO NOTHING`)
  - Store `Statements { full_identity: true, … }`; skip DEFAULT/INDEX `upsert`/`update`
- `update()`: after existing `key` branch, check `statements.full_identity`:
  - Resolve OLD and NEW shards via the router. If they differ on a sharded table:
    - With unchanged-TOAST in NEW → `Error::FullIdentityCrossShardToasted`
    - Otherwise → `DELETE` on old shard via `delete_full_identity`, `INSERT` on new shard via `insert`
  - Same shard, no Toasted → fast path (pre-prepared `update`,
    bind `[old cols][new cols]`, check `rows > 1`)
  - Same shard, has Toasted → slow path (shape cache via `update_full_identity_partial_set(present)`,
    bind `[old_full][partial_new]` of `N+K` params via `full_identity_bind_tuple`, check `rows > 1`)
  - `update.identity` not `Old` on a FULL table → `Error::FullIdentityMissingOld`
- `delete()`: when `full_identity`, bind `delete.old` to `statements.delete`;
  check `rows > 1` → `Err(FullIdentityAmbiguousMatch)`. `delete.old` absent → `Error::FullIdentityMissingOld`
- `insert()`: no change — existing `omni` dispatch selects the correct slot
- Sharded FULL tables: emit a `warn!` at `relation()` time naming the table — every UPDATE/DELETE will be a sequential scan unless secondary indexes are present on the destination, and lag will compound during catch-up. Omni FULL tables already require a unique index (Phase 4), so the warning is redundant for them.
  - `full_identity_insert_sharded` — plain INSERT lands once
  - `full_identity_insert_omni_dedup` — `ON CONFLICT DO NOTHING` skips replay duplicate
  - `full_identity_update_fast_path` — all columns non-Toasted; UPDATE applied correctly
  - `full_identity_update_slow_path` — one Toasted column in NEW; partial UPDATE via
     shape cache; OLD bound in full so bind supplies `N+K` params (not `2N`)
  - `full_identity_update_all_toasted_is_noop` — all Toasted → skip, no error
  - `full_identity_update_ambiguous_match` — two identical rows → `FullIdentityAmbiguousMatch`
  - `full_identity_delete` — DELETE via `delete.old`; row removed
  - `full_identity_delete_ambiguous_match` — two identical rows → `FullIdentityAmbiguousMatch`
  - `full_identity_nothing_rejected` — `relation()` returns `ReplicaIdentityNothing`
  - `full_identity_omni_no_unique_index_rejected` — `relation()` returns `FullIdentityOmniNoUniqueIndex`

### Phase 6 — Integration tests

Depends on all previous phases. End-to-end reshard with a FULL identity table.

- Seed a table with no PK and `ALTER TABLE t REPLICA IDENTITY FULL`
- Trigger reshard; verify destination matches source exactly
- Cover INSERT, UPDATE (fast path), UPDATE (TOAST slow path), DELETE
- Cover `REPLICA IDENTITY NOTHING` → reshard rejected with clear error
- Cover omni table without unique index → rejected before replication starts

### Files touched

| File | Phase |
|------|-------|
| `logical/publisher/table.rs` | 1, 2 |
| `logical/error.rs` | 2 |
| `net/messages/replication/logical/update.rs` | 3 |
| `logical/publisher/queries.rs` | 4 |
| `logical/subscriber/stream.rs` | 5 |
| `logical/subscriber/tests.rs` | 5 |
| `integration/rust/tests/…` | 6 |
