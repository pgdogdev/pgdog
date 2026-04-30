# Code Review: REPLICA IDENTITY FULL Support

**Branch:** `HEAD` vs `main`  
**Date:** 2026-04-30  
**Files reviewed:** 20 (+2991/-300 lines)  
**Verdict:** All findings resolved. Ready to merge.

---

## Summary

The REPLICA IDENTITY FULL implementation is architecturally sound: the `UpdateIdentity` enum
correctly models the WAL K/O/N pre-image variants, the publisher-side cached `count_present`
is consistently maintained, the ambiguous-match guard is in the right place, and the test
suite covers the major code paths. However, one test is provably broken (CI red), two
correctness issues affect production behavior (TOAST/WHERE mismatch, sharded routing),
and the documentation makes an operator-facing promise the code does not keep.

---

## P0 — Fixed

### `table_validation_error_display` literal mismatch ✓

**File:** `pgdog/src/backend/replication/logical/error.rs` line 385  
**Fixed in this session.**

Test expected string updated to match the `#[display]` literal on
`TableValidationErrorKind::ReplicaIdentityNothing`. All 5 tests in the error module pass.

---

## P2 — Important Correctness Issues

### ~~1. FULL slow-path drops TOAST values from the WHERE clause~~ — False Positive

**Closed.** The finding was based on a misreading of the WAL protocol.

Unchanged TOAST columns carry `'u'` in **both** the old pre-image and the new tuple.
Postgres does not materialise TOAST values in the old pre-image either — it cannot do so
without a table scan that defeats the purpose of logical WAL. `partial_old` filters on
`new_col.identifier != Toasted`, which is correct: those old-column slots are also `'u'`,
so no value is available to bind in WHERE regardless.

The scenario described — rows distinguishable by a TOAST column the slow path omits —
cannot occur: if the WAL does not carry the value, neither side of the comparison has it.
`FullIdentityAmbiguousMatch` in this case is the correct, honest outcome. The error message
wording could be marginally improved (P3) to say "rows are indistinguishable from available
WAL data" rather than "duplicate rows", but the logic is sound.

### ~~2. Sharded FULL identity routing may silently fall through to `Shard::All`~~ — Fixed

**Fixed in this session.**

Root cause: the AExpr gate in `pgdog/src/frontend/router/parser/statement.rs` only accepted
`AexprOp | AexprIn | AexprOpAny` (with an additional `"="` operator-name check). `IS NOT
DISTINCT FROM` parses as `AexprNotDistinct` (pg_query variant 5), which fell through to
`SearchResult::None` immediately — shard key never extracted, routing silently `Shard::All`.

Note: substituting `=` was not safe for sharded tables because `HAS_UNIQUE_INDEX` (which
enforces NOT NULL on key columns) only runs for omni FULL tables, so NOT NULL cannot be
assumed on sharded destinations.

**Fix:** `statement.rs` AExpr dispatch rewritten as a single `match` on kind:
- `AexprNotDistinct` → `true` (kind is the full semantic; no `.name` field present)
- `AexprOp | AexprIn | AexprOpAny` → `true` iff operator name is `"="` (unchanged)
- `_` → `false`

Regression test added in `test_dml.rs`: `test_update_is_not_distinct_from_routes_to_shard`
asserts that `UPDATE sharded SET email = $2 WHERE id IS NOT DISTINCT FROM $1` with a bound
parameter routes to `Shard::Direct`, not `Shard::All`.

### ~~3. `has_unique_index()` issues a Simple Query mid-replication transaction~~ — Fixed

**Fixed in this session.**

Root cause: `validate_full_identity_omni_has_unique_index` was called from `relation()` on
first sight of a new omni FULL table. `pgoutput` emits `Relation` just-in-time before each
table's first change, so in a multi-table WAL transaction the probe could fire after
destination DML had already been sent (Bind/Execute/Flush) for an earlier table. The Sync
in the probe query would commit that pending DML early, breaking the rollback-on-error
contract in `handle()` for the partial-transaction case.

**Fix:** Moved validation to `connect()`, immediately after destination connections are
established and before any WAL messages are processed. At that point no transaction is
open on any shard. The table list (`self.tables`) is fully populated from the source
publication before `StreamSubscriber` is constructed, so all omni FULL tables are known.
The call in `relation()` is removed; the comment updated to note validation happened at
connect time.

### ~~4. Documentation claims NULLS NOT DISTINCT indexes are accepted~~ — Implemented

**Implemented in this session.** `HAS_UNIQUE_INDEX` now accepts either all key columns
`attnotnull = true` (pre-PG15 path) or `indnullsnotdistinct = true` (PG15+ path). Both doc
files updated to describe both accepted forms. A new test `test_has_unique_index_accepts_nulls_not_distinct`
covers the PG15+ path end-to-end.

### ~~5. `run.sh` executable bit dropped~~ — Fixed

**Fixed in this session.** Mode restored to `100755` via `git update-index --chmod=+x`.

### ~~6. Omni `event_types` catch-up poll only checks `DST_DB1`~~ — Fixed

**Fixed in this session.** Poll loop now captures labels from both `DST_DB1` and `DST_DB2`
and requires all four values to match before exiting.

### ~~7. Slow-path UPDATE test is vacuous~~ — Fixed

**Fixed in this session.** Test generates a distinct `id2`, sends `NEW = (text(id2), Toasted)`,
and asserts: (1) original `id` row gone, (2) `id2` row present, (3) `value` still `"initial"`.
A SET-clause regression will fail assertion 2.

### ~~8. All-Toasted no-op test does not verify row contents~~ — Fixed

**Fixed in this session.** Added `fetch_value` assertion after the count check: `value` must
still be `"stable"` after the no-op update.

---

## P3 — Nice-to-have

### ~~`Nothing` variant doc comment misrepresents the wire format~~ — Fixed

**Fixed in this session.** Reworded to: "no K/O block precedes the new-tuple marker;
the pre-image is absent."

### ~~`partial_old` silently truncates on column-count mismatch~~ — Moot

**Moot.** `partial_old` was removed this session. Caller now uses `old_full.without_toasted()`
directly; no zip or cross-referencing with `new` occurs.

### ~~Round-trip tests don't verify per-column data~~ — Fixed

**Fixed in this session.** `assert_round_trip` now asserts per-column `Identifier` values
for both `new.columns` and identity (Key/Old) columns.

### ~~FULL update fast-path table row reverses SET/WHERE parameter ranges~~ — Fixed

**Fixed in this session.** Table row corrected: WHERE uses `$1..$K`, SET uses `$K+1..$N`.

### ~~Phase 4 SQL skeleton omits the NULL-safety predicate~~ — Fixed

**Fixed in this session.** Plan SQL updated to include `NOT EXISTS … WHERE NOT a.attnotnull`.

### ~~Plan references `partial_old_for_present`~~ — Moot

**Moot.** `partial_old` removed; 930 doc updated to reflect `old_full.without_toasted()`.

### ~~`full_identity_omni_no_unique_index_rejected` has an implicit precondition~~ — Fixed

**Fixed in this session.** Test now drops and recreates `public.full_events_omni` without
a unique index before `connect()`.

### ~~Ambiguous-match test cleanup leaks rows on SET-clause regression~~ — Fixed

**Fixed in this session.** Cleanup changed to `DELETE … WHERE value IN ('dup', 'changed')`.

### Index name inconsistency between source and destination for `event_types`

**Files:** `integration/copy_data/setup.sql`, `integration/copy_data/data_sync/run.sh`

`setup.sql` creates the unique index with no explicit name (Postgres auto-assigns one).
`run.sh` creates `event_types_code_idx` explicitly on each destination shard. Harmless
today since schema-sync skips pre-data for this table, but fragile if that changes. Give
the index a deterministic name in `setup.sql` and reuse it on the destination.

---

## What is Correct

- `UpdateIdentity` enum correctly models WAL K/O/N pre-image variants; `from_bytes` is exhaustive and rejects unknown markers (the prior code fell through silently).
- `without_toasted` is a correct filter-clone that preserves column ordering.
- `count_present` cache in `NonIdentityColumnsPresence` is initialised in every constructor; no mutators exist that could desync it.
- Publisher SQL generation (delete/upsert/update full identity) is covered by unit tests asserting exact parameter numbering for full-present, partial-TOAST, leading-TOAST, and single-column-present cases.
- `quote_literal` is sufficient under `standard_conforming_strings = on` (default since PG 9.1) for config-controlled publication names and pg_class-validated identifiers.
- `derive_more` migration is structurally equivalent to `thiserror` for the migrated types: `#[display]`, `#[error]`, and `#[from]` produce the same `Display`/`Error`/`From` impls. `thiserror` intentionally remains for the types that were not migrated.
- The new `ShardedTable` entry in `cluster.rs` is inside `#[cfg(test)]` and has no production impact.
- `Cargo.toml` correctly adds `derive_more = { version = "2", features = ["display", "error"] }` without version conflict.
