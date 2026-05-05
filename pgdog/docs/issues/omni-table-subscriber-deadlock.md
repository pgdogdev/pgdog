# Omni-table cross-subscriber distributed deadlock

## Root cause

pgdog runs one logical replication subscriber per source shard. Each subscriber holds a persistent
connection to **every** destination shard inside one implicit destination transaction (open from WAL
`Begin` to WAL `Commit`). For omni (unsharded) tables, every source subscriber fans out each DML to
every destination, producing `N×M` independent lock holders for the same rows.

`send()` in `src/backend/replication/logical/subscriber/stream.rs` uses three sequential loops:

```
for each destination: send Bind + Execute + Flush
for each destination: flush socket
for each destination: read response
```

Two subscribers × two destinations interleave:

```
sub-0: write → dest-0 (granted)         write → dest-1 (waits on sub-1)
sub-1: write → dest-0 (waits on sub-0)  write → dest-1 (granted)
```

Responses are read dest-0-first, so sub-0 blocks reading dest-1 and sub-1 blocks reading dest-0.
Neither reaches `Sync`; neither releases its lock.

**PostgreSQL won't resolve it:** each server sees one waiter on one holder. The cycle exists only
across both wait graphs jointly, and no cross-server detector exists.

**pgdog won't resolve it:** `stream.handle(data).await` has no timeout. The client query path's
`query_timeout` (default 30s) calls `backend.force_close()` on expiry; the replication path has no
equivalent. The failure surfaces only as source-slot LSNs frozen at non-advancing values.

## Conditions

1. Omni (unsharded) table in the publication.
2. ≥2 source shards and ≥2 destination shards.
3. Two concurrent source transactions touching the same omni row(s).

The single-row case needs precise timing (sub-0 wins dest-0 and sub-1 wins dest-1 before either
reads back). The multi-row case is more reliable: row locks accumulate across the WAL transaction,
so subscribers hold earlier rows' locks while applying later ones.

## Diagnosing a stuck stream

```sh
# Cross-blocked backends on both destinations.
for port in 15434 15435; do
  PGPASSWORD=pgdog psql -h 127.0.0.1 -p $port -U pgdog -d postgres -c "
    SELECT pid, wait_event_type, wait_event, state, left(query, 80)
      FROM pg_stat_activity
     WHERE backend_type = 'client backend' AND state <> 'idle';"
done

# Frozen source slot positions.
for port in 15432 15433; do
  PGPASSWORD=pgdog psql -h 127.0.0.1 -p $port -U pgdog -d postgres -c "
    SELECT slot_name, confirmed_flush_lsn,
           pg_current_wal_lsn() - confirmed_flush_lsn AS lag
      FROM pg_replication_slots;"
done
```

Two waiters cross-blocked on the two destinations, plus two source-slot LSNs frozen across
successive `[0.000 MB/sec]` log lines, confirms this deadlock.

## Reproduction

- **Unit test:** `cross_subscriber_omni_deadlock_two_databases` in
  `src/backend/replication/logical/subscriber/tests.rs` — two subscribers racing on separate
  databases behind a `tokio::sync::Barrier`.
- **Integration:** `integration/resharding/repro_deadlock.sh` — full Docker stack, two seeded rows,
  concurrent UPDATEs.

## Solutions

### Solution 1: sequential per-destination apply in `send()` ✅ implemented

**What:** collapse the three loops in `send()` into a single loop that completes
the full write→read cycle for each destination before moving to the next.

Collapse the three loops in `send()` (`stream.rs:238-277`) into one that completes write→read per
destination before moving on:

```rust
for conn in &mut conns {
    conn.send(&vec![bind.clone().into(), Execute::new().into(), Flush.into()].into()).await?;
    for _ in 0..2 {
        let msg = conn.read().await?;
        // ... response handling ...
    }
}
```

**Effect:** no subscriber holds locks on a second destination before the first one's response is
read. Breaks the single-row two-destination cycle.

**Limit:** does not fix multi-row transactions. Locks accumulate across the WAL transaction (held
until `Sync` in `commit()`), so two omni rows still produce a cross-row, cross-destination cycle.
This is the case in `repro_deadlock.sh`. **Not sufficient alone** for any workload with multi-row
omni transactions.

**Parallelism cost:** sequential apply trades per-row latency for safety — destination
round-trips are now additive rather than overlapping. Parallel sends cannot be restored
without additional mechanisms: they break consistent lock ordering, allowing sub-0 to
acquire dest-0's lock while sub-1 acquires dest-1's lock before either reads back,
re-introducing the cycle. The structural fix is destination-partitioned apply (see
below), which assigns disjoint destination shards to each subscriber so parallel sends
within a subscriber are safe.
---

### Solution 2: `lock_timeout` on destination connections

In `connect()` (`stream.rs:165-222`), set `lock_timeout` on each destination connection alongside
the existing `BEGIN`/`COMMIT` `Parse`. Same pattern as `statement_timeout` in
`src/backend/pool/pool_impl.rs:423-428`.

```rust
server.send(&vec![
    Query::new("SET lock_timeout = '5s'").into(),
    Sync.into(),
].into()).await?;
// drain ReadyForQuery
```

Make it configurable: add `lock_timeout: Option<Duration>` to `Replication` in
`pgdog-config/src/replication.rs`.

**Effect:** Postgres aborts blocked statements with `canceling statement due to lock timeout`. The
error propagates `send()` → `handle_inner()` → `handle()`, which clears `connections` and lets the
subscriber reconnect from the last flushed LSN. The failure becomes visible in logs immediately.

**Limit:** safety net, not a structural fix. Bounds how long a deadlock persists; the conflicting
transaction still retries from scratch, so high contention can produce repeated lock-timeout
errors. Combined with Solution 1, it catches the multi-row cases sequential apply can't prevent.

---

### Combined recommendation

| | Single-row deadlock | Multi-row deadlock | Recovery / visibility |
|---|:---:|:---:|:---:|
| Sequential apply | ✓ | ✗ | ✗ |
| `lock_timeout` | — | — | ✓ |
| Both | ✓ | bounded | ✓ |

Ship `lock_timeout` first — lower-risk, immediate protection against every deadlock shape.
Sequential apply reduces single-row contention and is a prerequisite for any
destination-partitioned apply (below).

### Further: destination-partitioned apply (long-term)

Each destination shard is written by exactly one source subscriber for omni-table DML:

```
owns_omni_destination(source_shard, dest_shard, n_sources) = dest_shard % n_sources == source_shard
```

Removes cross-subscriber row-lock contention entirely. Requires every source subscriber to see
every omni-table WAL event. If omni writes can originate from a single source only, partitioning
silently leaves other destinations stale — routing must first establish source-presence before
partitioning is safe.

---

### Solution 3: per-table async writer task

One long-lived Tokio task per replicated table. Subscribers buffer the full WAL transaction in
memory; on `Commit`, each table's slice is sent over mpsc to that table's writer. The writer owns
all destination connections for its table, fans out to every destination shard in parallel within
one transaction, and applies transactions on the same table one at a time.

The mpsc guarantees at most one writer touches a table's rows on any destination at a given moment,
so the omni-table deadlock is **structurally eliminated**.

**Limits:**

1. **Cross-table atomicity is broken.** A source transaction touching an omni table and a sharded
   table flows through two writers; their commits land at different times. Reads between the two
   see a partially applied transaction — exactly the invariant logical replication is meant to
   preserve. Affects every workload mixing omni and sharded writes.

2. **Foreign keys become a new responsibility.** With FK from `B` to `A`:
   - If `B`'s writer drains before `A`'s commits → `foreign_key_violation`. Recovery requires
     global replay coordination.
   - Even without violation, FK checks take a share lock on the referenced row, so `B`'s writer
     can wait on `A`'s writer — a new cross-writer deadlock surface for any schema with FKs across
     replicated tables. Closing it requires a dependency tracker, which is the same coordination
     layer that collapses Solution 4 to globally serialized apply under contention.

3. **Cross-table commit order is not preserved.** Per-table writers are independent; arrival order
   at the destination need not match source-commit order. Cross-table joins during the apply
   window can return incorrect results.

4. **Buffering is unbounded.** Full WAL transactions sit in memory until `Commit`. Bulk loads can
   OOM before any row commits downstream.

5. **Throughput: N-way → 1-way per omni table.** N subscribers funnel into one writer per table.
   Even with parallel shard fan-out inside a single transaction, transactions on the same table
   apply one at a time. Every omni table receives writes from every source shard, so every
   concurrent source transaction queues — even when row sets don't overlap. PostgreSQL's row locks
   would have run those concurrently; the writer serializes them unconditionally. Ceiling per omni
   table: `1 / (max-shard transaction apply time)`, independent of source count. More source
   shards makes this worse.

6. **Connection count is K×M.** One writer per table × M destinations. If subscribers keep their
   own connections during migration, total is K×M + N×M.

7. **Back-pressure is global.** A slow destination on one hot table fills its channel, stalling the
   subscriber and WAL advance for every other table. Bounded channels stall uniformly; unbounded
   ones trade stalls for memory growth.

8. **Recovery needs a global LSN barrier.** Any writer error must roll back every writer's
   in-flight transaction and replay from the last globally-flushed LSN — a sync barrier that
   doesn't exist today.

Solves the deadlock by breaking logical replication's central correctness invariant.
**Not recommended.**

---

### Solution 4: per-destination-shard async writer task

One Tokio task per destination shard, owning that shard's single connection. Subscribers enqueue
buffered transactions onto the relevant shard's channel; the shard task applies in arrival order.

One writer per shard means no two subscribers contend for the same row on a destination. For omni
tables this is **strictly worse than the bug it fixes**, and for all tables it removes
destination-side parallelism.

**Limits:**

1. **Cross-shard order inversion → silent permanent corruption.** Two omni transactions on the
   same row, observed by different subscribers:

   - T1 (sub-0): `UPDATE r SET x = 1`
   - T2 (sub-1): `UPDATE r SET x = 2`

   Each subscriber enqueues its buffered transaction onto every shard's channel via separate
   `tx.send()` calls — one per destination, **not atomic across destinations**. The four sends
   from the two subscribers interleave freely:

   ```
   t0: sub-0.send(chan-0, T1)   chan-0: [T1]
   t1: sub-1.send(chan-0, T2)   chan-0: [T1, T2]
   t2: sub-1.send(chan-1, T2)   chan-1: [T2]
   t3: sub-0.send(chan-1, T1)   chan-1: [T2, T1]
   ```

   Each shard task drains FIFO: shard-0 applies T1 → T2 and ends at `x = 2`; shard-1 applies
   T2 → T1 and ends at `x = 1`. The shards permanently disagree on row `r`. Postgres raises no
   error — each shard's local apply order was internally consistent — and pgdog has no
   cross-shard comparator. Reads of `r` return inconsistent values indefinitely.

   The current code cannot produce this: each subscriber writes serially through one connection
   per shard from a single task, so its writes to dest-0 and dest-1 always land in the same
   relative order on both. The lock contention is what produces the (observable, recoverable)
   deadlock instead of (silent) divergence.

   A global sequence number per source transaction would let shard tasks reorder before applying,
   but then shard-1 must wait for T1 to arrive before applying T2 — both shards apply in the same
   global order, eliminating per-shard parallelism for omni writes. Strictly worse than today.

2. **Cross-shard atomicity is lost.** Multi-shard transactions commit independently per shard with
   no coordinated rollback.

3. **Memory is unbounded** — same buffering requirement as Solution 3.

4. **Eliminates all destination parallelism for all tables.** Today N subscribers drive concurrent
   connections to each shard; unrelated transactions on different tables apply in parallel.
   Solution 4 serializes everything per shard FIFO — an `orders` row from sub-1 waits behind a
   `config` row from sub-0 even with no shared locks. One slow apply stalls everything behind it.

   Per-shard ceiling: `1 / (single-row apply RTT)`. Today: `N / (single-row apply RTT)` across N
   subscribers. The regression is total, not scoped to omni tables.

**Discard.** Silent corruption *and* total throughput regression. Solutions 1+2 give a bounded,
recoverable deadlock — strictly better on both axes.

---

### Solution 5: transaction batching with deferred `Sync`

Sometimes proposed alongside the deadlock fix; **not a deadlock fix**. Buffer the full WAL
transaction, then send all statements per destination in one batch terminated by `Sync`. `send()`
stops emitting per-row `Flush`; responses are read once at `commit()`.

**Why the deadlock survives.** Locks are acquired when Postgres processes `Execute`, as soon as
bytes hit the network — independent of whether the payload ends with `Flush` or `Sync`. By the
time the last row is flushed, all locks are held, and they release only at COMMIT. The
cross-subscriber interleave is equally possible. `lock_timeout` is still required.

**The current code does not have a partial-transaction-visibility bug.** `__pgdog_repl_begin` /
`__pgdog_repl_commit` are only `Parse`-prepared in `connect()` (`stream.rs:185-204`); never bound
or executed. Each destination runs an implicit extended-protocol transaction spanning the WAL
transaction, committed by the `Sync` in `commit()` (`stream.rs:671-674`). `send()` emits only
`Bind + Execute + Flush`; `prepare_statements` (`stream.rs:443-446`) emits `Sync` only when
`in_transaction == false`. On error, `handle()` (`stream.rs:826`) clears the pool, dropping
sockets and forcing a Postgres rollback. Solution 5 adds no correctness here.

**Unrelated benefits:**

- Removes per-row protocol round-trips (one response read per transaction instead of per row).
- Simpler `send()` control flow.

**Costs:**

- Unbounded memory: full WAL transaction per destination buffered before flush.
- Apply lag increases by one source-transaction duration.

Treat as a `send()`-loop optimization parallel to Solutions 1+2, not part of the deadlock fix.

---

### Comparison

| | Deadlock fixed | Cross-table/shard atomicity | Throughput impact | Memory risk | Complexity | Recommendation |
|---|:---:|:---:|:---:|:---:|:---:|---|
| Solution 1: sequential per-dest apply | single-row only | preserved | minor (serial dest RTTs per row) | none | low | Ship |
| Solution 2: `lock_timeout` | bounded recovery | preserved | none | none | low | Ship first |
| **Solutions 1 + 2 combined** | **yes (bounded for multi-row)** | **preserved** | **minor** | **none** | **low** | **Recommended** |
| Solution 3: per-table writer | yes | broken | severe: N-way → 1-way per omni table | unbounded | high | Not recommended |
| Solution 4: per-shard writer | silent divergence† | broken | severe: removes all destination parallelism | unbounded | high | **Discard** |
| Solution 5: buffer + `Sync` | no | preserved | minor (apply lag) | unbounded | low | Optional optimization |
| Destination-partitioned apply | yes | preserved | none | none | high | Long-term structural fix |

†Solution 4 produces persistent, undetectable row-level disagreement across shards when two
subscribers race on the same omni row. The deadlock is observable and recoverable; this divergence
is neither.

Ship 1+2 short-term; pursue destination-partitioned apply long-term. Solutions 3 and 4 trade the
deadlock for worse failure modes and throughput. Solution 5 is orthogonal.
