# ProtocolOutOfSync — Known Root Causes

`Error::ProtocolOutOfSync` fires when `ProtocolState`'s expected-response queue diverges from what
the backend actually sends. The catch site is `server.rs:394`; the connection is permanently marked
`State::Error` and discarded from the pool.

**Queue mechanics** (`pgdog/src/backend/protocol/state.rs`). `handle()` pushes one `ExecutionItem`
per anticipated response before forwarding any client message. As server bytes arrive, `forward()`
calls `action(code)` which pops the queue front and checks the match. Two conditions raise
`ProtocolOutOfSync`:

- **Empty queue** (`state.rs:168`) — a tracked message arrives but nothing was expected.
- **Ignore mismatch** (`state.rs:188–191`) — queue front is an `Ignore` slot but the server sent a
  different code.

---

## ✅ Issue 0 — `extended` flag never resets; `SET`-batch error poisons connection

**Severity:** High — triggered by normal operation when `query_parser = "on"` and any parameterised
query has run on the connection.

**Commit:** `54c9d3a942bdac55873f17d2af74ef1dfedb3f4a`

**Location:** `pgdog/src/backend/protocol/state.rs`, `action()`, Error and ReadyForQuery arms.

### Error

When `query_parser = "on"`, pgdog caches every `SET` issued by a client so it can replay them on
whichever backend connection services the next request. The replay calls `execute_batch`, which
queues one `Code(RFQ)` per command — two cached `SET`s produce `[Code(RFQ), Code(RFQ)]`.

Two independent defects caused `ProtocolOutOfSync` when a cached `SET` produced a server Error:

**Defect 1 — `extended` one-way latch.** `ProtocolState.extended` was set the moment the first
`Parse`/`Bind` message passed through the connection, and never cleared. The Error arm
unconditionally set `out_of_sync = true` whenever `extended == true`. This permanently poisoned any
connection that had ever run an extended-protocol query — even for entirely unrelated subsequent
errors, such as a failed `SET` replay that has nothing to do with the original parameterised query.

**Defect 2 — destructive queue clear on batch error.** The Error arm used `pop_back + clear +
push_back(RFQ)`, designed for a single-command queue. With two `Code(RFQ)` slots that sequence
collapses both into one: the first `ReadyForQuery` from the backend consumed the only remaining RFQ
slot; the second `SET`'s `CommandComplete` arrived against an empty queue → `ProtocolOutOfSync`.
The client never saw the actual SET error.

Both defects are independent. Defect 2 fires on any two-command batch regardless of `extended`;
Defect 1 additionally poisons the connection via `out_of_sync = true`.

All four conditions must hold simultaneously for the bug to surface:

1. `query_parser = "on"` — pgdog tracks and replays `SET` commands via `execute_batch`.
2. Two or more `SET` commands in the cached session state, at least one with an invalid value —
   produces two `Code(RFQ)` slots and causes PostgreSQL to return an Error on the first replayed `SET`.
3. A prior extended-protocol query on the same connection — sets `extended = true`, arming the
   `out_of_sync = true` branch on any subsequent error (Defect 1).
4. Any subsequent query after the failed `execute_batch` — hits a connection whose `out_of_sync` is
   `true`, causing `PG::ConnectionBad` at the client.

### Fix

Two changes in `state.rs` (commit `54c9d3a9`):

**Defect 2:** A `!self.extended` fast-path in the Error arm returns `Action::Forward` immediately
when the queue front is `Code(RFQ)`, leaving all remaining slots untouched. For the extended path,
a `rfq_pos` scan drains only items before the first `Code(RFQ)` boundary — never consuming slots
belonging to subsequent queued commands.

**Defect 1:** After every `ReadyForQuery`, `extended` is recomputed from the remaining queue items
(`self.queue.iter().any(ExecutionItem::extended)`). When the pipeline drains, `extended` resets to
`false`, breaking the one-way latch. The `ExecutionItem::extended()` helper in `state.rs` enables
this scan.

### Tests

Covered by a state-machine unit test (`test_pipelined_simple_query_error_keeps_next_query_response`
in `state.rs`) and two server integration tests in `server.rs` that require a live PostgreSQL
connection. An end-to-end reproduction lives in `integration/ruby/pg_spec.rb`.

---

## ✅ Issue 1 — Failed `PREPARE` orphans the EXECUTE ReadyForQuery

**Severity:** High — triggered by normal server behaviour; no client misbehaviour required.

**Location:** `pgdog/src/backend/prepared_statements.rs`, `ProtocolMessage::Prepare` arm;
`pgdog/src/backend/protocol/state.rs`, Error handler.

### Error

When pgdog rewrites a simple-query `EXECUTE stmt(args)` it injects a synthetic `PREPARE` ahead of
it, producing two messages that are enqueued independently. After both `handle()` calls the queue
is:

```
[Ignore(ExecutionCompleted), Ignore(ReadyForQuery), Code(ReadyForQuery)]
  ↑────────── handle(Prepare) ──────────↑           ↑── handle(Query) ──↑
```

If the injected `PREPARE` fails on the server:

| Step | Server sends | Error handler action | Queue after |
|---|---|---|---|
| 1 | `Error` for PREPARE | simple-query path: drain `Ignore(C)` and `Ignore(Z)`; stop at `Code(RFQ)` | `[Code(RFQ)]` |
| 2 | `ReadyForQuery` for PREPARE | `pop_front` → `Code(RFQ)` consumed | **empty** |
| 3 | `Error` for EXECUTE (statement absent) | simple-query path: queue empty; loop is a no-op | **empty** |
| 4 | `ReadyForQuery` for EXECUTE | `pop_front` on empty → **ProtocolOutOfSync** | — |

The simple-query Error handler pops items from the front of the queue until it reaches a
`Code(ReadyForQuery)`, which it treats as the boundary of the current logical request. When the
injected PREPARE fails, that drain consumes both `Ignore` slots and leaves `Code(ReadyForQuery)` as
the sole item. The subsequent `Z` for the PREPARE then consumes *that* slot, emptying the queue
entirely. The EXECUTE sub-request's own `Error` arrives against an empty queue — the loop is a
no-op — and its `ReadyForQuery` fires `ProtocolOutOfSync`.

Under high concurrency this becomes near-deterministic: the pool fast-path (`Guard::drop` →
`checkin` → `put`) hands a connection directly to a waiting client with no healthcheck and no
opportunity to drain the kernel socket buffer. The next query on that client consumes the stale
EXECUTE `Error + ReadyForQuery`, producing `ProtocolOutOfSync`.

### Fix

Error handler in `state.rs`, `ExecutionCode::Error` arm (see inline comments for full detail).

On error, find the first `Code(ReadyForQuery)` in the queue (the client's RFQ boundary), drain
everything before it, count the `Ignore(RFQ)` slots in the drained portion, and prepend one
`[Ignore(RFQ), Ignore(Error)]` pair per slot. This reconstructs the correct response skeleton for
every injected sub-request that was ahead of the failing one, so each pending `ReadyForQuery` from
the backend still has a matching slot to consume.

A separate fast-path at the top of the arm handles the case where the queue front is already
`Ignore(Error)` — subsequent errors from the same injected sub-request — by popping and returning
`Action::Ignore` directly, rather than running the full drain logic again.

### Tests

Two integration tests in `integration/prepared_statements_full/protocol_out_of_sync_spec.rb` cover
the session-pooled and transaction-pooled cases. A unit test (`test_injected_prepare_error_full_lifecycle`
in `state.rs`) guards the state-machine invariant directly.

---

## ✅ Issue 2 — Double `action()` call in `forward()` for server CopyDone

**Severity:** Medium — requires the client to omit a trailing `Sync`.

**Location:** `pgdog/src/backend/prepared_statements.rs`, `forward()`.

### Error

`forward()` called `state.action(code)` unconditionally near the top of the function to advance the
state machine, then called it a second time inside the `'c'` (CopyDone) match arm. For the common
case this was harmless: a `Code(ReadyForQuery)` is always in the queue, and `action('Z')` pushes
that slot back rather than consuming it, making the double call idempotent.

The unsafe path triggers when a client sends `Parse + Bind + Execute + Flush` without a trailing
`Sync`. In that case `handle()` builds `[Code(ParseComplete), Code(BindComplete),
Code(ExecutionCompleted)]` with no `Code(ReadyForQuery)` backstop. When the server responds with
CopyDone:

```
First  action('c'): pops Code(ExecutionCompleted) — consumed
Second action('c'): empty queue → ProtocolOutOfSync
```

No libpq-based driver ever sends `Execute` without `Sync`, so this path was not reachable through
normal client behaviour.

### Fix

Removed the redundant `self.state.action(code)?` from the `'c'` arm in `forward()`. The call at
the top of the function already advances the state machine for CopyDone; the arm body is now empty.

### Tests

Two state-machine unit tests in `state.rs` pin the safe (RFQ backstop present) and unsafe (no
backstop) paths. Two server-level tests in `server.rs` cover the `Parse + Bind + Execute + Flush`
sequence with and without a trailing `Sync`.

---

## ✅ Issue 4 — `extended` flag set at enqueue time, not at processing time

**Severity:** Low in production — not triggerable through current code paths. Dangerous if triggered:
a write query executed on the backend may never be confirmed to the client.

**Location:** `pgdog/src/backend/protocol/state.rs`, `add()` / `add_ignore()` and the
`ReadyForQuery` recalculation in `action()`.

### Error

Two related bugs in how `extended` was maintained:

**Bug A — `add()` sets the flag too early.** `add()` and `add_ignore()` updated `extended` the
moment any `ParseComplete` or `BindComplete` was enqueued:

```rust
self.extended = self.extended || code.extended();
```

If a simple query occupied the head of the queue and an extended query was enqueued behind it,
`extended` flipped to `true` before any message had been processed. When the simple query at the
head then errored, the Error handler saw `extended == true` and took the extended path — setting
`out_of_sync = true` and clearing the queue — destroying the extended query's pending entries even
though those entries had not yet been touched.

**Bug B — `ReadyForQuery` recalculation scans the entire remaining queue.** After consuming a
`ReadyForQuery`, the flag was recomputed as:

```rust
self.extended = self.queue.iter().any(ExecutionItem::extended);
```

This scanned all remaining entries, not just those belonging to the next sub-request. If two simple
queries preceded an extended query, after the first simple query's `ReadyForQuery` was consumed the
scan found the extended items two hops away and set `extended = true`. The second simple query's
error then incorrectly took the extended path.

Neither bug surfaces in production because `ClientRequest::spliced()` never places a simple query
and an extended query in the same `ProtocolState` queue — each sub-request gets its own fresh state.
However, if a write query were somehow affected, the backend would execute the statement while pgdog
hits `ProtocolOutOfSync` and discards the connection, leaving the client with no confirmation —
silent data inconsistency.

### Fix

The `extended` field was removed entirely from `ProtocolState`. Both bugs stemmed from maintaining
it as a cached flag that drifted out of phase with the queue head. Removing it eliminates both
defects at the root.

`out_of_sync` is now set unconditionally for all errors (simple or extended) and cleared
unconditionally on every `ReadyForQuery`. The distinction is irrelevant: after any error the
connection must wait for the peer's `ReadyForQuery` before being reused, and that `ReadyForQuery`
always arrives as the next terminal message in the same sub-request:

```
Error         → out_of_sync = true
ReadyForQuery → out_of_sync = false   (connection clean; entries beyond this boundary intact)
```

### Tests

Three unit tests in `state.rs` cover Bug A, Bug B, and the combined end-to-end case at the `server.rs`
level. The integration test `extended query succeeds after preceding simple query error` in
`integration/ruby/protocol_out_of_sync_spec.rb` serves as a regression guard via the normal pool path.

---

## ✅ Issue 5 — Error in first pipelined request clears subsequent requests' queue entries

**Severity:** Low — not reproducible through production code paths; included as a guard against
future refactoring.

**Location:** `pgdog/src/backend/protocol/state.rs`, Error handler (`action()`, extended branch).

### Error

When a client pipelines multiple extended-query sequences — each terminated by its own `Sync` — and
all of them share one `ProtocolState` queue, an error in the first sequence causes `queue.clear()`
on the entire queue. This destroys the pending entries for all subsequent sequences, causing
`ProtocolOutOfSync` when their backend responses arrive.

Suppose three sequences share one queue, abbreviated as entries `[1,2,C,Z]` per sequence (Parse,
Bind, CommandComplete, ReadyForQuery):

```
[1,2,C,Z,  1,2,C,Z,  1,2,C,Z]
 ^─seq1─^   ^─seq2─^   ^─seq3─^
```

After seq1 consumes ParseComplete (`1`) and BindComplete (`2`), the queue is `[C,Z,1,2,C,Z,1,2,C,Z]`.
Seq1 then errors:

| Step | Action | Queue after |
|---|---|---|
| Error arm fires | `pop_back()` → seq3's `Z`; `clear()` removes `[C,Z,1,2,C,Z,1,2,C]`; `push_back(Z)` | `[Z]` |
| seq1 ReadyForQuery arrives | pops `Z` normally | **empty** |
| seq2 ParseComplete arrives | `pop_front()` on empty queue | **ProtocolOutOfSync** |

This does not affect production because `ClientRequest::spliced()` splits every multi-Execute
pipeline into sub-requests at `Execute` boundaries, placing each `Sync` in its own standalone
sub-request that is sent and fully drained before the next one is enqueued. The `ProtocolState`
queue therefore only ever holds the entries for a single sync group at a time.

This is a load-bearing invariant. If `spliced()` is ever changed to batch multiple sync groups into
one send, this bug will surface immediately.

### Fix

The Error arm drains only the failing sync group's entries — from the current queue head up to and
including its own `ReadyForQuery` boundary — using `drain(..rfq_pos)`, leaving everything beyond
intact. A fallback `queue.clear()` applies only when no `Code(ReadyForQuery)` exists in the queue
at all (nothing beyond the current group to preserve).

The old `pop_back + clear + push_back(RFQ)` pattern was removed.

### Tests

Two unit tests in `state.rs` and one server-level test in `server.rs` cover the scenario where
seq1 errors and seq2/seq3 must complete normally.
