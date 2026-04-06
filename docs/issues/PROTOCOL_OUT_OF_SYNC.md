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

### Description

When `query_parser = "on"`, pgdog caches every `SET` issued by a client so it can replay them
on whichever backend connection services the next request. The replay calls `execute_batch`,
which queues one `Code(RFQ)` per command — two cached `SET`s produce `[Code(RFQ), Code(RFQ)]`.

Two independent defects caused `ProtocolOutOfSync` when a cached `SET` produced a server Error:

**Defect 1 — `extended` one-way latch.** `ProtocolState.extended` was set on the first
parameterised (`Parse`/`Bind`) query and never cleared. The Error arm unconditionally set
`out_of_sync = true` when `extended == true`, permanently poisoning any connection that had
ever used extended protocol — even for unrelated simple-query errors.

**Defect 2 — destructive queue clear.** The old Error arm (`pop_back + clear + push_back(RFQ)`)
was designed for a single-query queue. With two `Code(RFQ)` slots it collapsed to one:
the first `Z` consumed the only remaining slot, the second SET's `CommandComplete` hit empty
→ `ProtocolOutOfSync` before the client saw the actual SET error.

Both defects are independent. Defect 2 fires on any two-command batch regardless of `extended`;
Defect 1 additionally poisons the connection via `out_of_sync = true`.

### Conditions required

All four must hold simultaneously:

1. `query_parser = "on"` — pgdog tracks `SET` commands and replays them via `execute_batch`
   when syncing session state onto a checked-out connection.
2. Two or more `SET` commands in the cached session state, at least one with an invalid value
   (e.g. `SET lock_timeout TO sdf`) — produces two `Code(RFQ)` slots in the queue and causes
   PostgreSQL to return an Error on the first replayed `SET`.
3. A prior extended-protocol query (`Parse + Bind + Execute + Sync`) on the same connection
   — sets `extended = true`, arming the `out_of_sync = true` branch on any subsequent error
   (Defect 1).
4. Any subsequent query after the failed `execute_batch` — hits a connection whose
   `out_of_sync` is `true`, causing `PG::ConnectionBad` at the client.

### Reproduction

```ruby
# integration/ruby/pg_spec.rb
it 'out of sync' do
  conn = PG.connect(..., application_name: '')
  conn.exec_params 'SELECT $1', [1]          # sets extended = true
  conn.exec "SELECT 1"
  conn.exec "SET lock_timeout TO sdfs"       # invalid; query_parser will replay this
  conn.exec "SET statement_timeout TO '1s'"
  expect { conn.exec 'SELECT 1' }.to raise_error(/invalid value for parameter/)
end
```

```sh
cd integration/ruby && bundle exec rspec pg_spec.rb -e 'out of sync'
```

### Tests

**State-machine unit test (`state.rs`, no backend needed)**

- **`test_pipelined_simple_query_error_keeps_next_query_response`** — queues two `Code(RFQ)` slots,
  fires `action('E')`, asserts the second slot survives, walks `Z`/`C`/`Z` to completion.

**Server integration tests (`server.rs`, require PostgreSQL)**

- **`test_execute_batch_simple_query_error_then_success`** — `execute_batch(["syntax error", "SELECT 1"])`
  returns `ExecutionError` and leaves `server.done() == true`.
- **`test_out_of_sync_regression`** — exact production sequence: `Parse + Bind + Execute + Sync`,
  drain five responses, then `execute_batch` with invalid + valid `SET`; asserts `ExecutionError`
  and `server.done() == true`.

```sh
cargo test -p pgdog test_execute_batch_simple_query_error_then_success
cargo test -p pgdog test_out_of_sync_regression
```

### Fix

Two changes in `state.rs` (commit `54c9d3a9`):

**Defect 2:** A `!self.extended` fast-path in the Error arm returns `Action::Forward` immediately
when the queue front is `Code(RFQ)`, leaving all slots untouched. For the extended path,
a `rfq_pos` scan drains only items before the first `Code(RFQ)` boundary — never consuming
slots belonging to subsequent queries.

**Defect 1:** After every `ReadyForQuery`, `extended` is recomputed from remaining queue items
(`self.queue.iter().any(ExecutionItem::extended)`). When the pipeline drains, `extended` resets
to `false`, breaking the one-way latch. The `ExecutionItem::extended()` helper in `state.rs`
enables this scan.
---

## ✅ Issue 1 — Failed `Prepare` orphans the EXECUTE ReadyForQuery

**Severity:** High — triggered by normal server behaviour; no client misbehaviour required.

**Location:** `pgdog/src/backend/prepared_statements.rs`, `ProtocolMessage::Prepare` arm;
`pgdog/src/backend/protocol/state.rs`, Error handler.

### Description

When pgdog injects a `PREPARE` to rewrite a simple-query `EXECUTE` and that `PREPARE` fails on the
server, the Error handler incorrectly clears the queue. The subsequent `ReadyForQuery` from the now-
orphaned `EXECUTE` hits an empty queue and raises `ProtocolOutOfSync`.

### Code path

The simple-query rewriter turns `EXECUTE stmt_name(args)` into two prepended messages, each handled
independently by `handle()`. After both calls the queue is:

```
[Ignore(ExecutionCompleted), Ignore(ReadyForQuery), Code(ReadyForQuery)]
  ↑────────── handle(Prepare) ──────────↑           ↑── handle(Query) ──↑
```

If the injected `PREPARE` fails on the server:

| Step | Server sends | Error handler action | Queue after |
|---|---|---|---|
| 1 | `Error` for PREPARE | simple-query path (`!extended`): `pop_front` loop drains `Ignore(C)` and `Ignore(Z)`; stops at `Code(RFQ)` | `[Code(RFQ)]` |
| 2 | `ReadyForQuery` for PREPARE | `pop_front` → `Code(RFQ)` consumed | **empty** |
| 3 | `Error` for EXECUTE (statement absent) | simple-query path: queue empty; loop is a no-op | **empty** |
| 4 | `ReadyForQuery` for EXECUTE | `pop_front` on empty → **ProtocolOutOfSync** | — |

The simple-query Error handler (`state.rs:157–167`, the `!self.extended` arm) pops items from the
front of the queue until it reaches a `Code(ReadyForQuery)`. When the injected PREPARE fails this
drains both `Ignore` slots, leaving `Code(ReadyForQuery)` as the sole item. The subsequent `Z` for
the PREPARE consumes that slot, emptying the queue. The EXECUTE sub-request's `Error` then arrives
against an empty queue — the pop-front loop is a no-op — and its `ReadyForQuery` fires
`ProtocolOutOfSync`.

Under high concurrency this becomes near-deterministic: the pool fast-path (`Guard::drop` →
`checkin` → `put`) hands a connection directly to a waiting client with no healthcheck, no idle
time, and no opportunity to drain the kernel socket buffer. The next query on that client consumes
the stale EXECUTE `Error + ReadyForQuery`, producing `ProtocolOutOfSync`.

### Reproduction (historical)

1. Connect to pgdog with session or transaction pooling.
2. Issue a simple-query `EXECUTE` for a statement that will fail to prepare (schema mismatch, syntax
   error, or stale local cache with a duplicate name).
3. Issue any subsequent query on the same connection.
4. The second query fails with `PG::ConnectionBad` / `protocol is out of sync`.

```sh
cd integration/prepared_statements_full && bash run.sh
```

### Tests

Both tests live in `integration/prepared_statements_full/protocol_out_of_sync_spec.rb`.

| Test | Pool mode | What it guards against |
|---|---|---|
| 1 | session | Orphaned EXECUTE RFQ hits empty queue on the very next query → `PG::ConnectionBad` |
| 2 | transaction | Stale EXECUTE `Error` served to next borrower (pool_size=1) → `PG::InvalidSqlStatementName` |

- **Test 1 — Session mode.**
  Session-pooled user (`pgdog_session`) pinned to one backend. After the failed prepare-inject,
  `SELECT 1` is issued on the same connection. The orphaned EXECUTE `ReadyForQuery` hits an empty
  queue, raising `ProtocolOutOfSync`. Client sees `PG::ConnectionBad`.

- **Test 2 — Transaction mode (pool_size=1).**
  `pgdog_tx_single` (transaction mode, one backend). After the EXECUTE error the connection returns
  to the pool: the queue is empty and `out_of_sync` is false, so it appears clean. With only one
  backend the stale EXECUTE `Error + ReadyForQuery` remain in the TCP buffer. The next query
  (`SELECT 1`) reads the stale `Error` as its response → `PG::InvalidSqlStatementName`.

### Fix

Error handler in `state.rs`, `ExecutionCode::Error` arm. See inline comments for full detail.

On error, find the first `Code(ReadyForQuery)` in the queue (the client's RFQ boundary), drain
everything before it, count the `Ignore(RFQ)` slots in the drained portion, and prepend one
`[Ignore(RFQ), Ignore(Error)]` pair per slot. A separate fast-path at the top of the arm handles
the case where the queue front is already `Ignore(Error)` — subsequent errors from the same
injected sub-request — by popping and returning `Action::Ignore` directly.

See also: `test_injected_prepare_error_full_lifecycle` in `state.rs`.
---

## 🔴 Issue 2 — Double `action()` call in `forward()` for server CopyDone

**Severity:** Medium — requires the client to omit a trailing `Sync`.

**Location:** `pgdog/src/backend/prepared_statements.rs`, `forward()`, lines ~198 and ~237.

### Description

`forward()` calls `state.action(code)` unconditionally at line 198, then a second time inside the
`'c'` (CopyDone) match arm at line 237. When no `Code(ReadyForQuery)` backstop is present in the
queue, the second call hits an empty queue and raises `ProtocolOutOfSync`.

### Code path

Normal path (safe): `Code(ReadyForQuery)` is always in the queue. `action('Z')` pushes it back rather
than consuming it, making the double call idempotent.

Unsafe path — client sends `Parse + Bind + Execute + Flush` (no `Sync`). `handle()` builds:

```
[Code(ParseComplete), Code(BindComplete), Code(ExecutionCompleted)]
```

No `Code(ReadyForQuery)` is added. When the server responds with CopyDone:

```
First  action('c'): pops Code(ExecutionCompleted) — consumed
Second action('c'): empty queue → ProtocolOutOfSync
```

### Reproduction

Not triggerable via the `pg` gem or any libpq-based driver — libpq always appends `Sync` after
`Execute`. Requires sending raw protocol messages directly.

```sh
cargo test -p pgdog test_copy_out_done_double_action_out_of_sync_without_sync
```

### Tests

**State-machine unit tests (`state.rs`, no backend needed)**

- **`test_copydone_double_action_safe_with_rfq_backstop`** — queue `[Code(Copy), Code(ReadyForQuery)]`;
  two `action('c')` calls both succeed; RFQ slot is pushed back and survives.
- **`test_copydone_double_action_oos_without_rfq_backstop`** — queue `[Code(ExecutionCompleted)]`;
  second `action('c')` returns `Err(ProtocolOutOfSync)`.

**Server-level tests (`server.rs`, require PostgreSQL)**

- **`test_copydone_double_action_oos_without_sync`** — `Parse + Bind + Execute + Flush`
  (no Sync); reads ParseComplete, BindComplete, CopyOutResponse, CopyData ×2, then asserts
  `ProtocolOutOfSync` on CopyDone.
- **`test_copydone_double_action_safe_with_sync`** — same pipeline with `Sync`; full sequence
  completes without error; asserts `server.done()`.

```sh
cargo test -p pgdog test_copydone_double_action
```

### Fix

Remove the second `action()` call in the `'c'` arm of `forward()`, or guarantee that a
`Code(ReadyForQuery)` backstop is always in the queue before the CopyDone path is reached. Either
way, the invariant must be made explicit in code comments.

---

## 🔴 Issue 3 — Stale ReadyForQuery hits an `Ignore(ParseComplete)` slot

**Severity:** Low — practically unreachable in normal operation.

**Location:** `pgdog/src/backend/protocol/state.rs`, Ignore arm.

### Description

If a `ReadyForQuery` byte from a prior request cycle remains unread in the TCP receive buffer when
the next request starts, `action('Z')` fires while the queue front is `Ignore(ParseComplete)`. The
Ignore arm requires an exact code match; `ReadyForQuery != ParseComplete` → `ProtocolOutOfSync`.

### Code path

pgdog injects a Parse for a missing statement; queue front:

```
[Ignore(ParseComplete), Code(BindComplete), ...]
```

Stale `ReadyForQuery` arrives before `ParseComplete`:

```
action('Z'): generic pop → Ignore(ParseComplete)
  → ReadyForQuery != ParseComplete → ProtocolOutOfSync
```

### Reproduction

Not reproducible through normal pool operation. The `done()` guard chain prevents pool reclaim while
any `Ignore` item is present:

- `ProtocolState::done()` = `is_empty() && !out_of_sync` → `false` while any `Ignore` slot exists.
- `PreparedStatements::done()` adds a second gate blocking reclaim while an injected Parse is in flight.
- `Pool::maybe_check_in()` discards errored connections before `can_check_in()` is evaluated.

The precondition requires a concurrent-access bug that bypasses the pool guard, or direct TCP stream
injection.

### Tests

State-machine unit tests in `state.rs` cover the `action()` mismatch directly. A server-level
integration test is not practical; the precondition cannot be reached through normal sequential
protocol flow.

```sh
cargo test -p pgdog test_stale_rfq
```

### Fix

No code change required. The existing `done()` guard chain already prevents the precondition from
arising. If it were somehow reached, the resulting `ProtocolOutOfSync` would discard the connection
before reuse, bounding the blast radius to a single request.

---

## Issue 4 — `extended` flag set at enqueue time, not at processing time

**Severity:** Low in production — not triggerable through current code paths. Dangerous if triggered:
a write-query executed on the backend may never be confirmed to the client.

**Location:** `pgdog/src/backend/protocol/state.rs`, `add()` / `add_ignore()` and the
`ReadyForQuery` recalculation in `action()`.

### Description

Two related bugs in how `extended` is maintained:

**Bug A — `add()` sets the flag too early.**
`add()` and `add_ignore()` update `extended` the moment any `ParseComplete` or `BindComplete` is
enqueued, regardless of where in the queue that item sits:

```rust
self.extended = self.extended || code.extended();
```

If a simple query occupies the head of the queue and an extended query is enqueued behind it,
`extended` flips to `true` before any message has been processed. When the simple query then errors,
the Error handler sees `extended == true` and takes the extended path: sets `out_of_sync = true` and
clears the queue — destroying the extended query's pending entries.

**Bug B — `ReadyForQuery` recalculation scans the entire remaining queue.**
After consuming a `ReadyForQuery`, the flag is recalculated as:

```rust
self.extended = self.queue.iter().any(ExecutionItem::extended);
```

This scans all remaining entries, not just those belonging to the next sub-request. If two simple
queries precede an extended query, after the first simple query's `ReadyForQuery` is consumed the
scan finds the extended items two hops away and sets `extended = true`. The second simple query's
error then incorrectly takes the extended path.

### Why it does not affect production

`ClientRequest::spliced()` never places a simple query and an extended query in the same
`ProtocolState` queue. Each sub-request — whether simple or extended — gets its own fresh state.
The flag therefore always starts at `false` for a simple sub-request and becomes `true` only for
sub-requests that actually contain `Parse`/`Bind` messages.

### Why it is dangerous if triggered

If a simple query and an extended query share one queue and the simple query errors:
1. The extended query's pending entries are destroyed.
2. pgdog forwards the extended query's wire messages to the backend before reading any responses.
3. When the backend's response for the extended query arrives, pgdog hits `ProtocolOutOfSync` and
   discards the connection.
4. For a write query (`INSERT`/`UPDATE`/`DELETE`) the statement executed on the backend but the
   client receives no confirmation — silent data inconsistency.

### Reproduction

Not reproducible through normal pool operation. Reproduced at the `server.rs` level by bypassing
the splicing layer:

```sh
cargo test -p pgdog test_simple_query_error_before_queued_extended_request_does_not_set_out_of_sync
cargo test -p pgdog test_simple_query_error_after_rfq_before_extended_does_not_set_out_of_sync
cargo test -p pgdog test_simple_query_error_before_extended_query_in_same_batch
```

The first two unit tests fail at `assert!(!state.out_of_sync)`. The server test fails with
`ProtocolOutOfSync` when the extended query's `ParseComplete` arrives against an empty queue.

The integration test `extended query succeeds after preceding simple query error` in
`integration/ruby/protocol_out_of_sync_spec.rb` currently **passes** — pgdog's splicing prevents
the bug from reaching production — and serves as a regression guard.

### Tests

| Test | Level | Status | What it covers |
|---|---|---|---|
| `test_simple_query_error_before_queued_extended_request_does_not_set_out_of_sync` | unit (`state.rs`) | **fails** | Bug A: flag set by future enqueue poisons simple error path |
| `test_simple_query_error_after_rfq_before_extended_does_not_set_out_of_sync` | unit (`state.rs`) | **fails** | Bug B: full-queue scan sets flag for wrong sub-request |
| `test_simple_query_error_before_extended_query_in_same_batch` | server (`server.rs`) | **fails** | end-to-end: extended query lost after simple error in same batch |
| `extended query succeeds after preceding simple query error` | integration (Ruby) | passes | regression guard via normal pgdog path |

### Fix

**Bug A:** Remove the `extended` update from `add()` and `add_ignore()`. The flag must not be set
at enqueue time. Instead, set it in `action()` when a `ParseComplete` or `BindComplete` is
actually consumed from the queue front:

```rust
ExecutionItem::Code(in_queue_code) => {
    if in_queue_code.extended() {
        self.extended = true;
    }
    // ...
}
```

**Bug B:** The `ReadyForQuery` recalculation must scan only to the next `ReadyForQuery` boundary,
not the entire remaining queue:

```rust
self.extended = self.queue
    .iter()
    .take_while(|item| *item != &ExecutionItem::Code(ExecutionCode::ReadyForQuery))
    .any(ExecutionItem::extended);
```

This correctly reflects whether the **next sub-request** uses extended protocol, without being
contaminated by sub-requests further down the queue.

---

## Issue 5 — Error in first pipelined request clears subsequent requests' queue entries

**Severity:** Low — not reproducible through production code paths; included for completeness and as a guard against future refactoring.

**Location:** `pgdog/src/backend/protocol/state.rs`, Error handler (`action()`, extended branch).

### Description

When a client pipelines multiple extended-query sequences — each terminated by its own `Sync` — and the first sequence errors, the Error handler calls `queue.clear()` on the entire queue. This destroys the pending entries for all subsequent sequences, causing `ProtocolOutOfSync` when their backend responses arrive.

### Code path

Suppose three sequences share one `ProtocolState` queue:

```
[1,2,C,Z,  1,2,C,Z,  1,2,C,Z]
 ^─seq1─^   ^─seq2─^   ^─seq3─^
```

After seq1 consumes ParseComplete (`1`) and BindComplete (`2`), the queue is:

```
[C, Z, 1, 2, C, Z, 1, 2, C, Z]
```

Seq1 then errors:

| Step | Action | Queue after |
|---|---|---|
| Error arm fires | `pop_back()` → seq3's `Z`; `clear()` removes `[C,Z,1,2,C,Z,1,2,C]`; `push_back(Z)` | `[Z]` |
| seq1 ReadyForQuery arrives | pops `Z` normally | **empty** |
| seq2 ParseComplete arrives | `pop_front()` on empty queue | **ProtocolOutOfSync** |

### Why it does not affect production

`ClientRequest::spliced()` in `pgdog/src/frontend/client_request.rs` splits every multi-Execute pipeline into sub-requests at `Execute` boundaries, placing each `Sync` in its own standalone sub-request. Sub-requests are processed sequentially: each one is sent and fully drained before the next one is enqueued. The `ProtocolState` queue therefore only ever holds the entries for a single sync group at a time, so `queue.clear()` on error only ever sees that one group's entries.

This property is a load-bearing invariant. If `spliced()` is ever changed — for example to batch multiple sync groups into one send — this bug will surface immediately.

### Reproduction

Not reproducible through normal pool operation. Reproduced by loading all three sync groups into one `ProtocolState` directly:

```sh
cargo test -p pgdog test_pipeline_single_queue_error_only_clears_failing_sync_group
cargo test -p pgdog test_pipelined_multiple_syncs_first_fails
```

Both tests currently **fail** with `ProtocolOutOfSync`.

### Tests

| Test | Level | Status | What it covers |
|---|---|---|---|
| `test_pipeline_single_queue_error_clears_subsequent_sync_groups` | unit (`state.rs`) | passes | documents current (broken) behaviour: seq2 entries are gone |
| `test_pipeline_single_queue_error_only_clears_failing_sync_group` | unit (`state.rs`) | **fails** | specifies correct behaviour: seq2 and seq3 must survive |
| `test_pipelined_multiple_syncs_first_fails` | integration (`server.rs`) | **fails** | end-to-end reproduction against a real backend |

### Fix

The Error arm must clear only the failing sync group's entries — from the current queue head up to and including its own `ReadyForQuery` — leaving everything beyond that boundary intact:

```rust
// Remove entries up to and including this sync group's ReadyForQuery.
while let Some(item) = self.queue.pop_front() {
    if item == ExecutionItem::Code(ExecutionCode::ReadyForQuery) {
        break;
    }
}
```

Resetting only when `is_empty()` is safe: pipelined requests still in the queue keep `extended = true`
until the entire pipeline finishes.

A post-fix test should verify: (a) phase 3 above now produces `out_of_sync == false`, and (b) an
intermediate `ReadyForQuery` inside a pipelined extended request does not prematurely reset `extended`.
