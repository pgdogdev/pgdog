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

## Issue 1 — Failed `Prepare` orphans the EXECUTE ReadyForQuery

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
| 1 | `Error` for PREPARE | `pop_back` → `Code(RFQ)` matches; re-added | `[Code(RFQ)]` |
| 2 | `ReadyForQuery` for PREPARE | pops `Code(RFQ)` normally | **empty** |
| 3 | `Error` for EXECUTE (statement absent) | `pop_back` → None; nothing re-added | **empty** |
| 4 | `ReadyForQuery` for EXECUTE | `pop_front` on empty → **ProtocolOutOfSync** | — |

The Error handler at `state.rs:154–159` only re-adds a trailing `ReadyForQuery` when it finds
`Code(ReadyForQuery)` at the back. The two `Ignore` items representing the PREPARE sub-request are
invisible to it; once they are cleared the queue no longer knows the EXECUTE is still in-flight.

Under high concurrency this becomes near-deterministic: the pool fast-path (`Guard::drop` → `checkin`
→ `put`) hands a connection directly to a waiting client with no healthcheck, no idle time, and no
opportunity to drain the kernel socket buffer. The next query on that client consumes the stale EXECUTE
`Error + ReadyForQuery`, producing `ProtocolOutOfSync`.

### Reproduction

1. Connect to pgdog with session or transaction pooling.
2. Issue a simple-query `EXECUTE` for a statement that will fail to prepare (schema mismatch, syntax
   error, or stale local cache with a duplicate name).
3. Issue any subsequent query on the same connection.
4. The second query fails with `PG::ConnectionBad` / `protocol is out of sync`.

```sh
cd integration/prepared_statements_full && bash run.sh
```

### Tests

All three tests live in `integration/prepared_statements_full/protocol_out_of_sync_spec.rb`.

| Test | Pool mode | `got:` | `extended` | What it proves |
|---|---|---|---|---|
| 1 | session | Z | false | Orphaned RFQ hits empty queue on the very next query |
| 2 | transaction | C | false | Stale-chain: DML CommandComplete hits empty queue |
| 3 | session | Z | true | `extended = true` changes Error handler behavior (`out_of_sync`) |

- **Test 1 — Session mode, `got: Z`, `extended: false`.**
  Session-pooled user pinned to one backend. After triggering the failed prepare, `SELECT 1` consumes
  the stale EXECUTE error; the orphaned RFQ then hits an empty queue. Client sees `PG::ConnectionBad`.

- **Test 2 — Transaction mode stale-chain, `got: C`, `extended: false`.**
  `pgdog_tx_single` (transaction mode, pool_size=1). Each query consumes the previous query's stale
  response, creating a one-slot-shifted chain. A `BEGIN` that consumes a `'T'`-status RFQ keeps the
  connection open (`in_transaction = true`); the actual INSERT response then hits an empty queue
  (`got: C`). Client sees `PG::ConnectionBad`.

- **Test 3 — Session mode, `got: Z`, `extended: true`.**
  Same as Test 1, but a prior `exec_params` call (`SELECT $1::int`) permanently sets `extended = true`
  on the connection. The Error handler then sets `out_of_sync = true` before clearing the queue,
  changing connection-lifecycle behaviour. Client sees `PG::ConnectionBad`.

### Fix

Fix the Error handler in `state.rs:154–159`. When the failed message is part of a pgdog-injected
compound request, the handler must preserve the `Code(ReadyForQuery)` for the outer client-visible
request — not just the PREPARE's trailing slot. Concretely: the handler needs to recognise that
`Ignore` items at the back of the queue belong to a sub-request that is still in-flight, and must
keep the outer `Code(ReadyForQuery)` accordingly.

The TCP-peek approach (`FIONREAD` / `MSG_PEEK` at checkin) is a valid defensive catch-all but adds a
syscall on every checkin and does not fix the root cause.

---

## Issue 2 — Double `action()` call in `forward()` for server CopyDone

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

## Issue 3 — Stale ReadyForQuery hits an `Ignore(ParseComplete)` slot

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

## Issue 4 — `extended` flag is permanently set and never resets

**Severity:** Low-medium — affects connection-lifecycle semantics and silently changes Error handler
behaviour for all subsequent requests on a connection.

**Location:** `pgdog/src/backend/protocol/state.rs`, `add()` / `add_ignore()`; `state.rs:151–153`,
Error handler.

### Description

`ProtocolState.extended` is set to `true` the first time any parameterised query runs on a connection
and is never reset. The Error handler checks this flag to set `out_of_sync = true`; because the flag
is permanent, every error on that connection — including plain simple-query errors — sets
`out_of_sync = true` spuriously.

### Code path

`add()` and `add_ignore()` set the flag whenever `ParseComplete ('1')` or `BindComplete ('2')` is
enqueued:

```rust
self.extended = self.extended || code.extended();
```

The Error handler (`state.rs:151–153`):

```rust
ExecutionCode::Error => {
    if self.extended {
        self.out_of_sync = true;  // fires on every error, forever
    }
    // ...
}
```

There is no reset path.

### Consequences

- `done()` stays `false` one extra round-trip (until RFQ clears `out_of_sync`) on simple-query
  errors for connections that have ever served a parameterised query. Harmless in practice today, but
  more conservative than necessary.
- Future changes to the Error handler that add `extended`-specific behaviour will silently apply to
  all long-lived connections, not just those currently mid-pipeline.
- `extended` reads as "has this connection *ever* been in extended-protocol mode", not "is this
  connection *currently* in extended-protocol mode" — a semantic mismatch that will mislead future
  readers.

### Reproduction

1. Connect to pgdog.
2. Execute a parameterised query (any `$1` placeholder) — sets `extended = true`.
3. Execute `SELECT 1/0` (simple query).
4. Observe `server.out_of_sync() == true` immediately after the `'E'` response, before RFQ arrives.
   Expected: `false`.

```sh
cargo test -p pgdog test_extended_flag_never_resets
```

### Tests

**`test_extended_flag_never_resets_spurious_out_of_sync`** in `server.rs` (requires PostgreSQL) —
three phases on one connection:

1. *Baseline* — fresh connection (`extended = false`); `SELECT 1/0`; asserts `out_of_sync == false`.
2. *Trigger* — `Parse + Bind + Execute + Sync` for `SELECT $1::int`; permanently sets `extended = true`.
3. *Regression* — `SELECT 1/0` twice more; asserts `out_of_sync == true` after each `'E'`. Each
   `ReadyForQuery` resets `out_of_sync` to `false` but leaves `extended` unchanged.

```sh
cargo test -p pgdog test_extended_flag_never_resets
```

### Fix

Reset `extended` to `false` at the same point `out_of_sync` resets — when `ReadyForQuery` is
processed and the queue is fully drained:

```rust
ExecutionCode::ReadyForQuery => {
    self.out_of_sync = false;
    if self.is_empty() {
        self.extended = false;  // pipeline complete; reset for next request
    }
}
```

Resetting only when `is_empty()` is safe: pipelined requests still in the queue keep `extended = true`
until the entire pipeline finishes.

A post-fix test should verify: (a) phase 3 above now produces `out_of_sync == false`, and (b) an
intermediate `ReadyForQuery` inside a pipelined extended request does not prematurely reset `extended`.

---

## Common thread

All four issues share the same underlying fragility: the `ProtocolState` queue and the actual server
response stream diverge whenever an error or unexpected message interrupts a multi-message sub-request
injected transparently by pgdog. The Error handler was written for a single client-visible request and
does not account for the compound structures the prepared-statement rewriter produces.

Issue 4 is a secondary consequence: `extended` was added as a guard for the Error handler but was
attached to the connection rather than the current pipeline, so it outlives the requests it was meant
to describe.
