# ProtocolOutOfSync — Known Root Causes

`Error::ProtocolOutOfSync` fires when `ProtocolState`'s expected-response queue diverges from what
the backend actually sends. The catch site is `server.rs:394`; the connection is permanently marked
`State::Error` and discarded from the pool.

**Queue mechanics** (`pgdog/src/backend/protocol/state.rs`). `handle()` pushes one `ExecutionItem`
per anticipated response before forwarding any client message. As server bytes arrive, `forward()`
calls `action(code)` which pops the queue front and checks the match. Two conditions raise
`ProtocolOutOfSync`:

- **Empty queue** — a tracked message arrives but nothing was expected.
- **Ignore mismatch** — queue front is an `Ignore` slot but the server sent a different code.

---

## ✅ Issue 1 — Failed `Prepare` orphans the EXECUTE ReadyForQuery

**Severity:** High — triggered by normal server behaviour; no client misbehaviour required.

**Location:** `pgdog/src/backend/prepared_statements.rs`, `ProtocolMessage::Prepare` arm;
`pgdog/src/backend/protocol/state.rs`, Error handler.

### Description

When pgdog injects a `PREPARE` to rewrite a simple-query `EXECUTE` and that `PREPARE` fails on the
server, the old Error handler incorrectly cleared the queue. The subsequent `ReadyForQuery` from the
now-orphaned `EXECUTE` hit an empty queue and raised `ProtocolOutOfSync`.

### Code path

The simple-query rewriter turns `EXECUTE stmt_name(args)` into two prepended messages, each handled
independently by `handle()`. After both calls the queue is:

```
[Ignore(CommandComplete), Ignore(ReadyForQuery), Code(ReadyForQuery)]
  ↑──────────── handle(Prepare) ─────────────↑  ↑─── handle(Query) ───↑
```

The old handler popped the last item, cleared the queue, and optionally re-added a trailing
`Code(ReadyForQuery)`. That assumed a flat, single-request queue. With the injected sub-request the
queue is compound, so clearing it discarded the client's own `Code(ReadyForQuery)`:

| Step | Server sends | Old handler action | Queue after |
|---|---|---|---|
| 1 | `Error` for PREPARE | `pop_back` → `Code(RFQ)` re-added | `[Code(RFQ)]` |
| 2 | `ReadyForQuery` for PREPARE | pops `Code(RFQ)` normally | **empty** |
| 3 | `Error` for EXECUTE (statement absent) | `pop_back` → None; nothing re-added | **empty** |
| 4 | `ReadyForQuery` for EXECUTE | `pop_front` on empty → **ProtocolOutOfSync** | — |

Under high concurrency this became near-deterministic: the pool fast-path (`Guard::drop` → `checkin`
→ `put`) hands a connection directly to a waiting client with no healthcheck and no opportunity to
drain the kernel socket buffer. The next query consumed the stale EXECUTE `Error + ReadyForQuery`,
producing `ProtocolOutOfSync`.

### Reproduction (historical)

```sh
cd integration/prepared_statements_full && bash run.sh
```

### Tests

**State-machine unit test (`state.rs`, no backend needed)**

- **`test_injected_prepare_error_full_lifecycle`** — builds the exact queue that
  `prepared_statements.rs` produces (`add_ignore('C')`, `add_ignore('Z')`, `add('Z')`), fires
  `action('E')` and asserts the intermediate queue shape `[Ignore(RFQ), Ignore(Error), Code(Z)]`,
  then walks the remaining Z→Ignore, E→Ignore (fast-path), Z→Forward sequence to completion.

**Server-level integration test (`server.rs`, requires PostgreSQL)**

The test that previously asserted `ProtocolOutOfSync` on the fourth message now asserts `E` then
`Z` (both forwarded). All three configurations now pass.

| Test | Pool mode | `got:` | `extended` | What it proves |
|---|---|---|---|---|
| 1 | session | Z | false | Failed PREPARE no longer orphans the EXECUTE RFQ — **fixed** |
| 2 | transaction | — | false | Stale-chain: injected E+Z drained internally; pool socket is clean — **fixed** |
| 3 | session | Z | true | `extended` now resets after RFQ drain — **fixed by Issue 4** |

- **Test 1** (`pgdog_session`): session-pooled connection; failed PREPARE then EXECUTE; subsequent `SELECT 1` must succeed.
- **Test 2** (`pgdog_tx_single`, `pool_size=1`): same failure sequence in transaction mode. Issue 1 drains the orphaned EXECUTE E+Z internally before the connection returns to the pool, so no stale bytes shift subsequent queries.
- **Test 3** (`pgdog_session` with prior `exec_params`): `extended=true` before the failure; Issue 4 ensures the flag resets after the RFQ drains the queue.

See `integration/prepared_statements_full/protocol_out_of_sync_spec.rb` for full test bodies.

### Fix

Error handler in `state.rs`, `ExecutionCode::Error` arm. On error, find the first
`Code(ReadyForQuery)` in the queue (the client's RFQ boundary), drain everything before it, count
the `Ignore(RFQ)` slots in the drained portion, and prepend one `[Ignore(RFQ), Ignore(Error)]` pair
per slot. A fast-path at the top of the arm handles subsequent errors from the same injected
sub-request — when the queue front is already `Ignore(Error)` — by popping and returning
`Action::Ignore` directly.

See also: `test_injected_prepare_error_full_lifecycle` in `state.rs`.

---

## ✅ Issue 2 — Double `action()` call in `forward()` for server CopyDone

**Severity:** Medium — requires the client to omit a trailing `Sync`.

**Location:** `pgdog/src/backend/prepared_statements.rs`, `forward()`.

### Description

`forward()` called `state.action(code)` unconditionally near the top of the function, then called
it a second time inside the `'c'` (CopyDone) match arm. Without a `Code(ReadyForQuery)` backstop in
the queue the second call hit an empty queue and raised `ProtocolOutOfSync`.

### Code path

Normal path (safe): `Code(ReadyForQuery)` is always in the queue. `action('Z')` pushes it back
rather than consuming it, making the double call idempotent.

Unsafe path — client sends `Parse + Bind + Execute + Flush` (no `Sync`). `handle()` builds:

```
[Code(ParseComplete), Code(BindComplete), Code(ExecutionCompleted)]
```

No `Code(ReadyForQuery)` is added. When the server responded with CopyDone:

```
First  action('c'): pops Code(ExecutionCompleted) — consumed
Second action('c'): empty queue → ProtocolOutOfSync
```

### Reproduction (historical)

Not triggerable via the `pg` gem or any libpq-based driver — libpq always appends `Sync` after
`Execute`. Required sending raw protocol messages directly.

```sh
cargo test -p pgdog --lib -- test_copydone_double_action_oos_without_rfq_backstop
```

### Tests

**State-machine unit tests (`state.rs`, no backend needed)**

- **`test_copydone_double_action_safe_with_rfq_backstop`** — queue `[Code(Copy), Code(ReadyForQuery)]`;
  two `action('c')` calls both succeed; RFQ slot is pushed back and survives.
- **`test_copydone_double_action_oos_without_rfq_backstop`** — documents the raw state-machine
  invariant: calling `action('c')` twice with no RFQ backstop still causes `ProtocolOutOfSync`
  directly on the state machine. `forward()` no longer makes this second call; this path is
  unreachable through normal protocol flow. Test is retained to pin the underlying invariant.

**Server-level tests (`server.rs`, require PostgreSQL)**

- **`test_copydone_single_action_without_sync`** — `Parse + Bind + Execute + Flush` (no Sync);
  reads ParseComplete, BindComplete, CopyOutResponse, CopyData ×2, then asserts CopyDone is
  forwarded successfully. The trailing CommandComplete then hits an empty queue (no RFQ backstop)
  and raises `ProtocolOutOfSync` — that is the correct remaining behavior with no `Sync`.
- **`test_copydone_double_action_safe_with_sync`** — same pipeline with `Sync`; full sequence
  completes without error; asserts `server.done()`.

```sh
cargo test -p pgdog --lib -- test_copydone_double_action
cargo test -p pgdog -- test_copydone
```

### Fix

Removed the redundant `self.state.action(code)?` from the `'c'` arm in `forward()`. The call at
the top of the function already advances the state machine for CopyDone; the arm body is now empty.

---

## ✅ Issue 3 — Stale ReadyForQuery hits an `Ignore(ParseComplete)` slot

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
cargo test -p pgdog --lib -- test_stale_rfq
```

### Fix

No code change required. The existing `done()` guard chain already prevents the precondition from
arising. If it were somehow reached, the resulting `ProtocolOutOfSync` would discard the connection
before reuse, bounding the blast radius to a single request.

---

## ✅ Issue 4 — `extended` flag is permanently set and never resets

**Severity:** Low-medium — affects connection-lifecycle semantics and silently changes Error handler
behaviour for all subsequent requests on a connection.

**Location:** `pgdog/src/backend/protocol/state.rs`, `add()` / `add_ignore()`; `Code` arm of
`action()`.

### Description

`ProtocolState.extended` was set to `true` the first time any parameterised query ran on a
connection and was never reset. The Error handler checked this flag to set `out_of_sync = true`;
because the flag was permanent, every subsequent error on that connection — including plain
simple-query errors — set `out_of_sync = true` spuriously.

### Code path

`add()` and `add_ignore()` latch the flag via `self.extended = self.extended || code.extended()`
whenever `ParseComplete ('1')` or `BindComplete ('2')` is enqueued. Once set, the Error handler
checked `self.extended` to set `out_of_sync = true`, with no reset path, so every subsequent
error on the connection triggered it regardless of whether the current request was parameterised.

### Consequences

- `done()` stayed `false` one extra round-trip (until RFQ cleared `out_of_sync`) on simple-query
  errors for connections that had ever served a parameterised query.
- Future changes to the Error handler that added `extended`-specific behaviour would silently apply
  to all long-lived connections, not just those currently mid-pipeline.
- `extended` read as "has this connection *ever* been in extended-protocol mode", not "is this
  connection *currently* in extended-protocol mode" — a semantic mismatch.

### Reproduction (historical)

1. Connect to pgdog.
2. Execute a parameterised query (any `$1` placeholder) — permanently sets `extended = true`.
3. Execute `SELECT 1/0` (simple query).
4. Observe `server.out_of_sync() == true` immediately after the `'E'` response, before RFQ arrives.
   Expected: `false`.

```sh
cargo test -p pgdog -- test_extended_resets_after_rfq_drain
```

### Tests

**State-machine unit tests (`state.rs`, no backend needed)**

- **`test_extended_resets_on_rfq_drain`** — parameterised queue drains; `extended` is `true` before
  the final RFQ and `false` after.
- **`test_extended_stays_true_mid_pipeline`** — an intermediate RFQ with items still queued behind
  it does not prematurely reset `extended`; only the last RFQ that drains the queue resets it.
- **`test_no_spurious_out_of_sync_after_extended_reset`** — after a parameterised pipeline
  completes and `extended` resets, a subsequent simple-query error does not set `out_of_sync`.

**Server-level test (`server.rs`, requires PostgreSQL)**

- **`test_extended_resets_after_rfq_drain`** — four phases on one connection: (1) baseline simple
  error, no `out_of_sync`; (2) parameterised query sets `extended`, RFQ drain resets it; (3) and
  (4) simple errors after reset, both assert `out_of_sync == false`.

```sh
cargo test -p pgdog --lib -- test_extended_resets
cargo test -p pgdog -- test_extended_resets_after_rfq_drain
```

### Fix

In the `Code(in_queue_code)` arm of `action()`, after `pop_front()` has already consumed the
RFQ item, `self.extended` is reset to `false` when the queue is now empty. The check must live
here — after the pop — so `is_empty()` reflects the post-pop state. Placing it in the outer
`ReadyForQuery` match arm (as originally proposed) runs before `pop_front()` and would never
observe an empty queue. Resetting only when `is_empty()` is safe: pipelined requests still in
the queue keep `extended = true` until the entire pipeline finishes.

---

## Common thread

All four issues share the same underlying fragility: the `ProtocolState` queue and the actual server
response stream diverge whenever an error or unexpected message interrupts a multi-message
sub-request injected transparently by pgdog. The Error handler was written for a single
client-visible request and did not account for the compound structures the prepared-statement
rewriter produces.

Issue 4 was a secondary consequence: `extended` was added as a guard for the Error handler but was
attached to the connection rather than the current pipeline, so it outlived the requests it was meant
to describe.
