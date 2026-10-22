# Routing: `main` vs branch

This file compares routing on `main` with routing on the branch. It has two parts: the priority of routing sources, and examples per configuration.

## Priority

Highest first.

Branch: "any server" means the primary with `read_write_split = "prefer_primary"`, otherwise the servers that `read_write_split` allows.

### `query_parser = "on"`

Also `auto` when the parser runs: more than one shard, a primary and replicas, multi-tenant, dry run, or `prepared_statements = "full"`.

| # | `main` | Branch |
|---|---|---|
| 1 | Server already bound in the transaction | Server already bound in the transaction |
| 2 | Explicit transaction binding from `BEGIN`: `BEGIN READ ONLY` → read; conservative `BEGIN` → write; aggressive or read-only cluster → first statement decides | Comment role (skipped in a conservative read/write transaction when the cluster has a primary) |
| 3 | Sticky startup `pgdog.role` | `SET LOCAL pgdog.role` |
| 4 | Plugin read/write | `SET pgdog.role` |
| 5 | Statement type (DML/DDL → write) | Startup `pgdog.role` |
| 6 | Comment role (only when the router is needed or in dry run) | `users.toml` `role` |
| 7 | Conservative implicit transaction (multi-`Execute` pipeline) → write, not on a read-only cluster. For explicit `BEGIN`, row 2 already decides | Conservative read/write or implicit transaction → primary (only if the cluster has a primary) |
| 8 | `SET LOCAL` / `SET` / startup `pgdog.role` (`primary` forces write; `replica` only blocks row 9) | `BEGIN READ ONLY` → any server |
| 9 | `prefer_primary` → write | Plugin read/write |
| 10 | Default: `SELECT` → read | Statement type (read → any server, write → primary) |

### `query_parser = "session_control"`

Also `session_control_and_locks`, and `auto` when the parser does not run for other reasons. PgDog parses only session statements (`SET`, `RESET`, `BEGIN`, `COMMIT`, `ROLLBACK`, ...). Other statements use the bypass.

| # | `main` | Branch |
|---|---|---|
| 1 | Server already bound in the transaction | Server already bound in the transaction |
| 2 | Transaction binding from `BEGIN`: `BEGIN READ ONLY` → read; conservative `BEGIN` → write; aggressive or read-only cluster → first statement decides | `SET LOCAL pgdog.role` |
| 3 | Sticky startup `pgdog.role` | `SET pgdog.role` |
| 4 | Read-only cluster (`user.read_only` or no primary) → read | Startup `pgdog.role` |
| 5 | Write-only cluster (no replicas) → write | `users.toml` `role` |
| 6 | `SET LOCAL` / `SET` / startup `pgdog.role` (`replica` → read, `primary` or `auto` → write) | Conservative read/write transaction → primary (only if the cluster has a primary) |
| 7 | `prefer_primary` → write | `BEGIN READ ONLY` → any server |
| 8 | `exclude_primary` → read | No primary → read |
| 9 | Default → write | `exclude_primary` with replicas → read |
| 10 | — | Default → write (also `prefer_primary`) |

### `query_parser = "off"`

PgDog parses nothing. `SET`, comments, and `BEGIN READ ONLY` have no effect. The transaction stays on the server that `BEGIN` reached.

| # | `main` | Branch |
|---|---|---|
| 1 | Server already bound in the transaction | Server already bound in the transaction |
| 2 | Sticky startup `pgdog.role` | Startup `pgdog.role` |
| 3 | Read-only cluster (`user.read_only` or no primary) → read | `users.toml` `role` |
| 4 | Write-only cluster (no replicas) → write | No primary → read |
| 5 | `prefer_primary` → write | `exclude_primary` with replicas → read |
| 6 | `exclude_primary` → read | Default → write (also `prefer_primary`) |
| 7 | Default → write | — |

## Examples

Each section is one configuration. Every section uses the same client sessions. **Bold** rows give a different result on `main` and on the branch.

Common assumptions:

- One shard.
- "Startup `pgdog.role=X`" means that the client connects with `options=-c pgdog.role=X`.
- "primary or replica" means the load balancer picks one.
- "error" means Postgres rejects a write: a replica is read-only, and a `read_only` user has `default_transaction_read_only = on`.
- With `query_parser = "session_control"`, PgDog parses `SET` and `BEGIN`, but not `SELECT` or `UPDATE`. So comments on `SELECT`/`UPDATE` are not read.
- With `query_parser = "off"`, PgDog parses nothing. `SET pgdog.role` and `BEGIN` go to the server like any query, and PgDog does not record the role.

With a primary and replicas on one shard, `query_parser = "auto"` runs the parser for every statement, so it behaves like `"on"`.

### 1. Primary and replica, `query_parser = "on"`

```toml
[general]
query_parser = "on"
read_write_strategy = "conservative"
read_write_split = "include_primary"

[[databases]]
name = "pgdog"
host = "10.0.0.1"
role = "primary"

[[databases]]
name = "pgdog"
host = "10.0.0.2"
role = "replica"
```

| Client sends | `main` | Branch |
|---|---|---|
| _No role_ | | |
| `SELECT 1` | primary or replica | primary or replica |
| `UPDATE t SET id = 1` | primary | primary |
| `BEGIN; SELECT 1` | primary | primary |
| `BEGIN READ ONLY; SELECT 1` | primary or replica | primary or replica |
| _`SET pgdog.role`_ | | |
| **`SET pgdog.role TO 'replica'; SELECT 1`** | **primary or replica** | **replica** |
| **`SET pgdog.role TO 'replica'; UPDATE t SET id = 1`** | **primary** | **replica → error** |
| `SET pgdog.role TO 'primary'; SELECT 1` | primary | primary |
| **`SET pgdog.role TO 'primary'; BEGIN READ ONLY; SELECT 1`** | **primary or replica** | **primary** |
| `SET pgdog.role TO 'auto'; SELECT 1` | primary or replica | primary or replica |
| `SET pgdog.role TO 'auto'; UPDATE t SET id = 1` | primary | primary |
| _`SET LOCAL pgdog.role`_ | | |
| **`BEGIN; SET LOCAL pgdog.role TO 'replica'; SELECT 1`** | **primary** | **replica** |
| `BEGIN; SET LOCAL pgdog.role TO 'primary'; SELECT 1` | primary | primary |
| `BEGIN; SET LOCAL pgdog.role TO 'auto'; SELECT 1` | primary | primary |
| **`BEGIN; SELECT 1; SET LOCAL pgdog.role TO 'replica'`** | **primary; the `SET` has no effect** | **primary; the `SET` returns an error** |
| _Comment `/* pgdog_role */`_ | | |
| **`/* pgdog_role: replica */ SELECT 1`** | **primary or replica** | **replica** |
| **`/* pgdog_role: replica */ UPDATE t SET id = 1`** | **primary** | **replica → error** |
| **Pipeline `Execute` × 2 + `Sync`, first `/* pgdog_role: replica */ SELECT 1`** | **primary or replica** | **primary** |
| `/* pgdog_role: primary */ SELECT 1` | primary | primary |
| `/* pgdog_role: auto */ SELECT 1` | primary or replica | primary or replica |
| _Startup `pgdog.role`_ | | |
| **Startup `pgdog.role=replica`; `SELECT 1`** | **primary or replica** | **replica** |
| **Startup `pgdog.role=replica`; `UPDATE t SET id = 1`** | **primary or replica; error on the replica** | **replica → error** |
| **Startup `pgdog.role=replica`; `BEGIN; SELECT 1`** | **primary** | **replica** |
| **Startup `pgdog.role=primary`; `BEGIN READ ONLY; SELECT 1`** | **primary or replica** | **primary** |
| Startup `pgdog.role=auto`; `SELECT 1` | primary or replica | primary or replica |
| _Override: a higher source wins over a lower one_ | | |
| **`SET pgdog.role TO 'primary'; /* pgdog_role: replica */ SELECT 1`** | **primary or replica** | **replica** |
| `SET pgdog.role TO 'replica'; /* pgdog_role: primary */ SELECT 1` | primary | primary |
| `SET pgdog.role TO 'replica'; /* pgdog_role: auto */ SELECT 1` | primary or replica | primary or replica |
| **`SET pgdog.role TO 'primary'; BEGIN; SET LOCAL pgdog.role TO 'replica'; SELECT 1`** | **primary** | **replica** |
| **`BEGIN; SET LOCAL pgdog.role TO 'replica'; /* pgdog_role: primary */ SELECT 1`** | **primary** | **replica** |
| **Startup `pgdog.role=replica`; `/* pgdog_role: primary */ SELECT 1`** | **primary or replica** | **primary** |
| **Startup `pgdog.role=replica`; `SET pgdog.role TO 'primary'; SELECT 1`** | **primary or replica** | **primary** |
| Startup `pgdog.role=replica`; `/* pgdog_role: auto */ SELECT 1` | primary or replica | primary or replica |

### 2. Primary and replica, `query_parser = "session_control"`

```toml
[general]
query_parser = "session_control"
read_write_strategy = "conservative"
read_write_split = "include_primary"

[[databases]]
name = "pgdog"
host = "10.0.0.1"
role = "primary"

[[databases]]
name = "pgdog"
host = "10.0.0.2"
role = "replica"
```

| Client sends | `main` | Branch |
|---|---|---|
| _No role_ | | |
| `SELECT 1` | primary | primary |
| `UPDATE t SET id = 1` | primary | primary |
| `BEGIN; SELECT 1` | primary | primary |
| `BEGIN READ ONLY; SELECT 1` | primary or replica | primary or replica |
| _`SET pgdog.role`_ | | |
| **`SET pgdog.role TO 'replica'; SELECT 1`** | **primary or replica** | **replica** |
| **`SET pgdog.role TO 'replica'; UPDATE t SET id = 1`** | **primary or replica; error on the replica** | **replica → error** |
| `SET pgdog.role TO 'primary'; SELECT 1` | primary | primary |
| **`SET pgdog.role TO 'primary'; BEGIN READ ONLY; SELECT 1`** | **primary or replica** | **primary** |
| `SET pgdog.role TO 'auto'; SELECT 1` | primary | primary |
| `SET pgdog.role TO 'auto'; UPDATE t SET id = 1` | primary | primary |
| _`SET LOCAL pgdog.role`_ | | |
| **`BEGIN; SET LOCAL pgdog.role TO 'replica'; SELECT 1`** | **primary** | **replica** |
| `BEGIN; SET LOCAL pgdog.role TO 'primary'; SELECT 1` | primary | primary |
| `BEGIN; SET LOCAL pgdog.role TO 'auto'; SELECT 1` | primary | primary |
| **`BEGIN; SELECT 1; SET LOCAL pgdog.role TO 'replica'`** | **primary; the `SET` has no effect** | **primary; the `SET` returns an error** |
| _Comment `/* pgdog_role */`_ | | |
| `/* pgdog_role: replica */ SELECT 1` | primary | primary |
| `/* pgdog_role: replica */ UPDATE t SET id = 1` | primary | primary |
| Pipeline `Execute` × 2 + `Sync`, first `/* pgdog_role: replica */ SELECT 1` | primary | primary |
| `/* pgdog_role: primary */ SELECT 1` | primary | primary |
| `/* pgdog_role: auto */ SELECT 1` | primary | primary |
| _Startup `pgdog.role`_ | | |
| **Startup `pgdog.role=replica`; `SELECT 1`** | **primary or replica** | **replica** |
| **Startup `pgdog.role=replica`; `UPDATE t SET id = 1`** | **primary or replica; error on the replica** | **replica → error** |
| **Startup `pgdog.role=replica`; `BEGIN; SELECT 1`** | **primary** | **replica** |
| **Startup `pgdog.role=primary`; `BEGIN READ ONLY; SELECT 1`** | **primary or replica** | **primary** |
| Startup `pgdog.role=auto`; `SELECT 1` | primary | primary |
| _Override: a higher source wins over a lower one_ | | |
| `SET pgdog.role TO 'primary'; /* pgdog_role: replica */ SELECT 1` | primary | primary |
| **`SET pgdog.role TO 'replica'; /* pgdog_role: primary */ SELECT 1`** | **primary or replica** | **replica** |
| **`SET pgdog.role TO 'replica'; /* pgdog_role: auto */ SELECT 1`** | **primary or replica** | **replica** |
| **`SET pgdog.role TO 'primary'; BEGIN; SET LOCAL pgdog.role TO 'replica'; SELECT 1`** | **primary** | **replica** |
| **`BEGIN; SET LOCAL pgdog.role TO 'replica'; /* pgdog_role: primary */ SELECT 1`** | **primary** | **replica** |
| **Startup `pgdog.role=replica`; `/* pgdog_role: primary */ SELECT 1`** | **primary or replica** | **replica** |
| **Startup `pgdog.role=replica`; `SET pgdog.role TO 'primary'; SELECT 1`** | **primary or replica** | **primary** |
| **Startup `pgdog.role=replica`; `/* pgdog_role: auto */ SELECT 1`** | **primary or replica** | **replica** |

### 3. Primary and replica, `query_parser = "off"`

```toml
[general]
query_parser = "off"
read_write_strategy = "conservative"
read_write_split = "include_primary"

[[databases]]
name = "pgdog"
host = "10.0.0.1"
role = "primary"

[[databases]]
name = "pgdog"
host = "10.0.0.2"
role = "replica"
```

| Client sends | `main` | Branch |
|---|---|---|
| _No role_ | | |
| `SELECT 1` | primary | primary |
| `UPDATE t SET id = 1` | primary | primary |
| `BEGIN; SELECT 1` | primary | primary |
| `BEGIN READ ONLY; SELECT 1` | primary | primary |
| _`SET pgdog.role`_ | | |
| `SET pgdog.role TO 'replica'; SELECT 1` | primary | primary |
| `SET pgdog.role TO 'replica'; UPDATE t SET id = 1` | primary | primary |
| `SET pgdog.role TO 'primary'; SELECT 1` | primary | primary |
| `SET pgdog.role TO 'primary'; BEGIN READ ONLY; SELECT 1` | primary | primary |
| `SET pgdog.role TO 'auto'; SELECT 1` | primary | primary |
| `SET pgdog.role TO 'auto'; UPDATE t SET id = 1` | primary | primary |
| _`SET LOCAL pgdog.role`_ | | |
| `BEGIN; SET LOCAL pgdog.role TO 'replica'; SELECT 1` | primary | primary |
| `BEGIN; SET LOCAL pgdog.role TO 'primary'; SELECT 1` | primary | primary |
| `BEGIN; SET LOCAL pgdog.role TO 'auto'; SELECT 1` | primary | primary |
| `BEGIN; SELECT 1; SET LOCAL pgdog.role TO 'replica'` | primary; the `SET` has no effect | primary; the `SET` has no effect |
| _Comment `/* pgdog_role */`_ | | |
| `/* pgdog_role: replica */ SELECT 1` | primary | primary |
| `/* pgdog_role: replica */ UPDATE t SET id = 1` | primary | primary |
| Pipeline `Execute` × 2 + `Sync`, first `/* pgdog_role: replica */ SELECT 1` | primary | primary |
| `/* pgdog_role: primary */ SELECT 1` | primary | primary |
| `/* pgdog_role: auto */ SELECT 1` | primary | primary |
| _Startup `pgdog.role`_ | | |
| **Startup `pgdog.role=replica`; `SELECT 1`** | **primary or replica** | **replica** |
| **Startup `pgdog.role=replica`; `UPDATE t SET id = 1`** | **primary or replica; error on the replica** | **replica → error** |
| **Startup `pgdog.role=replica`; `BEGIN; SELECT 1`** | **primary or replica** | **replica** |
| Startup `pgdog.role=primary`; `BEGIN READ ONLY; SELECT 1` | primary | primary |
| Startup `pgdog.role=auto`; `SELECT 1` | primary | primary |
| _Override: a higher source wins over a lower one_ | | |
| `SET pgdog.role TO 'primary'; /* pgdog_role: replica */ SELECT 1` | primary | primary |
| `SET pgdog.role TO 'replica'; /* pgdog_role: primary */ SELECT 1` | primary | primary |
| `SET pgdog.role TO 'replica'; /* pgdog_role: auto */ SELECT 1` | primary | primary |
| `SET pgdog.role TO 'primary'; BEGIN; SET LOCAL pgdog.role TO 'replica'; SELECT 1` | primary | primary |
| `BEGIN; SET LOCAL pgdog.role TO 'replica'; /* pgdog_role: primary */ SELECT 1` | primary | primary |
| **Startup `pgdog.role=replica`; `/* pgdog_role: primary */ SELECT 1`** | **primary or replica** | **replica** |
| **Startup `pgdog.role=replica`; `SET pgdog.role TO 'primary'; SELECT 1`** | **primary or replica** | **replica** |
| **Startup `pgdog.role=replica`; `/* pgdog_role: auto */ SELECT 1`** | **primary or replica** | **replica** |

### 4. `prefer_primary`, `query_parser = "on"`

```toml
[general]
query_parser = "on"
read_write_strategy = "conservative"
read_write_split = "prefer_primary"

[[databases]]
name = "pgdog"
host = "10.0.0.1"
role = "primary"

[[databases]]
name = "pgdog"
host = "10.0.0.2"
role = "replica"
```

| Client sends | `main` | Branch |
|---|---|---|
| _No role_ | | |
| `SELECT 1` | primary | primary |
| `UPDATE t SET id = 1` | primary | primary |
| `BEGIN; SELECT 1` | primary | primary |
| **`BEGIN READ ONLY; SELECT 1`** | **replica** | **primary** |
| _`SET pgdog.role`_ | | |
| `SET pgdog.role TO 'replica'; SELECT 1` | replica | replica |
| **`SET pgdog.role TO 'replica'; UPDATE t SET id = 1`** | **primary** | **replica → error** |
| `SET pgdog.role TO 'primary'; SELECT 1` | primary | primary |
| **`SET pgdog.role TO 'primary'; BEGIN READ ONLY; SELECT 1`** | **replica** | **primary** |
| `SET pgdog.role TO 'auto'; SELECT 1` | primary | primary |
| `SET pgdog.role TO 'auto'; UPDATE t SET id = 1` | primary | primary |
| _`SET LOCAL pgdog.role`_ | | |
| **`BEGIN; SET LOCAL pgdog.role TO 'replica'; SELECT 1`** | **primary** | **replica** |
| `BEGIN; SET LOCAL pgdog.role TO 'primary'; SELECT 1` | primary | primary |
| `BEGIN; SET LOCAL pgdog.role TO 'auto'; SELECT 1` | primary | primary |
| **`BEGIN; SELECT 1; SET LOCAL pgdog.role TO 'replica'`** | **primary; the `SET` has no effect** | **primary; the `SET` returns an error** |
| _Comment `/* pgdog_role */`_ | | |
| `/* pgdog_role: replica */ SELECT 1` | replica | replica |
| **`/* pgdog_role: replica */ UPDATE t SET id = 1`** | **primary** | **replica → error** |
| **Pipeline `Execute` × 2 + `Sync`, first `/* pgdog_role: replica */ SELECT 1`** | **replica** | **primary** |
| `/* pgdog_role: primary */ SELECT 1` | primary | primary |
| `/* pgdog_role: auto */ SELECT 1` | primary | primary |
| _Startup `pgdog.role`_ | | |
| Startup `pgdog.role=replica`; `SELECT 1` | replica | replica |
| Startup `pgdog.role=replica`; `UPDATE t SET id = 1` | replica → error | replica → error |
| **Startup `pgdog.role=replica`; `BEGIN; SELECT 1`** | **primary** | **replica** |
| **Startup `pgdog.role=primary`; `BEGIN READ ONLY; SELECT 1`** | **replica** | **primary** |
| Startup `pgdog.role=auto`; `SELECT 1` | primary | primary |
| _Override: a higher source wins over a lower one_ | | |
| `SET pgdog.role TO 'primary'; /* pgdog_role: replica */ SELECT 1` | replica | replica |
| `SET pgdog.role TO 'replica'; /* pgdog_role: primary */ SELECT 1` | primary | primary |
| **`SET pgdog.role TO 'replica'; /* pgdog_role: auto */ SELECT 1`** | **replica** | **primary** |
| **`SET pgdog.role TO 'primary'; BEGIN; SET LOCAL pgdog.role TO 'replica'; SELECT 1`** | **primary** | **replica** |
| **`BEGIN; SET LOCAL pgdog.role TO 'replica'; /* pgdog_role: primary */ SELECT 1`** | **primary** | **replica** |
| **Startup `pgdog.role=replica`; `/* pgdog_role: primary */ SELECT 1`** | **replica** | **primary** |
| **Startup `pgdog.role=replica`; `SET pgdog.role TO 'primary'; SELECT 1`** | **replica** | **primary** |
| **Startup `pgdog.role=replica`; `/* pgdog_role: auto */ SELECT 1`** | **replica** | **primary** |

### 5. `prefer_primary`, `query_parser = "session_control"`

```toml
[general]
query_parser = "session_control"
read_write_strategy = "conservative"
read_write_split = "prefer_primary"

[[databases]]
name = "pgdog"
host = "10.0.0.1"
role = "primary"

[[databases]]
name = "pgdog"
host = "10.0.0.2"
role = "replica"
```

| Client sends | `main` | Branch |
|---|---|---|
| _No role_ | | |
| `SELECT 1` | primary | primary |
| `UPDATE t SET id = 1` | primary | primary |
| `BEGIN; SELECT 1` | primary | primary |
| **`BEGIN READ ONLY; SELECT 1`** | **replica** | **primary** |
| _`SET pgdog.role`_ | | |
| `SET pgdog.role TO 'replica'; SELECT 1` | replica | replica |
| `SET pgdog.role TO 'replica'; UPDATE t SET id = 1` | replica → error | replica → error |
| `SET pgdog.role TO 'primary'; SELECT 1` | primary | primary |
| **`SET pgdog.role TO 'primary'; BEGIN READ ONLY; SELECT 1`** | **replica** | **primary** |
| `SET pgdog.role TO 'auto'; SELECT 1` | primary | primary |
| `SET pgdog.role TO 'auto'; UPDATE t SET id = 1` | primary | primary |
| _`SET LOCAL pgdog.role`_ | | |
| **`BEGIN; SET LOCAL pgdog.role TO 'replica'; SELECT 1`** | **primary** | **replica** |
| `BEGIN; SET LOCAL pgdog.role TO 'primary'; SELECT 1` | primary | primary |
| `BEGIN; SET LOCAL pgdog.role TO 'auto'; SELECT 1` | primary | primary |
| **`BEGIN; SELECT 1; SET LOCAL pgdog.role TO 'replica'`** | **primary; the `SET` has no effect** | **primary; the `SET` returns an error** |
| _Comment `/* pgdog_role */`_ | | |
| `/* pgdog_role: replica */ SELECT 1` | primary | primary |
| `/* pgdog_role: replica */ UPDATE t SET id = 1` | primary | primary |
| Pipeline `Execute` × 2 + `Sync`, first `/* pgdog_role: replica */ SELECT 1` | primary | primary |
| `/* pgdog_role: primary */ SELECT 1` | primary | primary |
| `/* pgdog_role: auto */ SELECT 1` | primary | primary |
| _Startup `pgdog.role`_ | | |
| Startup `pgdog.role=replica`; `SELECT 1` | replica | replica |
| Startup `pgdog.role=replica`; `UPDATE t SET id = 1` | replica → error | replica → error |
| **Startup `pgdog.role=replica`; `BEGIN; SELECT 1`** | **primary** | **replica** |
| **Startup `pgdog.role=primary`; `BEGIN READ ONLY; SELECT 1`** | **replica** | **primary** |
| Startup `pgdog.role=auto`; `SELECT 1` | primary | primary |
| _Override: a higher source wins over a lower one_ | | |
| `SET pgdog.role TO 'primary'; /* pgdog_role: replica */ SELECT 1` | primary | primary |
| `SET pgdog.role TO 'replica'; /* pgdog_role: primary */ SELECT 1` | replica | replica |
| `SET pgdog.role TO 'replica'; /* pgdog_role: auto */ SELECT 1` | replica | replica |
| **`SET pgdog.role TO 'primary'; BEGIN; SET LOCAL pgdog.role TO 'replica'; SELECT 1`** | **primary** | **replica** |
| **`BEGIN; SET LOCAL pgdog.role TO 'replica'; /* pgdog_role: primary */ SELECT 1`** | **primary** | **replica** |
| Startup `pgdog.role=replica`; `/* pgdog_role: primary */ SELECT 1` | replica | replica |
| **Startup `pgdog.role=replica`; `SET pgdog.role TO 'primary'; SELECT 1`** | **replica** | **primary** |
| Startup `pgdog.role=replica`; `/* pgdog_role: auto */ SELECT 1` | replica | replica |

### 6. `exclude_primary`, `query_parser = "on"`

```toml
[general]
query_parser = "on"
read_write_strategy = "conservative"
read_write_split = "exclude_primary"

[[databases]]
name = "pgdog"
host = "10.0.0.1"
role = "primary"

[[databases]]
name = "pgdog"
host = "10.0.0.2"
role = "replica"
```

| Client sends | `main` | Branch |
|---|---|---|
| _No role_ | | |
| `SELECT 1` | replica | replica |
| `UPDATE t SET id = 1` | primary | primary |
| `BEGIN; SELECT 1` | primary | primary |
| `BEGIN READ ONLY; SELECT 1` | replica | replica |
| _`SET pgdog.role`_ | | |
| `SET pgdog.role TO 'replica'; SELECT 1` | replica | replica |
| **`SET pgdog.role TO 'replica'; UPDATE t SET id = 1`** | **primary** | **replica → error** |
| `SET pgdog.role TO 'primary'; SELECT 1` | primary | primary |
| **`SET pgdog.role TO 'primary'; BEGIN READ ONLY; SELECT 1`** | **replica** | **primary** |
| `SET pgdog.role TO 'auto'; SELECT 1` | replica | replica |
| `SET pgdog.role TO 'auto'; UPDATE t SET id = 1` | primary | primary |
| _`SET LOCAL pgdog.role`_ | | |
| **`BEGIN; SET LOCAL pgdog.role TO 'replica'; SELECT 1`** | **primary** | **replica** |
| `BEGIN; SET LOCAL pgdog.role TO 'primary'; SELECT 1` | primary | primary |
| `BEGIN; SET LOCAL pgdog.role TO 'auto'; SELECT 1` | primary | primary |
| **`BEGIN; SELECT 1; SET LOCAL pgdog.role TO 'replica'`** | **primary; the `SET` has no effect** | **primary; the `SET` returns an error** |
| _Comment `/* pgdog_role */`_ | | |
| `/* pgdog_role: replica */ SELECT 1` | replica | replica |
| **`/* pgdog_role: replica */ UPDATE t SET id = 1`** | **primary** | **replica → error** |
| **Pipeline `Execute` × 2 + `Sync`, first `/* pgdog_role: replica */ SELECT 1`** | **replica** | **primary** |
| `/* pgdog_role: primary */ SELECT 1` | primary | primary |
| `/* pgdog_role: auto */ SELECT 1` | replica | replica |
| _Startup `pgdog.role`_ | | |
| Startup `pgdog.role=replica`; `SELECT 1` | replica | replica |
| Startup `pgdog.role=replica`; `UPDATE t SET id = 1` | replica → error | replica → error |
| **Startup `pgdog.role=replica`; `BEGIN; SELECT 1`** | **primary** | **replica** |
| **Startup `pgdog.role=primary`; `BEGIN READ ONLY; SELECT 1`** | **replica** | **primary** |
| Startup `pgdog.role=auto`; `SELECT 1` | replica | replica |
| _Override: a higher source wins over a lower one_ | | |
| `SET pgdog.role TO 'primary'; /* pgdog_role: replica */ SELECT 1` | replica | replica |
| `SET pgdog.role TO 'replica'; /* pgdog_role: primary */ SELECT 1` | primary | primary |
| `SET pgdog.role TO 'replica'; /* pgdog_role: auto */ SELECT 1` | replica | replica |
| **`SET pgdog.role TO 'primary'; BEGIN; SET LOCAL pgdog.role TO 'replica'; SELECT 1`** | **primary** | **replica** |
| **`BEGIN; SET LOCAL pgdog.role TO 'replica'; /* pgdog_role: primary */ SELECT 1`** | **primary** | **replica** |
| **Startup `pgdog.role=replica`; `/* pgdog_role: primary */ SELECT 1`** | **replica** | **primary** |
| **Startup `pgdog.role=replica`; `SET pgdog.role TO 'primary'; SELECT 1`** | **replica** | **primary** |
| Startup `pgdog.role=replica`; `/* pgdog_role: auto */ SELECT 1` | replica | replica |

### 7. `exclude_primary`, `query_parser = "session_control"`

```toml
[general]
query_parser = "session_control"
read_write_strategy = "conservative"
read_write_split = "exclude_primary"

[[databases]]
name = "pgdog"
host = "10.0.0.1"
role = "primary"

[[databases]]
name = "pgdog"
host = "10.0.0.2"
role = "replica"
```

| Client sends | `main` | Branch |
|---|---|---|
| _No role_ | | |
| `SELECT 1` | replica | replica |
| `UPDATE t SET id = 1` | replica → error | replica → error |
| `BEGIN; SELECT 1` | primary | primary |
| `BEGIN READ ONLY; SELECT 1` | replica | replica |
| _`SET pgdog.role`_ | | |
| `SET pgdog.role TO 'replica'; SELECT 1` | replica | replica |
| `SET pgdog.role TO 'replica'; UPDATE t SET id = 1` | replica → error | replica → error |
| `SET pgdog.role TO 'primary'; SELECT 1` | primary | primary |
| **`SET pgdog.role TO 'primary'; BEGIN READ ONLY; SELECT 1`** | **replica** | **primary** |
| `SET pgdog.role TO 'auto'; SELECT 1` | replica | replica |
| `SET pgdog.role TO 'auto'; UPDATE t SET id = 1` | replica → error | replica → error |
| _`SET LOCAL pgdog.role`_ | | |
| **`BEGIN; SET LOCAL pgdog.role TO 'replica'; SELECT 1`** | **primary** | **replica** |
| `BEGIN; SET LOCAL pgdog.role TO 'primary'; SELECT 1` | primary | primary |
| `BEGIN; SET LOCAL pgdog.role TO 'auto'; SELECT 1` | primary | primary |
| **`BEGIN; SELECT 1; SET LOCAL pgdog.role TO 'replica'`** | **primary; the `SET` has no effect** | **primary; the `SET` returns an error** |
| _Comment `/* pgdog_role */`_ | | |
| `/* pgdog_role: replica */ SELECT 1` | replica | replica |
| `/* pgdog_role: replica */ UPDATE t SET id = 1` | replica → error | replica → error |
| **Pipeline `Execute` × 2 + `Sync`, first `/* pgdog_role: replica */ SELECT 1`** | **replica** | **primary** |
| `/* pgdog_role: primary */ SELECT 1` | replica | replica |
| `/* pgdog_role: auto */ SELECT 1` | replica | replica |
| _Startup `pgdog.role`_ | | |
| Startup `pgdog.role=replica`; `SELECT 1` | replica | replica |
| Startup `pgdog.role=replica`; `UPDATE t SET id = 1` | replica → error | replica → error |
| **Startup `pgdog.role=replica`; `BEGIN; SELECT 1`** | **primary** | **replica** |
| **Startup `pgdog.role=primary`; `BEGIN READ ONLY; SELECT 1`** | **replica** | **primary** |
| Startup `pgdog.role=auto`; `SELECT 1` | replica | replica |
| _Override: a higher source wins over a lower one_ | | |
| `SET pgdog.role TO 'primary'; /* pgdog_role: replica */ SELECT 1` | primary | primary |
| `SET pgdog.role TO 'replica'; /* pgdog_role: primary */ SELECT 1` | replica | replica |
| `SET pgdog.role TO 'replica'; /* pgdog_role: auto */ SELECT 1` | replica | replica |
| **`SET pgdog.role TO 'primary'; BEGIN; SET LOCAL pgdog.role TO 'replica'; SELECT 1`** | **primary** | **replica** |
| **`BEGIN; SET LOCAL pgdog.role TO 'replica'; /* pgdog_role: primary */ SELECT 1`** | **primary** | **replica** |
| Startup `pgdog.role=replica`; `/* pgdog_role: primary */ SELECT 1` | replica | replica |
| **Startup `pgdog.role=replica`; `SET pgdog.role TO 'primary'; SELECT 1`** | **replica** | **primary** |
| Startup `pgdog.role=replica`; `/* pgdog_role: auto */ SELECT 1` | replica | replica |

### 8. Read-only user, `query_parser = "auto"`

```toml
[general]
query_parser = "auto"
read_write_strategy = "conservative"
read_write_split = "include_primary"

[[databases]]
name = "pgdog"
host = "10.0.0.1"
role = "primary"

[[databases]]
name = "pgdog"
host = "10.0.0.2"
role = "replica"
```

```toml
# users.toml
[[users]]
name = "reader"
database = "pgdog"
read_only = true
```

On `main`, the parser runs only for `SET` and `BEGIN` of this user. On the branch, it runs for every statement.

| Client sends | `main` | Branch |
|---|---|---|
| _No role_ | | |
| **`SELECT 1`** | **replica** | **primary or replica** |
| **`UPDATE t SET id = 1`** | **replica → error** | **primary → error** |
| **`BEGIN; SELECT 1`** | **replica** | **primary** |
| **`BEGIN READ ONLY; SELECT 1`** | **replica** | **primary or replica** |
| _`SET pgdog.role`_ | | |
| `SET pgdog.role TO 'replica'; SELECT 1` | replica | replica |
| `SET pgdog.role TO 'replica'; UPDATE t SET id = 1` | replica → error | replica → error |
| **`SET pgdog.role TO 'primary'; SELECT 1`** | **replica** | **primary** |
| **`SET pgdog.role TO 'primary'; BEGIN READ ONLY; SELECT 1`** | **replica** | **primary** |
| **`SET pgdog.role TO 'auto'; SELECT 1`** | **replica** | **primary or replica** |
| **`SET pgdog.role TO 'auto'; UPDATE t SET id = 1`** | **replica → error** | **primary → error** |
| _`SET LOCAL pgdog.role`_ | | |
| `BEGIN; SET LOCAL pgdog.role TO 'replica'; SELECT 1` | replica | replica |
| **`BEGIN; SET LOCAL pgdog.role TO 'primary'; SELECT 1`** | **replica** | **primary** |
| **`BEGIN; SET LOCAL pgdog.role TO 'auto'; SELECT 1`** | **replica** | **primary** |
| **`BEGIN; SELECT 1; SET LOCAL pgdog.role TO 'replica'`** | **replica; the `SET` has no effect** | **primary; the `SET` returns an error** |
| _Comment `/* pgdog_role */`_ | | |
| `/* pgdog_role: replica */ SELECT 1` | replica | replica |
| `/* pgdog_role: replica */ UPDATE t SET id = 1` | replica → error | replica → error |
| **Pipeline `Execute` × 2 + `Sync`, first `/* pgdog_role: replica */ SELECT 1`** | **replica** | **primary** |
| **`/* pgdog_role: primary */ SELECT 1`** | **replica** | **primary** |
| **`/* pgdog_role: auto */ SELECT 1`** | **replica** | **primary or replica** |
| _Startup `pgdog.role`_ | | |
| Startup `pgdog.role=replica`; `SELECT 1` | replica | replica |
| Startup `pgdog.role=replica`; `UPDATE t SET id = 1` | replica → error | replica → error |
| Startup `pgdog.role=replica`; `BEGIN; SELECT 1` | replica | replica |
| **Startup `pgdog.role=primary`; `BEGIN READ ONLY; SELECT 1`** | **replica** | **primary** |
| **Startup `pgdog.role=auto`; `SELECT 1`** | **replica** | **primary or replica** |
| _Override: a higher source wins over a lower one_ | | |
| `SET pgdog.role TO 'primary'; /* pgdog_role: replica */ SELECT 1` | replica | replica |
| **`SET pgdog.role TO 'replica'; /* pgdog_role: primary */ SELECT 1`** | **replica** | **primary** |
| **`SET pgdog.role TO 'replica'; /* pgdog_role: auto */ SELECT 1`** | **replica** | **primary or replica** |
| `SET pgdog.role TO 'primary'; BEGIN; SET LOCAL pgdog.role TO 'replica'; SELECT 1` | replica | replica |
| `BEGIN; SET LOCAL pgdog.role TO 'replica'; /* pgdog_role: primary */ SELECT 1` | replica | replica |
| **Startup `pgdog.role=replica`; `/* pgdog_role: primary */ SELECT 1`** | **replica** | **primary** |
| **Startup `pgdog.role=replica`; `SET pgdog.role TO 'primary'; SELECT 1`** | **replica** | **primary** |
| **Startup `pgdog.role=replica`; `/* pgdog_role: auto */ SELECT 1`** | **replica** | **primary or replica** |

### 9. Read-only user, `query_parser = "on"`

```toml
[general]
query_parser = "on"
read_write_strategy = "conservative"
read_write_split = "include_primary"

[[databases]]
name = "pgdog"
host = "10.0.0.1"
role = "primary"

[[databases]]
name = "pgdog"
host = "10.0.0.2"
role = "replica"
```

```toml
# users.toml
[[users]]
name = "reader"
database = "pgdog"
read_only = true
```

On `main`, comments are not read for this user, because the cluster counts as read-only.

| Client sends | `main` | Branch |
|---|---|---|
| _No role_ | | |
| **`SELECT 1`** | **replica** | **primary or replica** |
| `UPDATE t SET id = 1` | primary → error | primary → error |
| **`BEGIN; SELECT 1`** | **replica** | **primary** |
| **`BEGIN READ ONLY; SELECT 1`** | **replica** | **primary or replica** |
| _`SET pgdog.role`_ | | |
| `SET pgdog.role TO 'replica'; SELECT 1` | replica | replica |
| **`SET pgdog.role TO 'replica'; UPDATE t SET id = 1`** | **primary → error** | **replica → error** |
| `SET pgdog.role TO 'primary'; SELECT 1` | primary | primary |
| **`SET pgdog.role TO 'primary'; BEGIN READ ONLY; SELECT 1`** | **replica** | **primary** |
| **`SET pgdog.role TO 'auto'; SELECT 1`** | **replica** | **primary or replica** |
| `SET pgdog.role TO 'auto'; UPDATE t SET id = 1` | primary → error | primary → error |
| _`SET LOCAL pgdog.role`_ | | |
| `BEGIN; SET LOCAL pgdog.role TO 'replica'; SELECT 1` | replica | replica |
| `BEGIN; SET LOCAL pgdog.role TO 'primary'; SELECT 1` | primary | primary |
| **`BEGIN; SET LOCAL pgdog.role TO 'auto'; SELECT 1`** | **replica** | **primary** |
| **`BEGIN; SELECT 1; SET LOCAL pgdog.role TO 'replica'`** | **replica; the `SET` has no effect** | **primary; the `SET` returns an error** |
| _Comment `/* pgdog_role */`_ | | |
| `/* pgdog_role: replica */ SELECT 1` | replica | replica |
| **`/* pgdog_role: replica */ UPDATE t SET id = 1`** | **primary → error** | **replica → error** |
| **Pipeline `Execute` × 2 + `Sync`, first `/* pgdog_role: replica */ SELECT 1`** | **replica** | **primary** |
| **`/* pgdog_role: primary */ SELECT 1`** | **replica** | **primary** |
| **`/* pgdog_role: auto */ SELECT 1`** | **replica** | **primary or replica** |
| _Startup `pgdog.role`_ | | |
| Startup `pgdog.role=replica`; `SELECT 1` | replica | replica |
| Startup `pgdog.role=replica`; `UPDATE t SET id = 1` | replica → error | replica → error |
| Startup `pgdog.role=replica`; `BEGIN; SELECT 1` | replica | replica |
| **Startup `pgdog.role=primary`; `BEGIN READ ONLY; SELECT 1`** | **replica** | **primary** |
| **Startup `pgdog.role=auto`; `SELECT 1`** | **replica** | **primary or replica** |
| _Override: a higher source wins over a lower one_ | | |
| **`SET pgdog.role TO 'primary'; /* pgdog_role: replica */ SELECT 1`** | **primary** | **replica** |
| **`SET pgdog.role TO 'replica'; /* pgdog_role: primary */ SELECT 1`** | **replica** | **primary** |
| **`SET pgdog.role TO 'replica'; /* pgdog_role: auto */ SELECT 1`** | **replica** | **primary or replica** |
| `SET pgdog.role TO 'primary'; BEGIN; SET LOCAL pgdog.role TO 'replica'; SELECT 1` | replica | replica |
| `BEGIN; SET LOCAL pgdog.role TO 'replica'; /* pgdog_role: primary */ SELECT 1` | replica | replica |
| **Startup `pgdog.role=replica`; `/* pgdog_role: primary */ SELECT 1`** | **replica** | **primary** |
| **Startup `pgdog.role=replica`; `SET pgdog.role TO 'primary'; SELECT 1`** | **replica** | **primary** |
| **Startup `pgdog.role=replica`; `/* pgdog_role: auto */ SELECT 1`** | **replica** | **primary or replica** |

### 10. Read-only user, `query_parser = "off"`

```toml
[general]
query_parser = "off"
read_write_strategy = "conservative"
read_write_split = "include_primary"

[[databases]]
name = "pgdog"
host = "10.0.0.1"
role = "primary"

[[databases]]
name = "pgdog"
host = "10.0.0.2"
role = "replica"
```

```toml
# users.toml
[[users]]
name = "reader"
database = "pgdog"
read_only = true
```

| Client sends | `main` | Branch |
|---|---|---|
| _No role_ | | |
| **`SELECT 1`** | **replica** | **primary** |
| **`UPDATE t SET id = 1`** | **replica → error** | **primary → error** |
| **`BEGIN; SELECT 1`** | **replica** | **primary** |
| **`BEGIN READ ONLY; SELECT 1`** | **replica** | **primary** |
| _`SET pgdog.role`_ | | |
| **`SET pgdog.role TO 'replica'; SELECT 1`** | **replica** | **primary** |
| **`SET pgdog.role TO 'replica'; UPDATE t SET id = 1`** | **replica → error** | **primary → error** |
| **`SET pgdog.role TO 'primary'; SELECT 1`** | **replica** | **primary** |
| **`SET pgdog.role TO 'primary'; BEGIN READ ONLY; SELECT 1`** | **replica** | **primary** |
| **`SET pgdog.role TO 'auto'; SELECT 1`** | **replica** | **primary** |
| **`SET pgdog.role TO 'auto'; UPDATE t SET id = 1`** | **replica → error** | **primary → error** |
| _`SET LOCAL pgdog.role`_ | | |
| **`BEGIN; SET LOCAL pgdog.role TO 'replica'; SELECT 1`** | **replica** | **primary** |
| **`BEGIN; SET LOCAL pgdog.role TO 'primary'; SELECT 1`** | **replica** | **primary** |
| **`BEGIN; SET LOCAL pgdog.role TO 'auto'; SELECT 1`** | **replica** | **primary** |
| **`BEGIN; SELECT 1; SET LOCAL pgdog.role TO 'replica'`** | **replica; the `SET` has no effect** | **primary; the `SET` has no effect** |
| _Comment `/* pgdog_role */`_ | | |
| **`/* pgdog_role: replica */ SELECT 1`** | **replica** | **primary** |
| **`/* pgdog_role: replica */ UPDATE t SET id = 1`** | **replica → error** | **primary → error** |
| **Pipeline `Execute` × 2 + `Sync`, first `/* pgdog_role: replica */ SELECT 1`** | **replica** | **primary** |
| **`/* pgdog_role: primary */ SELECT 1`** | **replica** | **primary** |
| **`/* pgdog_role: auto */ SELECT 1`** | **replica** | **primary** |
| _Startup `pgdog.role`_ | | |
| Startup `pgdog.role=replica`; `SELECT 1` | replica | replica |
| Startup `pgdog.role=replica`; `UPDATE t SET id = 1` | replica → error | replica → error |
| Startup `pgdog.role=replica`; `BEGIN; SELECT 1` | replica | replica |
| Startup `pgdog.role=primary`; `BEGIN READ ONLY; SELECT 1` | primary | primary |
| **Startup `pgdog.role=auto`; `SELECT 1`** | **replica** | **primary** |
| _Override: a higher source wins over a lower one_ | | |
| **`SET pgdog.role TO 'primary'; /* pgdog_role: replica */ SELECT 1`** | **replica** | **primary** |
| **`SET pgdog.role TO 'replica'; /* pgdog_role: primary */ SELECT 1`** | **replica** | **primary** |
| **`SET pgdog.role TO 'replica'; /* pgdog_role: auto */ SELECT 1`** | **replica** | **primary** |
| **`SET pgdog.role TO 'primary'; BEGIN; SET LOCAL pgdog.role TO 'replica'; SELECT 1`** | **replica** | **primary** |
| **`BEGIN; SET LOCAL pgdog.role TO 'replica'; /* pgdog_role: primary */ SELECT 1`** | **replica** | **primary** |
| Startup `pgdog.role=replica`; `/* pgdog_role: primary */ SELECT 1` | replica | replica |
| Startup `pgdog.role=replica`; `SET pgdog.role TO 'primary'; SELECT 1` | replica | replica |
| Startup `pgdog.role=replica`; `/* pgdog_role: auto */ SELECT 1` | replica | replica |

### 11. Migration: `read_only` on `main` vs `read_only` + `role = "replica"` on the branch, `query_parser = "auto"`

This section compares two different `users.toml` files: the old setting on `main`, and the setting that replaces it on the branch. Both use this `pgdog.toml`:

```toml
[general]
query_parser = "auto"
read_write_strategy = "conservative"
read_write_split = "include_primary"

[[databases]]
name = "pgdog"
host = "10.0.0.1"
role = "primary"

[[databases]]
name = "pgdog"
host = "10.0.0.2"
role = "replica"
```

`main`:

```toml
# users.toml
[[users]]
name = "reader"
database = "pgdog"
read_only = true
```

Branch:

```toml
# users.toml
[[users]]
name = "reader"
database = "pgdog"
read_only = true
role = "replica"
```

| Client sends | `main` | Branch |
|---|---|---|
| _No role_ | | |
| `SELECT 1` | replica | replica |
| `UPDATE t SET id = 1` | replica → error | replica → error |
| `BEGIN; SELECT 1` | replica | replica |
| `BEGIN READ ONLY; SELECT 1` | replica | replica |
| _`SET pgdog.role`_ | | |
| `SET pgdog.role TO 'replica'; SELECT 1` | replica | replica |
| `SET pgdog.role TO 'replica'; UPDATE t SET id = 1` | replica → error | replica → error |
| **`SET pgdog.role TO 'primary'; SELECT 1`** | **replica** | **primary** |
| **`SET pgdog.role TO 'primary'; BEGIN READ ONLY; SELECT 1`** | **replica** | **primary** |
| **`SET pgdog.role TO 'auto'; SELECT 1`** | **replica** | **primary or replica** |
| **`SET pgdog.role TO 'auto'; UPDATE t SET id = 1`** | **replica → error** | **primary → error** |
| _`SET LOCAL pgdog.role`_ | | |
| `BEGIN; SET LOCAL pgdog.role TO 'replica'; SELECT 1` | replica | replica |
| **`BEGIN; SET LOCAL pgdog.role TO 'primary'; SELECT 1`** | **replica** | **primary** |
| **`BEGIN; SET LOCAL pgdog.role TO 'auto'; SELECT 1`** | **replica** | **primary** |
| **`BEGIN; SELECT 1; SET LOCAL pgdog.role TO 'replica'`** | **replica; the `SET` has no effect** | **replica; the `SET` returns an error** |
| _Comment `/* pgdog_role */`_ | | |
| `/* pgdog_role: replica */ SELECT 1` | replica | replica |
| `/* pgdog_role: replica */ UPDATE t SET id = 1` | replica → error | replica → error |
| Pipeline `Execute` × 2 + `Sync`, first `/* pgdog_role: replica */ SELECT 1` | replica | replica |
| **`/* pgdog_role: primary */ SELECT 1`** | **replica** | **primary** |
| **`/* pgdog_role: auto */ SELECT 1`** | **replica** | **primary or replica** |
| _Startup `pgdog.role`_ | | |
| Startup `pgdog.role=replica`; `SELECT 1` | replica | replica |
| Startup `pgdog.role=replica`; `UPDATE t SET id = 1` | replica → error | replica → error |
| Startup `pgdog.role=replica`; `BEGIN; SELECT 1` | replica | replica |
| **Startup `pgdog.role=primary`; `BEGIN READ ONLY; SELECT 1`** | **replica** | **primary** |
| **Startup `pgdog.role=auto`; `SELECT 1`** | **replica** | **primary or replica** |
| _Override: a higher source wins over a lower one_ | | |
| `SET pgdog.role TO 'primary'; /* pgdog_role: replica */ SELECT 1` | replica | replica |
| **`SET pgdog.role TO 'replica'; /* pgdog_role: primary */ SELECT 1`** | **replica** | **primary** |
| **`SET pgdog.role TO 'replica'; /* pgdog_role: auto */ SELECT 1`** | **replica** | **primary or replica** |
| `SET pgdog.role TO 'primary'; BEGIN; SET LOCAL pgdog.role TO 'replica'; SELECT 1` | replica | replica |
| `BEGIN; SET LOCAL pgdog.role TO 'replica'; /* pgdog_role: primary */ SELECT 1` | replica | replica |
| **Startup `pgdog.role=replica`; `/* pgdog_role: primary */ SELECT 1`** | **replica** | **primary** |
| **Startup `pgdog.role=replica`; `SET pgdog.role TO 'primary'; SELECT 1`** | **replica** | **primary** |
| **Startup `pgdog.role=replica`; `/* pgdog_role: auto */ SELECT 1`** | **replica** | **primary or replica** |

### 12. Replicas only, static roles, `query_parser = "on"`

```toml
[general]
query_parser = "on"
read_write_strategy = "conservative"
read_write_split = "include_primary"

[[databases]]
name = "pgdog"
host = "10.0.0.2"
role = "replica"
```

| Client sends | `main` | Branch |
|---|---|---|
| _No role_ | | |
| `SELECT 1` | replica | replica |
| **`UPDATE t SET id = 1`** | **`NoPrimary` error from PgDog** | **replica → error** |
| `BEGIN; SELECT 1` | replica | replica |
| `BEGIN READ ONLY; SELECT 1` | replica | replica |
| _`SET pgdog.role`_ | | |
| `SET pgdog.role TO 'replica'; SELECT 1` | replica | replica |
| **`SET pgdog.role TO 'replica'; UPDATE t SET id = 1`** | **`NoPrimary` error from PgDog** | **replica → error** |
| **`SET pgdog.role TO 'primary'; SELECT 1`** | **`NoPrimary` error from PgDog** | **replica** |
| `SET pgdog.role TO 'primary'; BEGIN READ ONLY; SELECT 1` | replica | replica |
| `SET pgdog.role TO 'auto'; SELECT 1` | replica | replica |
| **`SET pgdog.role TO 'auto'; UPDATE t SET id = 1`** | **`NoPrimary` error from PgDog** | **replica → error** |
| _`SET LOCAL pgdog.role`_ | | |
| `BEGIN; SET LOCAL pgdog.role TO 'replica'; SELECT 1` | replica | replica |
| **`BEGIN; SET LOCAL pgdog.role TO 'primary'; SELECT 1`** | **`NoPrimary` error from PgDog** | **replica** |
| `BEGIN; SET LOCAL pgdog.role TO 'auto'; SELECT 1` | replica | replica |
| **`BEGIN; SELECT 1; SET LOCAL pgdog.role TO 'replica'`** | **replica; the `SET` has no effect** | **replica; the `SET` returns an error** |
| _Comment `/* pgdog_role */`_ | | |
| `/* pgdog_role: replica */ SELECT 1` | replica | replica |
| **`/* pgdog_role: replica */ UPDATE t SET id = 1`** | **`NoPrimary` error from PgDog** | **replica → error** |
| Pipeline `Execute` × 2 + `Sync`, first `/* pgdog_role: replica */ SELECT 1` | replica | replica |
| `/* pgdog_role: primary */ SELECT 1` | replica | replica |
| `/* pgdog_role: auto */ SELECT 1` | replica | replica |
| _Startup `pgdog.role`_ | | |
| Startup `pgdog.role=replica`; `SELECT 1` | replica | replica |
| Startup `pgdog.role=replica`; `UPDATE t SET id = 1` | replica → error | replica → error |
| Startup `pgdog.role=replica`; `BEGIN; SELECT 1` | replica | replica |
| Startup `pgdog.role=primary`; `BEGIN READ ONLY; SELECT 1` | replica | replica |
| Startup `pgdog.role=auto`; `SELECT 1` | replica | replica |
| _Override: a higher source wins over a lower one_ | | |
| **`SET pgdog.role TO 'primary'; /* pgdog_role: replica */ SELECT 1`** | **`NoPrimary` error from PgDog** | **replica** |
| `SET pgdog.role TO 'replica'; /* pgdog_role: primary */ SELECT 1` | replica | replica |
| `SET pgdog.role TO 'replica'; /* pgdog_role: auto */ SELECT 1` | replica | replica |
| `SET pgdog.role TO 'primary'; BEGIN; SET LOCAL pgdog.role TO 'replica'; SELECT 1` | replica | replica |
| `BEGIN; SET LOCAL pgdog.role TO 'replica'; /* pgdog_role: primary */ SELECT 1` | replica | replica |
| Startup `pgdog.role=replica`; `/* pgdog_role: primary */ SELECT 1` | replica | replica |
| Startup `pgdog.role=replica`; `SET pgdog.role TO 'primary'; SELECT 1` | replica | replica |
| Startup `pgdog.role=replica`; `/* pgdog_role: auto */ SELECT 1` | replica | replica |

On the branch, the result is the same with every `query_parser` value. On `main`, it is not: section 13 shows the other values.

### 13. Replicas only, static roles, `query_parser = "auto"`

```toml
[general]
query_parser = "auto"
read_write_strategy = "conservative"
read_write_split = "include_primary"

[[databases]]
name = "pgdog"
host = "10.0.0.2"
role = "replica"
```

The cluster has no primary, so the parser runs only for `SET` and `BEGIN` on both versions. Other statements use the bypass, which sends them to the replica as reads. `query_parser = "session_control"` gives the same results. `query_parser = "off"` also gives the same results, except for the late `SET LOCAL` row: PgDog does not parse that `SET`, so the branch accepts it with no effect, like `main`.

| Client sends | `main` | Branch |
|---|---|---|
| _No role_ | | |
| `SELECT 1` | replica | replica |
| `UPDATE t SET id = 1` | replica → error | replica → error |
| `BEGIN; SELECT 1` | replica | replica |
| `BEGIN READ ONLY; SELECT 1` | replica | replica |
| _`SET pgdog.role`_ | | |
| `SET pgdog.role TO 'replica'; SELECT 1` | replica | replica |
| `SET pgdog.role TO 'replica'; UPDATE t SET id = 1` | replica → error | replica → error |
| `SET pgdog.role TO 'primary'; SELECT 1` | replica | replica |
| `SET pgdog.role TO 'primary'; BEGIN READ ONLY; SELECT 1` | replica | replica |
| `SET pgdog.role TO 'auto'; SELECT 1` | replica | replica |
| `SET pgdog.role TO 'auto'; UPDATE t SET id = 1` | replica → error | replica → error |
| _`SET LOCAL pgdog.role`_ | | |
| `BEGIN; SET LOCAL pgdog.role TO 'replica'; SELECT 1` | replica | replica |
| `BEGIN; SET LOCAL pgdog.role TO 'primary'; SELECT 1` | replica | replica |
| `BEGIN; SET LOCAL pgdog.role TO 'auto'; SELECT 1` | replica | replica |
| **`BEGIN; SELECT 1; SET LOCAL pgdog.role TO 'replica'`** | **replica; the `SET` has no effect** | **replica; the `SET` returns an error** |
| _Comment `/* pgdog_role */`_ | | |
| `/* pgdog_role: replica */ SELECT 1` | replica | replica |
| `/* pgdog_role: replica */ UPDATE t SET id = 1` | replica → error | replica → error |
| Pipeline `Execute` × 2 + `Sync`, first `/* pgdog_role: replica */ SELECT 1` | replica | replica |
| `/* pgdog_role: primary */ SELECT 1` | replica | replica |
| `/* pgdog_role: auto */ SELECT 1` | replica | replica |
| _Startup `pgdog.role`_ | | |
| Startup `pgdog.role=replica`; `SELECT 1` | replica | replica |
| Startup `pgdog.role=replica`; `UPDATE t SET id = 1` | replica → error | replica → error |
| Startup `pgdog.role=replica`; `BEGIN; SELECT 1` | replica | replica |
| Startup `pgdog.role=primary`; `BEGIN READ ONLY; SELECT 1` | replica | replica |
| Startup `pgdog.role=auto`; `SELECT 1` | replica | replica |
| _Override: a higher source wins over a lower one_ | | |
| `SET pgdog.role TO 'primary'; /* pgdog_role: replica */ SELECT 1` | replica | replica |
| `SET pgdog.role TO 'replica'; /* pgdog_role: primary */ SELECT 1` | replica | replica |
| `SET pgdog.role TO 'replica'; /* pgdog_role: auto */ SELECT 1` | replica | replica |
| `SET pgdog.role TO 'primary'; BEGIN; SET LOCAL pgdog.role TO 'replica'; SELECT 1` | replica | replica |
| `BEGIN; SET LOCAL pgdog.role TO 'replica'; /* pgdog_role: primary */ SELECT 1` | replica | replica |
| Startup `pgdog.role=replica`; `/* pgdog_role: primary */ SELECT 1` | replica | replica |
| Startup `pgdog.role=replica`; `SET pgdog.role TO 'primary'; SELECT 1` | replica | replica |
| Startup `pgdog.role=replica`; `/* pgdog_role: auto */ SELECT 1` | replica | replica |
