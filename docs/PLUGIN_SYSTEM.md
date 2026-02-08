# PgDog Plugin System - Architecture & Execution Workflow

This document provides a comprehensive overview of PgDog's plugin system, including its architecture, execution flow, integration points, and development guidelines.

## Overview

PgDog's plugin system enables dynamic, runtime customization of query routing behavior through Rust-based plugins compiled as shared libraries. The system uses FFI (Foreign Function Interface) to safely communicate between PgDog and plugins while maintaining strict version compatibility guarantees.

## Architecture

### Core Components

PgDog's plugin system is built on four main crates:

#### 1. **pgdog-plugin** - Plugin Interface Bridge
- **Location**: `pgdog-plugin/`
- **Role**: Main user-facing plugin library
- **Key Modules**:
  - `plugin.rs` - Plugin lifecycle and FFI symbol loading via `libloading`
  - `context.rs` - `Context` and `Route` types for plugin I/O
  - `parameters.rs` - Prepared statement parameter handling
  - `bindings.rs` - Auto-generated C ABI bindings via bindgen
  - `prelude.rs` - Common imports (macros, types, functions)

**Key Responsibilities**:
- Provides safe Rust interfaces for plugin development
- Manages FFI symbol resolution and execution
- Performs automatic version compatibility checks
- Re-exports `pg_query` with guaranteed version matching

#### 2. **pgdog-macros** - Plugin Code Generation
- **Location**: `pgdog-macros/src/lib.rs`
- **Purpose**: Procedural macros that auto-generate required FFI functions

**Exported Macros**:
```rust
macros::plugin!()
// Generates three required FFI functions:
// - pgdog_rustc_version() → returns compiler version used
// - pgdog_pg_query_version() → returns pg_query version
// - pgdog_plugin_version() → returns plugin version from Cargo.toml

#[macros::init]
// Wraps user function into pgdog_init() FFI function
// Executed once at plugin load time (synchronous)

#[macros::route]
// Wraps user function into pgdog_route() FFI function
// Executed for every query (can run on any thread)

#[macros::fini]
// Wraps user function into pgdog_fini() FFI function
// Executed once at PgDog shutdown (synchronous)
```

#### 3. **pgdog-plugin-build** - Build-Time Helpers
- **Location**: `pgdog-plugin-build/src/lib.rs`
- **Role**: Provides `pg_query_version()` function for plugin build scripts
- **Purpose**: Extracts exact `pg_query` version from plugin's `Cargo.toml` and sets `PGDOG_PGQUERY_VERSION` environment variable, ensuring PgDog and plugin use identical versions

#### 4. **pgdog** - Plugin Runtime Manager
- **Location**: `pgdog/src/plugin/mod.rs`
- **Role**: Main plugin loader and lifecycle manager
- **Key Functions**:
  - `load(names: &[&str])` - Load plugins and perform version checks
  - `plugins()` - Retrieve loaded plugins
  - `shutdown()` - Call plugin cleanup routines

## Execution Workflow

### Phase 1: Startup & Plugin Loading

```
PgDog starts
    ↓
Read configuration (pgdog.toml)
    ↓
load_from_config()
    ├─ Extract plugin names from config.plugins[]
    └─ Call load()
        ↓
        For each plugin:
          ├─ Plugin::library(name)
          │  └─ Use libloading to dlopen() shared library
          │
          ├─ Plugin::load(name, &lib)
          │  ├─ Resolve pgdog_init symbol
          │  ├─ Resolve pgdog_fini symbol
          │  ├─ Resolve pgdog_route symbol
          │  ├─ Resolve pgdog_rustc_version symbol
          │  └─ Resolve pgdog_plugin_version symbol
          │
          ├─ VERSION CHECKS
          │  ├─ Get PgDog's rustc version via comp::rustc_version()
          │  ├─ Get plugin's rustc version via pgdog_rustc_version()
          │  ├─ If mismatch: WARN and SKIP plugin
          │  └─ If match: Continue
          │
          ├─ plugin.init()
          │  └─ Call pgdog_init() if defined (synchronous)
          │
          └─ Log: "loaded {name} plugin (v{version}) [timing]ms"

Store plugins in static PLUGINS (OnceCell)
    ↓
PgDog ready to serve connections
```

**Location**: `pgdog/src/plugin/mod.rs` lines 15-85

### Phase 2: Query Routing (Per Query)

```
Client sends query
    ↓
PostgreSQL Frontend Parser
    ├─ Tokenize and parse query
    └─ Generate pg_query AST
       ↓
Query Router: QueryParser::parse()
    ├─ Create RouterContext (shards, replicas, etc.)
    ├─ Generate PdRouterContext for plugins
    │  └─ context.plugin_context(ast, bind_params)
    │     ├─ Extract AST from pg_query ParseResult
    │     ├─ Pack bind parameters into PdParameters
    │     └─ Include cluster metadata (shards, replicas, transaction state)
    │
    ├─ Call QueryParser::plugins()
    │  └─ For each loaded plugin:
    │     ├─ plugin.route(PdRouterContext)
    │     │  └─ Call pgdog_route(context, &mut output)
    │     │
    │     ├─ Parse returned PdRoute:
    │     │  ├─ Shard::Direct(n) → override shard to n
    │     │  ├─ Shard::All → broadcast to all shards
    │     │  ├─ Shard::Unknown → use default routing
    │     │  ├─ Shard::Blocked → block query
    │     │  ├─ ReadWrite::Read → send to replica
    │     │  ├─ ReadWrite::Write → send to primary
    │     │  └─ ReadWrite::Unknown → use default logic
    │     │
    │     ├─ Record plugin override (if any)
    │     └─ If route provided: BREAK (first plugin wins)
    │
    ├─ Apply plugin overrides to routing decision
    └─ Continue with default routing logic
       ↓
Select target shard/replica
    ↓
Execute on database
```

**Locations**:
- Context creation: `pgdog/src/frontend/router/parser/context.rs` lines 107-140
- Plugin execution: `pgdog/src/frontend/router/parser/query/plugins.rs` lines 20-105

### Phase 3: Shutdown

```
PgDog receives shutdown signal
    ↓
Call plugin::shutdown()
    └─ For each loaded plugin:
       ├─ plugin.fini()
       │  └─ Call pgdog_fini() if defined (synchronous)
       └─ Log: "plugin {name} shutdown"
    ↓
Clean up remaining resources
    ↓
Process exits
```

**Location**: `pgdog/src/plugin/mod.rs` lines 77-83

## Plugin Development Workflow

### Step 1: Create Plugin Project

```bash
cargo init --lib my_plugin
cd my_plugin
```

### Step 2: Configure Cargo.toml

```toml
[package]
name = "my_plugin"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["rlib", "cdylib"]  # Critical: enables C ABI shared library

[dependencies]
pgdog-plugin = "0.1.8"

[build-dependencies]
pgdog-plugin-build = "0.1.0"
```

### Step 3: Create build.rs

```rust
// build.rs
fn main() {
    pgdog_plugin_build::pg_query_version();
}
```

This extracts the exact `pg_query` version from your `Cargo.toml` and sets it as environment variable for runtime version checking.

### Step 4: Implement Plugin Functions

```rust
// src/lib.rs
use pgdog_plugin::prelude::*;
use pgdog_macros as macros;

// Generate required FFI functions
macros::plugin!();

// Called once at plugin load time (blocking)
#[macros::init]
fn init() {
    // Initialize state, load config, set up synchronization, etc.
    println!("Plugin initialized");
}

// Called for every query (non-blocking, runs on async executor)
#[macros::route]
fn route(context: Context) -> Route {
    // Access query information:
    let ast = context.statement().protobuf();       // pg_query AST
    let params = context.parameters();               // Prepared statement params
    let shards = context.shards();                   // Number of shards
    let has_replicas = context.has_replicas();      // Cluster has replicas?
    let in_transaction = context.in_transaction(); // Connection in transaction?

    // Implement custom routing logic

    // Return routing decision
    Route::new(Shard::Unknown, ReadWrite::Unknown)  // or any other shard/ro combo
}

// Called once at PgDog shutdown (blocking)
#[macros::fini]
fn shutdown() {
    // Cleanup, flush stats, close connections, etc.
    println!("Plugin shutting down");
}
```

### Step 5: Build Plugin

```bash
# Development
cargo build

# Release (recommended for production)
cargo build --release
```

Build output:
- **Debug**: `target/debug/libmy_plugin.so` (Linux), `.dylib` (macOS)
- **Release**: `target/release/libmy_plugin.so`

### Step 6: Deploy Plugin

Choose one of three methods:

**Method 1: System Library Path**
```bash
cp target/release/libmy_plugin.so /usr/lib/
```

**Method 2: Environment Variable**
```bash
export LD_LIBRARY_PATH=/path/to/plugin/target/release:$LD_LIBRARY_PATH
pgdog --config pgdog.toml
```

**Method 3: pgdog.toml Configuration**
```toml
[[plugins]]
name = "my_plugin"                           # If in system/LD_LIBRARY_PATH
# or
name = "libmy_plugin.so"                    # Relative path
# or
name = "/usr/lib/libmy_plugin.so"          # Absolute path
```

### Step 7: Verify Plugin Loading

Check PgDog startup logs:

```
INFO: 🐕 PgDog v0.1.29 (rustc 1.89.0)
INFO: loaded "my_plugin" plugin (v0.1.0) [2.3456ms]
```

## Integration Points

### Router Context Creation

**File**: `pgdog/src/frontend/router/parser/context.rs` lines 107-140

When a query arrives, the router creates a `PdRouterContext` for plugins:

```rust
pub struct PdRouterContext {
    shards: u64,                    // Number of shards in cluster
    has_replicas: u64,              // Cluster has replicas (0 or 1)
    has_primary: u64,               // Cluster has primary (0 or 1)
    in_transaction: u64,            // Connection in transaction (0 or 1)
    query: PdStatement,             // Parsed AST from pg_query
    write_override: u64,            // Write intent (0=read, 1=write)
    params: PdParameters,           // Bind parameters (if prepared statement)
}
```

The AST is the pg_query protobuf structure:

```rust
pub struct ParseResult {
    pub version: u32,
    pub stmts: Vec<RawStmt>,  // Array of statements
}

pub struct RawStmt {
    pub stmt: Option<Node>,
}

pub enum NodeEnum {
    SelectStmt(...),
    InsertStmt(...),
    UpdateStmt(...),
    DeleteStmt(...),
    // ... many more SQL statement types
}
```

### Plugin Execution Point

**File**: `pgdog/src/frontend/router/parser/query/plugins.rs` lines 20-105

The query parser invokes plugins after parsing but before applying default routing logic:

```rust
pub(super) fn plugins(
    &mut self,
    context: &QueryParserContext,
    statement: &Ast,
    read: bool,
) -> Result<(), Error> {
    // Skip if no plugins loaded
    let plugins = if let Some(plugins) = crate::plugin::plugins() {
        plugins
    } else {
        return Ok(());
    };

    // Create context for plugins
    let mut context = context.plugin_context(
        &statement.parse_result().protobuf,
        &context.router_context.bind,
    );

    // Try each plugin in order
    for plugin in plugins {
        if let Some(route) = plugin.route(context) {
            // First plugin to return a route wins
            // Parse and apply the route
            self.plugin_output.shard = Some(Shard::Direct(route.shard));
            self.plugin_output.read = Some(route.read_write);
            break;  // Stop processing plugins
        }
    }

    Ok(())
}
```

### Configuration Loading

**File**: `pgdog-config/src/users.rs` lines 1-20

Plugin names are loaded from `pgdog.toml`:

```rust
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Plugin {
    pub name: String,  // Plugin library name or path
}
```

The `name` field can be:
- Library name (will search system paths + `LD_LIBRARY_PATH`)
- Relative path to `.so`/`.dylib`
- Absolute path to shared library

## Safety & Compatibility

### Rust Compiler Version Checking

PgDog enforces that plugins are compiled with the exact same Rust compiler version as PgDog itself.

**How it works**:

1. At PgDog build time:
   - `pgdog-plugin` calls `comp::rustc_version()` which uses `rustc --version`
   - This version is embedded via the `plugin!()` macro

2. At plugin build time:
   - The `plugin!()` macro generates `pgdog_rustc_version()` function
   - This function returns the compiler version used to build the plugin

3. At plugin load time:
   - PgDog calls both functions and compares versions
   - If mismatch: **plugin is skipped** with warning
   - If match: plugin is loaded and initialized

**Why**: Rust doesn't have a stable ABI. Memory layouts of types can change between compiler versions. Using mismatched versions could lead to undefined behavior.

### pg_query Version Matching

Since plugins receive `pg_query` AST types directly via FFI, they must use the exact same version as PgDog.

**How it works**:

1. In plugin's `build.rs`:
   - `pgdog_plugin_build::pg_query_version()` reads `Cargo.toml`
   - Extracts version constraint on `pg_query` (e.g., `"6.1.0"`)
   - Sets `PGDOG_PGQUERY_VERSION` environment variable

2. In plugin's `src/lib.rs`:
   - The `plugin!()` macro generates `pgdog_pg_query_version()` function
   - This function returns the version from environment variable

3. At plugin load time:
   - PgDog can verify version match (currently informational)

**Best Practice**: Always use the `pgdog-plugin` re-exports:

```rust
// Good: uses pgdog-plugin's pg_query version
use pgdog_plugin::prelude::*;  // includes pg_query
use pgdog_plugin::pg_query;

// Bad: independently using pg_query
use pg_query;  // Version mismatch risk!
```

### FFI Safety

The `pgdog-plugin` crate handles FFI safety by:

1. **Wrapping plugin context**: `PdRouterContext` → `Context`
   - Only exposes safe methods that validate input

2. **Wrapping plugin output**: `Route` → `PdRoute`
   - Converts Rust types to C-compatible structures

3. **Symbol resolution**: Uses `libloading::Symbol` with correct type signatures
   - Type mismatch = load failure (not UB)

4. **Lifetime management**: FFI pointers are valid only during function call
   - Context is created on stack, passed by reference, cleaned up after

## Advanced Topics

### Variables Available in Plugin Context

From `Context` struct:

```rust
impl Context {
    /// Number of configured shards
    pub fn shards(&self) -> u64

    /// Does cluster have readable replicas?
    pub fn has_replicas(&self) -> bool

    /// Does cluster have writable primary?
    pub fn has_primary(&self) -> bool

    /// Is this connection in a transaction?
    pub fn in_transaction(&self) -> bool

    /// Parsed query AST from pg_query
    pub fn statement(&self) -> Statement

    /// Prepared statement parameters (if any)
    pub fn parameters(&self) -> Parameters

    /// Force query as READ even if normally a write
    pub fn write_override(&self) -> bool
}
```

### Plugin Output Options

```rust
impl Route {
    /// No routing decision
    pub fn unknown() -> Self

    /// Block query from executing
    pub fn block() -> Self

    /// Direct to specific shard
    pub fn direct(shard: u64) -> Self

    /// Broadcast to all shards
    pub fn all() -> Self

    /// Specify shard and read/write
    pub fn new(shard: Shard, read_write: ReadWrite) -> Self
}

pub enum Shard {
    Direct(u64),    // Specific shard index
    All,            // All shards
    Unknown,        // Use default routing
    Blocked,        // Block query
}

pub enum ReadWrite {
    Read,           // Send to replica
    Write,          // Send to primary
    Unknown,        // Use default logic
}
```

### Accessing Query Parameters

For prepared statements, access bind parameters:

```rust
#[macros::route]
fn route(context: Context) -> Route {
    let params = context.parameters();

    // Get count of parameters
    let count = params.count();

    // Get specific parameter by index (0-based)
    if let Some(param) = params.get(0) {
        // Decode based on format
        if let Some(value) = param.decode(params.parameter_format(0)) {
            match value {
                ParameterValue::Text(s) => println!("String: {}", s),
                ParameterValue::Int(i) => println!("Int: {}", i),
                ParameterValue::Query(q) => println!("Query: {}", q),
                // ... other types
            }
        }
    }

    Route::unknown()
}
```

### Query Blocking Example

Block queries that don't follow security requirements:

```rust
#[macros::route]
fn route(context: Context) -> Route {
    let ast = context.statement().protobuf();

    // Block INSERT without explicit column list
    if let Some(raw_stmt) = ast.stmts.first() {
        if let Some(stmt) = &raw_stmt.stmt {
            if let Some(NodeEnum::InsertStmt(insert)) = &stmt.node {
                if insert.cols.is_empty() {
                    return Route::block();  // Block it!
                }
            }
        }
    }

    Route::unknown()
}
```

### Logging from Plugins

Use `eprintln!()` or standard Rust logging (routes to stderr):

```rust
#[macros::route]
fn route(context: Context) -> Route {
    eprintln!("Processing query with {} params", context.parameters().count());

    Route::unknown()
}
```

## Performance Considerations

1. **Plugin initialization is synchronous**: Keep `init()` fast or it blocks PgDog startup
2. **Plugin routes run on async executor**: Can do async work, but not thread-blocking operations
3. **First-match semantics**: Plugins are checked in order; earlier plugins take precedence
4. **No error handling**: Plugin functions cannot return errors or panic
   - Must return default `Route::unknown()` in error cases
5. **AST parsing is done before plugins**: Plugins can't change query parsing, only routing

## Debugging Plugins

### Check compiler version match:

```bash
# PgDog shows at startup:
# INFO: 🐕 PgDog v0.1.29 (rustc 1.89.0 (29483883e 2025-08-04))

# Update your plugin's Rust:
rustup update
rustc --version
# rustc 1.89.0 (29483883e 2025-08-04)

# Rebuild plugin:
cargo clean && cargo build --release
```

### Check plugin loads:

```bash
# In PgDog logs, should see:
INFO: loaded "my_plugin" plugin (v0.1.0) [X.XXXXms]

# If plugin is skipped:
WARN: skipping plugin "my_plugin" because it was compiled with different compiler version
WARN: skipping plugin "my_plugin" because it doesn't expose its Rust compiler version
```

### Library not found:

```bash
# Verify library exists:
ldd /usr/lib/libmy_plugin.so  # Linux
otool -L /usr/local/lib/libmy_plugin.dylib  # macOS

# Check LD_LIBRARY_PATH:
echo $LD_LIBRARY_PATH

# Verify symbols:
nm -D /usr/lib/libmy_plugin.so | grep pgdog_
```

## Example: Tenant-Based Routing Plugin

```rust
use pgdog_plugin::prelude::*;
use pgdog_macros as macros;

macros::plugin!();

#[macros::init]
fn init() {
    eprintln!("Tenant routing plugin initialized");
}

#[macros::route]
fn route(context: Context) -> Route {
    let params = context.parameters();

    // Extract tenant_id from function parameter
    if let Some(param) = params.get(0) {
        if let Some(ParameterValue::Int(tenant_id)) = param.decode(ParameterFormat::Text) {
            // Route to shard based on tenant_id
            // Assuming simple hash modulo sharding
            let shard = (tenant_id as u64) % context.shards();
            return Route::direct(shard);
        }
    }

    // No tenant_id found, use default routing
    Route::unknown()
}

#[macros::fini]
fn shutdown() {
    eprintln!("Tenant routing plugin shutting down");
}
```

## Testing

### Integration Tests

**Location**: [integration/plugins/](../integration/plugins/)

**Main test suite**: [integration/plugins/extended_spec.rb](../integration/plugins/extended_spec.rb)

The integration test validates the FFI parameter passing mechanism by:
- Executing parameterized queries (`SELECT ... WHERE col = $1`) through PgDog with the plugin loaded
- Testing 10 separate connections, each executing 25 prepared statements with different parameter values
- Verifying that parameter values correctly pass through the FFI boundary to the plugin
- Ensuring query results match the input parameters

**What it validates**:
- Prepared statement parameters pass correctly through FFI
- Plugin receives and can decode parameter values
- Extended protocol (parameterized queries) works with plugins
- Multiple connections and queries function correctly
- Query execution continues despite plugin intervention
- The example plugin's parameter logging functionality

**Configuration**: [integration/plugins/pgdog.toml](../integration/plugins/pgdog.toml) configures PgDog to load the example plugin

**How to run**:
```bash
cd integration/plugins
bash run.sh
```

**Note**: This test is NOT part of the default integration test suite ([integration/run.sh](../integration/run.sh)).

### Unit Tests

#### Plugin Data Structures

**Location**: [pgdog-plugin/src/](../pgdog-plugin/src/)

**Test modules**:
- [parameters.rs#L238](../pgdog-plugin/src/parameters.rs#L238) - Tests parameter FFI structure creation and empty parameter handling
- [ast.rs#L108](../pgdog-plugin/src/ast.rs#L108) - Tests AST FFI conversion by parsing SQL and verifying the protobuf structure is correctly accessible through FFI
- [string.rs#L69](../pgdog-plugin/src/string.rs#L69) - Tests PdStr conversions from both `&str` and `String` types

**Testing approach**: These unit tests validate that FFI data structures correctly wrap and expose Rust types without memory corruption or lifetime issues.

**How to run**:
```bash
cd pgdog-plugin
cargo test
```

#### Example Plugin Tests

**Location**: [plugins/pgdog-example-plugin/src/plugin.rs#L104-136](../plugins/pgdog-example-plugin/src/plugin.rs#L104-136)

**Test approach**:
- Manually constructs a `PdRouterContext` with a parsed `SELECT` query
- Calls the plugin's `route_query()` function directly
- Verifies the routing decision matches expected behavior (Read route for SELECT on replica-enabled cluster)

This demonstrates how to unit test plugin routing logic without running the full PgDog proxy.

### Test Coverage Gaps

The following areas lack test coverage:

#### Plugin Loading & Lifecycle
- ❌ Rust compiler version mismatch scenarios
- ❌ pg_query version verification (currently not implemented)
- ❌ Plugin symbol resolution failures
- ❌ Plugin with missing required functions
- ❌ Plugin init/fini execution order
- ❌ Plugin initialization failures
- ❌ Library loading errors (bad path, missing dependencies)

#### Routing Behavior
- ❌ `Route::direct(n)` - routing to specific shard
- ❌ `Route::all()` - broadcasting to all shards
- ❌ `Route::block()` - blocking query execution
- ❌ Multiple plugins with precedence rules
- ❌ Plugin routing overrides (shard + read/write combinations)
- ❌ Read/write routing decisions

#### Error Handling
- ❌ Plugin panics (should not crash PgDog)
- ❌ Invalid route values
- ❌ Malformed plugin libraries
- ❌ ABI violations

#### Concurrency
- ❌ Concurrent route() calls from multiple threads
- ❌ Thread safety of plugin state
- ❌ Race conditions in plugin initialization


## Related Documentation

- [PgDog Plugin Documentation](https://docs.pgdog.dev/features/plugins/)
- [pgdog-plugin Rust Docs](https://docsrs.pgdog.dev/pgdog_plugin/)
- [pg_query Rust Docs](https://docsrs.pgdog.dev/pg_query/)
- [Query Router Configuration](https://docs.pgdog.dev/configuration/pgdog.toml/plugins/)
