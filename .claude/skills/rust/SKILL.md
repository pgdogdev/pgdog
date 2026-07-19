---
name: rust
description: Rust coding best practices for idiomatic, efficient, and maintainable code. Use when writing Rust code, reviewing code, or learning Rust patterns.
allowed-tools: Read, Edit, Write, Bash, Grep, Glob, Task
---

# Rust Best Practices

Guidelines for writing idiomatic, efficient, and maintainable Rust code.

## Core Principles

1. **Leverage the type system** - Make invalid states unrepresentable
2. **Prefer compile-time checks** - Catch errors before runtime
3. **Be explicit about ownership** - Don't fight the borrow checker
4. **Write code that passes fmt/clippy first** - Not after fixing

## Code health

1. Prefer smaller files, refactor large files into multi-file modules
2. Refactor large modules into several
3. Prefer re-usable functions. Don't create separate functions if re-use is not planned.

## Error Handling

Use `thiserror`

```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Failed to read config: {0}")]
    Io(#[from] std::io::Error),

    #[error("Failed to parse config: {0}")]
    Parse(#[from] toml::de::Error),

    #[error("Invalid configuration: {message}")]
    Invalid { message: String },
}
```

### Never Use `.unwrap()`

```rust
// BAD
let value = map.get("key").unwrap();

// GOOD
let value = map.get("key").ok_or_else(|| Error::MissingKey("key"))?;

// GOOD (when None is truly impossible)
let value = map.get("key").expect("key always present after init");
```

## Ownership & Borrowing

### Prefer Borrowing Over Cloning

```rust
// BAD - unnecessary clone
fn process(data: String) { ... }
process(my_string.clone());

// GOOD - borrow when possible
fn process(data: &str) { ... }
process(&my_string);
```

### Use `Cow` for Flexible Ownership

```rust
use std::borrow::Cow;

fn process(data: Cow<'_, str>) -> Cow<'_, str> {
    if data.contains("bad") {
        Cow::Owned(data.replace("bad", "good"))
    } else {
        data  // No allocation if unchanged
    }
}
```

### Return Owned Data from Constructors

```rust
// GOOD - clear ownership
impl User {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}
```

## API Design

### Builder Pattern for Complex Configuration

```rust
#[derive(Default)]
pub struct ServerBuilder {
    host: Option<String>,
    port: Option<u16>,
    timeout: Option<Duration>,
}

impl ServerBuilder {
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    pub fn build(self) -> Result<Server, ConfigError> {
        Ok(Server {
            host: self.host.unwrap_or_else(|| "localhost".into()),
            port: self.port.ok_or(ConfigError::MissingPort)?,
            timeout: self.timeout.unwrap_or(Duration::from_secs(30)),
        })
    }
}
```

### Newtype Pattern for Type Safety

```rust
// BAD - easy to mix up
fn transfer(from: i64, to: i64, amount: i64) { ... }

// GOOD - compile-time safety
pub struct AccountId(i64);
pub struct Amount(i64);

fn transfer(from: AccountId, to: AccountId, amount: Amount) { ... }
```

### Use `#[must_use]` for Important Returns

```rust
#[must_use]
pub fn validate(&self) -> Result<(), ValidationError> {
    // ...
}
```

## Collections & Iterators

### Prefer Iterators Over Loops

```rust
// BAD
let mut results = Vec::new();
for item in items {
    if item.is_valid() {
        results.push(item.transform());
    }
}

// GOOD
let results: Vec<_> = items
    .into_iter()
    .filter(|item| item.is_valid())
    .map(|item| item.transform())
    .collect();
```

### Use `collect()` Type Inference

```rust
// Collect into Vec
let vec: Vec<_> = iter.collect();

// Collect into HashMap
let map: HashMap<_, _> = iter.collect();

// Collect Results
let results: Result<Vec<_>, _> = iter.collect();
```

## Async Patterns

### Use `tokio` for Async Runtime

```rust
#[tokio::main]
async fn main() -> Result<()> {
    let result = fetch_data().await?;
    Ok(())
}
```

### Avoid Blocking in Async Code

```rust
// BAD - blocks the runtime
async fn bad() {
    std::thread::sleep(Duration::from_secs(1));
}

// GOOD - async sleep
async fn good() {
    tokio::time::sleep(Duration::from_secs(1)).await;
}

// GOOD - spawn blocking for CPU-intensive work
async fn compute() -> i32 {
    tokio::task::spawn_blocking(|| expensive_computation()).await.unwrap()
}
```

## Testing

### Unit Tests in Same File

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() {
        assert_eq!(add(1, 2), 3);
    }

    #[test]
    fn test_edge_case() {
        assert!(validate("").is_err());
    }
}
```

### Integration Tests in `tests/`

```rust
// tests/integration_test.rs
use my_crate::public_api;

#[test]
fn test_full_workflow() {
    let result = public_api::process("input");
    assert!(result.is_ok());
}
```

### Use `assert!` Macros Effectively

```rust
assert!(condition);
assert_eq!(left, right);
assert_ne!(left, right);
assert!(result.is_ok());
assert!(result.is_err());
assert_matches!(value, Pattern::Variant { .. });
```

## Performance

### Avoid Premature Allocation

```rust
// BAD - allocates even if not needed
fn maybe_string() -> String {
    String::from("default")
}

// GOOD - return static str when possible
fn maybe_string() -> &'static str {
    "default"
}
```

### Use `Vec::with_capacity` for Known Sizes

```rust
// BAD - multiple reallocations
let mut vec = Vec::new();
for i in 0..1000 {
    vec.push(i);
}

// GOOD - single allocation
let mut vec = Vec::with_capacity(1000);
for i in 0..1000 {
    vec.push(i);
}
```

### Import instead of using absolute paths

```rust
/// BAD
let v = tokio::net::TcpStream::connect("localhost:8080");

/// GOOD
use tokio::net::TcpStream;
let v = TcpStream::connect("localhost:8080");
```

### Profile Before Optimizing

```bash
cargo build --release
cargo flamegraph  # requires cargo-flamegraph
```

## Module Organization

### Keep Modules Focused

```rust
// src/lib.rs
pub mod config;
pub mod client;
pub mod error;

// Re-export public API
pub use config::Config;
pub use client::Client;
pub use error::Error;
```

### Use `pub(crate)` for Internal APIs

```rust
// Public to crate, not external users
pub(crate) fn internal_helper() { ... }
```

## Documentation

### Document Public APIs

```rust
/// Creates a new client with the given configuration.
///
/// # Arguments
///
/// * `config` - The client configuration
///
/// # Errors
///
/// Returns an error if the configuration is invalid.
///
/// # Examples
///
/// ```
/// let client = Client::new(Config::default())?;
/// ```
pub fn new(config: Config) -> Result<Self> {
    // ...
}
```

## Anti-Patterns to Avoid

| Anti-Pattern | Better Approach |
|--------------|-----------------|
| `.unwrap()` everywhere | Use `?` operator |
| `clone()` to satisfy borrow checker | Restructure ownership |
| `String` parameters | Use `&str` or `impl Into<String>` |
| Boolean parameters | Use enums |
| Long function bodies | Extract to smaller functions |
| Deep nesting | Use early returns |
| Magic numbers | Use named constants |

## Quick Reference

```bash
# Quality gates
cargo fmt -- --check && cargo clippy -- -D warnings && cargo test

# Common cargo commands
cargo check           # Fast syntax/type check
cargo build           # Debug build
cargo build --release # Release build
cargo nextest run     # Run tests
cargo doc --open      # Generate and view docs
cargo clippy --fix    # Auto-fix lint issues
```
