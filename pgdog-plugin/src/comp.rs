//! Compatibility checks.

use crate::PdStr;

/// Rust compiler version used to build this library.
pub fn rustc_version() -> PdStr {
    env!("RUSTC_VERSION").into()
}

/// pg_query version detected at build time.
pub fn pg_query_version() -> PdStr {
    option_env!("PGDOG_PGQUERY_VERSION")
        .map(|p| p.into())
        .unwrap_or_default()
}
