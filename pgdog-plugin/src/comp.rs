//! Compatibility checks.

use crate::PdStr;

/// Rust compiler version used to build this library.
pub fn rustc_version() -> PdStr {
    env!("RUSTC_VERSION").into()
}
