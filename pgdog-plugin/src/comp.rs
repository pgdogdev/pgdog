//! Compatibility checks.

use crate::PdStr;

/// Rust compiler version used to build this library.
pub fn rustc_version() -> PdStr {
    env!("RUSTC_VERSION").into()
}

/// Version of pgdog-plugin itself.
pub fn plugin_lib_version() -> PdStr {
    env!("CARGO_PKG_VERSION").into()
}
