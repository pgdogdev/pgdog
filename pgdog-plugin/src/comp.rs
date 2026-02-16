//! Compatibility checks.

use crate::PdStr;

/// Rust compiler version used to build this library.
pub fn rustc_version() -> PdStr {
    env!("RUSTC_VERSION").into()
}

/// pgdog-plugin version currently used
pub fn pgdog_plugin_api_version() -> PdStr {
    env!("CARGO_PKG_VERSION").into()
}
