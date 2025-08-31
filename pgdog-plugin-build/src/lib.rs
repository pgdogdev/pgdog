//! Build-time helpers for PgDog plugins.
//!
//! Include this package as a build dependency only.
//!

use std::{fs::File, io::Read};

/// Extracts the `pg_query` crate version from `Cargo.toml`
/// and sets it as an environment variable.
///
/// This should be used at build time only. It expects `Cargo.toml` to be present in the same
/// folder as `build.rs`.
///
/// ### Note
///
/// You should have a strict version constraint on `pg_query`, for example:
///
/// ```toml
/// pg_query = "6.1.0"
/// ```
///
/// If the version in your plugin doesn't match what PgDog is using, your plugin won't be loaded.
///
pub fn pg_query_version() {
    let mut contents = String::new();
    if let Ok(mut file) = File::open("Cargo.toml") {
        file.read_to_string(&mut contents).ok();
    } else {
        panic!("Cargo.toml not found");
    }

    let contents: Option<toml::Value> = toml::from_str(&contents).ok();
    if let Some(contents) = contents
        && let Some(dependencies) = contents.get("dependencies")
            && let Some(pg_query) = dependencies.get("pg_query")
                && let Some(version) = pg_query.as_str() {
                    println!("cargo:rustc-env=PGDOG_PGQUERY_VERSION={}", version);
                }
}
