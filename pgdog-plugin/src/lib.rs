//! PgDog plugins library.
//!
//! Implements data types and methods plugins can use to interact with PgDog at runtime.
//!
//! # Getting started
//!
//! Create a Rust library package with Cargo:
//!
//! ```bash
//! cargo init --lib my_plugin
//! ```
//!
//! The plugin needs to be built as a C ABI-compatible shared library. Add the following to Cargo.toml in the new plugin directory:
//!
//! ```toml
//! [lib]
//! crate-type = ["rlib", "cdylib"]
//! ```
//!
//! ## Dependencies
//!
//! PgDog is using [`pg_query`] to parse SQL. It produces an Abstract Syntax Tree (AST) which plugins can use to inspect queries
//! and make routing decisions.
//!
//! PgDog is responsible for parsing SQL. It passes the AST down to plugins using an FFI interface. To make this safe, plugins must implement the
//! following two requirements:
//!
//! 1. Plugins must be built with the same version of the Rust compiler as PgDog. This is automatically checked at runtime and plugins that don't
//! follow this requirement are not loaded.
//! 2. Plugins must use the same version of [`pg_query`] crate as PgDog. This is checked at runtime but requires a build-time script
//! executed by the plugin.
//!
//! #### Cargo.toml
//!
//! Add the following to the plugin's Cargo.toml:
//!
//! ```toml
//! [dependencies]
//! pg_query = "6.1.0"
//!
//! [build-dependencies]
//! pgdog-plugin-build = "0.1"
//! ```
//!
//! #### Build script
//!
//! In the same directory as Cargo.toml, create the `build.rs` file and add the following code:
//!
//! ```no_run
//! fn main {
//!     pgdog_plugin_build::pg_query_version();
//! }
//! ```
//!
//!
//! # Required methods
//!
//! All plugins need to implement required functions that are called at runtime to load it. You can implement them automatically
//! using a macro. Inside `src/lib.rs`, add the following:
//!
//! ```
//! // src/lib.rs
//! use pgdog_plugin::macros;
//!
//! macros::plugin!();
//! ```
//!
//! # Routing queries
//!
//! Plugins can be used to route queries. To do this, they need to implement a function that reads
//! the [`Context`] passed in by PgDog and returns a [`Route`] indicating which database the query should go to.
//!

#[allow(non_upper_case_globals)]
#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
pub mod bindings;

pub mod ast;
pub mod comp;
pub mod context;
pub mod plugin;
pub mod string;

pub use bindings::*;
pub use context::*;
pub use plugin::*;

pub use libloading;

pub use pgdog_macros as macros;
