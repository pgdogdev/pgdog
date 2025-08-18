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
//! and make statement routing decisions.
//!
//! The AST is computed by PgDog at runtime. It then passes it down to plugins, using a FFI interface. To make this safe, plugins must follow the
//! following 2 requirements:
//!
//! 1. Plugins must be compiled with the **same version of the Rust compiler** as PgDog. This is automatically checked at runtime and plugins that don't do this are not loaded.
//! 2. Plugins must use the **same version of [`pg_query`] crate** as PgDog. This is checked at runtime but requires a build-time script executed by the plugin.
//!
//! #### Configure dependencies
//!
//! Add the following to your plugin's `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! pg_query = "6.1.0"
//! pgdog-plugin = "0.1.4"
//!
//! [build-dependencies]
//! pgdog-plugin-build = "0.1"
//! ```
//!
//! #### Build script
//!
//! In the same directory as `Cargo.toml`, add the `build.rs` file and with the following code:
//!
//! ```ignore
//! fn main() {
//!     pgdog_plugin_build::pg_query_version();
//! }
//! ```
//!
//!
//! # Required methods
//!
//! All plugins need to implement a set of functions that PgDog calls at runtime to load the plugin. You can implement them automatically
//! using a macro. Inside the plugin's `src/lib.rs` file, add the following code:
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
//! Plugins are most commonly used to route queries. To do this, they need to implement a function that reads
//! the [`Context`] passed in by PgDog, and returns a [`Route`] that indicates which database the query should be sent to.
//!
//! ### Example
//!
//! ```
//! use pgdog_plugin::{macros, Context, Route};
//!
//! #[macros::route]
//! fn route(context: Context) -> Route {
//!     Route::default()
//! }
//! ```
//!
//! The [`macros::route`] macro wraps the function into a safe FFI interface which PgDog calls at runtime.
//!
//! ### Errors
//!
//! Plugin functions cannot return errors or panic. To handle errors, you can log them to `stderr` and return a default route,
//! which PgDog will ignore. Plugins currently cannot be used to block queries.
//!
//! # Enabling plugins
//!
//! Plugins are shared libraries, loaded by PgDog at runtime using `dlopen(3)`. If specifying only its name, make sure to place the plugin's shared library
//! into one of the following locations:
//!
//! - Any of the system default paths, e.g.: `/lib`, `/usr/lib`, `/lib64`, `/usr/lib64`, etc.
//! - Path specified by the `LD_LIBRARY_PATH` (on Linux) or `DYLD_LIBRARY_PATH` (Mac OS) environment variables.
//!
//! PgDog doesn't load plugins automatically. For each plugin you'd like to use, add it to `pgdog.toml`:
//!
//! ```toml
//! # Plugin should be in /usr/lib or in LD_LIBRARY_PATH.
//! [[plugins]]
//! name = "my_plugin"
//!
//! # Plugin should be in $PWD/libmy_plugin.so
//! [[plugins]]
//! name = "libmy_plugin.so"
//!
//! # Absolute path to the plugin.
//! [[plugins]]
//! name = "/usr/local/lib/libmy_plugin.so"
//! ```
//!
//! If only specifying the name of the plugin, just like with compilers, omit the `lib` prefix and the platform-specific extension (e.g.: `.so` or `.dylib`).

/// Bindgen-generated FFI bindings.
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
