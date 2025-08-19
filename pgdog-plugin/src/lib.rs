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
//! 2. Plugins must use the **same version of [`pg_query`] crate** as PgDog. This happens automatically when using `pg_query` structs re-exported by this crate.
//!
//!
//! #### Configure dependencies
//!
//! Add the following to your plugin's `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! pgdog-plugin = "0.1.6"
//! ```
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
//! ```no_run
//! use pgdog_plugin::prelude::*;
//! use pg_query::{protobuf::{Node, RawStmt}, NodeEnum};
//!
//! #[route]
//! fn route(context: Context) -> Route {
//!     let proto = context
//!         .statement()
//!         .protobuf();
//!     let root = proto.stmts.first();
//!     if let Some(root) = root {
//!         if let Some(ref stmt) = root.stmt {
//!             if let Some(ref node) = stmt.node {
//!                 if let NodeEnum::SelectStmt(_) = node {
//!                     return Route::new(Shard::Unknown, ReadWrite::Read);
//!                 }
//!             }
//!         }
//!     }
//!
//!     Route::new(Shard::Unknown, ReadWrite::Write)
//! }
//! ```
//!
//! The [`macros::route`] macro wraps the function into a safe FFI interface which PgDog calls at runtime.
//!
//! ### Parsing parameters
//!
//! If your clients are using prepared statements (or the extended protocol), query parameters will be sent separately
//! from query text. They are stored in the [`crate::parameters::Parameters`] struct, passed down from PgDog's query parser:
//!
//! ```
//! # use pgdog_plugin::prelude::*;
//! # let context = unsafe { Context::doc_test() };
//! let params = context.parameters();
//! if let Some(param) = params
//!     .get(0)
//!     .map(|p| p.decode(params.parameter_format(0)))
//!     .flatten() {
//!         println!("param $1 = {:?}", param);
//! }
//! ```
//!
//! ### Errors
//!
//! Plugin functions cannot return errors or panic. To handle errors, you can log them to `stderr` and return a default route,
//! which PgDog will ignore.
//!
//! ### Blocking queries
//!
//! Plugins can block queries from executing. This is useful if you'd like to enforce specific requirements,
//! like a mandatory `tenant_id` column, for example, or want to block your apps from saving sensitive information,
//! like credit card numbers or plain text passwords.
//!
//! #### Example
//!
//! ```
//! use pgdog_plugin::prelude::*;
//!
//! #[route]
//! fn route(context: Context) -> Route {
//!     let params = context.parameters();
//!     let password = params
//!         .get(3)
//!         .map(|param| param.decode(ParameterFormat::Text))
//!         .flatten();
//!     if let Some(ParameterValue::Text(password)) = password {
//!         if !password.starts_with("$bcrypt") {
//!             return Route::block();
//!         }
//!     }
//!
//!     Route::unknown()
//! }
//! ```
//!
//! # Enabling plugins
//!
//! Plugins are shared libraries, loaded by PgDog at runtime using `dlopen(3)`. If specifying only its name, make sure to place the plugin's shared library
//! into one of the following locations:
//!
//! - Any of the system default paths, e.g.: `/lib`, `/usr/lib`, `/lib64`, `/usr/lib64`, etc.
//! - Path specified by the `LD_LIBRARY_PATH` (on Linux) or `DYLD_LIBRARY_PATH` (Mac OS) environment variables.
//!
//! Alternatively, specify the relative or absolute path to the shared library as the plugin name. Plugins aren't loaded automatically. For each plugin you want to enable, add it to `pgdog.toml`:
//!
//! ```toml
//! [[plugins]]
//! # Plugin should be in /usr/lib or in LD_LIBRARY_PATH.
//! name = "my_plugin"
//!
//! [[plugins]]
//! # Plugin should be in $PWD/libmy_plugin.so
//! name = "libmy_plugin.so"
//!
//! [[plugins]]
//! # Absolute path to the plugin.
//! name = "/usr/local/lib/libmy_plugin.so"
//! ```
//!

/// Bindgen-generated FFI bindings.
#[allow(non_upper_case_globals)]
#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
pub mod bindings;

pub mod ast;
pub mod comp;
pub mod context;
pub mod parameters;
pub mod plugin;
pub mod prelude;
pub mod string;

pub use bindings::*;
pub use context::*;
pub use plugin::*;

pub use libloading;

pub use pg_query;
pub use pgdog_macros as macros;
