//! PgDog plugin interface.

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
