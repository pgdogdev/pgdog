//! Query rewrite engine.
//!
//! It handles the following scenarios:
//!
//! 1. Sharding key UPDATE: rewrite to send a DELETE and INSERT
//! 2. Multi-tuple INSERT: rewrite to send multiple INSERTs
//! 3. pgdog.unique_id() call: inject a unique ID
//!
pub mod error;
pub mod unique_id;

pub use error::Error;
