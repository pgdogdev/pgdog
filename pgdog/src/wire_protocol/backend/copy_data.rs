//! Module: wire_protocol::backend::copy_data
//!
//! Re-exports the bidirectional CopyDataFrame and CopyDataError
//! to avoid duplicating the implementation.

pub use crate::wire_protocol::bidirectional::copy_data::{CopyDataError, CopyDataFrame};
