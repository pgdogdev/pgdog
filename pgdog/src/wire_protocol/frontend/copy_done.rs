//! Module: wire_protocol::frontend::copy_done
//!
//! Re-exports the bidirectional CopyDoneFrame and CopyDoneError
//! to avoid duplicating the implementation.

pub use crate::wire_protocol::bidirectional::copy_done::{CopyDoneError, CopyDoneFrame};
