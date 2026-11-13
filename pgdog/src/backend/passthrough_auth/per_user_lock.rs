//! Per-connection-pool throttle when creating users.
//!
//! This is a workaround for config and connection pools held in
//! different globals and not switched atomically.
//!
