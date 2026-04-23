//! Two-phase commit write-ahead log.
//!
//! See [`record`] for the on-disk record format.

mod error;
mod record;
mod segment;
