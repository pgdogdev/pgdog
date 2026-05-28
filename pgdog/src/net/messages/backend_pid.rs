//! BackendPid — lightweight process identity for routing and statistics.
//!
//! This is the canonical identifier for a backend server or client connection.

use std::sync::atomic::{AtomicI32, Ordering};

use pgdog_postgres_types::ToDataRowColumn;

use once_cell::sync::Lazy;
use rand::Rng;

static COUNTER: Lazy<AtomicI32> = Lazy::new(|| AtomicI32::new(0));

/// Increment the global connection counter and return the old value.
fn next_counter() -> i32 {
    COUNTER.fetch_add(1, Ordering::SeqCst)
}

/// Opaque backend-process identifier.
///
/// Used as `HashMap` keys, struct fields, and function arguments everywhere the
/// cancel *secret* is not needed.
#[derive(Copy, Clone, Debug, Display, Hash, PartialEq, Eq, PartialOrd, Ord, From, Into)]
pub struct BackendPid(i32);

impl BackendPid {
    /// Create a random `BackendPid` (used for server connections).
    pub fn random() -> Self {
        Self(rand::rng().random())
    }

    /// Create the next sequential `BackendPid` (used for client connections).
    pub fn next() -> Self {
        Self(next_counter())
    }
}

impl ToDataRowColumn for BackendPid {
    fn to_data_row_column(&self) -> pgdog_postgres_types::Data {
        // Displayed as a decimal integer, same as i64.
        pgdog_postgres_types::Data::from(bytes::Bytes::copy_from_slice(
            self.0.to_string().as_bytes(),
        ))
    }
}
