use std::sync::atomic::{AtomicU64, Ordering};

use derive_more::Display;
use pgdog_postgres_types::ToDataRowColumn;

use crate::net::BackendKeyData;

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// Identifies a backend connection to the real PostgreSQL backend returned as [BackendKeyData].
/// `seq` is process-unique and helps to avoid collisions if the same pid is returned by multiple backends;
/// `pid` is the real Postgres backend pid kept for `pg_stat_activity` correlation and debug output.
///
/// Identity is minted via `BackendPid::from(&key)`, which auto-increments the global `seq`, so two
/// conversions of the same key are never equal. Use `for_test(n)` in tests that require stable,
/// equality-comparable identities.
#[derive(Copy, Clone, Debug, Display, Hash, PartialEq, Eq)]
#[display("{seq}#{pid}")]
pub struct BackendPid {
    pub(crate) seq: u64,
    pub(crate) pid: i32,
}

impl BackendPid {
    /// Real Postgres backend pid.
    pub fn pid(&self) -> i32 {
        self.pid
    }
}

impl From<&BackendKeyData> for BackendPid {
    fn from(key: &BackendKeyData) -> Self {
        Self {
            seq: COUNTER.fetch_add(1, Ordering::SeqCst),
            pid: key.pid,
        }
    }
}

#[cfg(test)]
impl BackendPid {
    /// Stable identity for tests. Does not touch the global counter.
    /// Never use outside `#[cfg(test)]` — the value is not process-unique.
    pub fn for_test(n: i32) -> Self {
        Self {
            seq: n as u64,
            pid: n,
        }
    }
}

impl ToDataRowColumn for BackendPid {
    fn to_data_row_column(&self) -> pgdog_postgres_types::Data {
        // Emit the real Postgres pid so SHOW SERVERS.remote_pid is unchanged.
        pgdog_postgres_types::Data::from(bytes::Bytes::copy_from_slice(
            self.pid.to_string().as_bytes(),
        ))
    }
}
