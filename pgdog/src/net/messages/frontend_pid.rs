use std::sync::atomic::{AtomicU32, Ordering};

use derive_more::Display;
use pgdog_postgres_types::ToDataRowColumn;

use crate::net::BackendKeyData;

static COUNTER: AtomicU32 = AtomicU32::new(0);

/// Identifies the client that connected to pgdog. The client expects pgdog to act
/// as a PostgreSQL backend, and this pid is used to generate the related [BackendKeyData]
/// and as a key to fetch the stored [BackendKeyData].
/// The pid is the same pid as in the [BackendKeyData] but without the secret, and it's
/// a monotonically increasing counter.
#[derive(Copy, Clone, Debug, Display, Hash, PartialEq, Eq)]
pub struct FrontendPid(i32);

impl Default for FrontendPid {
    fn default() -> Self {
        Self::new()
    }
}

impl FrontendPid {
    pub fn new() -> Self {
        // Mask off the sign bit so the synthetic pid is always non-negative,
        // matching the Postgres convention that backend pids are positive.
        Self((COUNTER.fetch_add(1, Ordering::SeqCst) & i32::MAX as u32) as i32)
    }

    pub fn pid(&self) -> i32 {
        self.0
    }
}

impl From<&BackendKeyData> for FrontendPid {
    fn from(key: &BackendKeyData) -> Self {
        Self(key.pid())
    }
}

impl ToDataRowColumn for FrontendPid {
    fn to_data_row_column(&self) -> pgdog_postgres_types::Data {
        pgdog_postgres_types::Data::from(bytes::Bytes::copy_from_slice(
            self.0.to_string().as_bytes(),
        ))
    }
}
