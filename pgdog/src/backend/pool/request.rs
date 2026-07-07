use tokio::time::Instant;

use crate::net::messages::FrontendPid;

/// Connection request.
#[derive(Clone, Debug, Copy)]
pub struct Request {
    pub id: FrontendPid,
    pub created_at: Instant,
    pub read: bool,
    /// Minimum replica replay LSN (64-bit) the read requires. Reads are routed
    /// only to a replica that has replayed at least this far; `None` disables
    /// the floor.
    pub min_lsn: Option<i64>,
}

impl Request {
    pub fn new(id: FrontendPid, read: bool) -> Self {
        Self {
            id,
            created_at: Instant::now(),
            read,
            min_lsn: None,
        }
    }

    pub fn unrouted(id: FrontendPid) -> Self {
        Self {
            id,
            created_at: Instant::now(),
            read: false,
            min_lsn: None,
        }
    }

    /// Require the serving replica to have replayed at least `lsn` (read-your-writes).
    pub fn with_min_lsn(mut self, lsn: Option<i64>) -> Self {
        self.min_lsn = lsn;
        self
    }
}

impl Default for Request {
    fn default() -> Self {
        Self::unrouted(FrontendPid::new())
    }
}
