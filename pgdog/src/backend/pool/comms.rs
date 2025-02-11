use tokio::sync::Notify;

/// Internal pool notifications.
pub(super) struct Comms {
    /// An idle connection is available in the pool.
    pub(super) ready: Notify,
    /// A client requests a new connection to be open
    /// or waiting for one to be returned to the pool.
    pub(super) request: Notify,
    /// Pool is shutting down.
    pub(super) shutdown: Notify,
}

impl Comms {
    /// Create new comms.
    pub(super) fn new() -> Self {
        Self {
            ready: Notify::new(),
            request: Notify::new(),
            shutdown: Notify::new(),
        }
    }
}
