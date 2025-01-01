//! Connection state.

/// Client/server state.
pub enum State {
    /// Waiting for work.
    Idle,
    /// Reading/writing data from/to the network.
    Active,
    /// In a transaction, but waiting for more work.
    IdleInTransaction,
    /// Waiting for a connection.
    Waiting,
    /// Connection is closed.
    Disconnected,
}
