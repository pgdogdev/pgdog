//! Connection state.

/// Client/server state.
#[derive(Debug, PartialEq)]
pub enum State {
    /// Waiting for work.
    Idle,
    /// Reading/writing data from/to the network.
    Active,
    /// In a transaction, but waiting for more work.
    IdleInTransaction,
    /// Transaction returned an error, but the connection is still ok to use.
    TransactionError,
    /// Waiting for a connection.
    Waiting,
    /// Connection is closed.
    Disconnected,
    /// An error occurered.
    Error,
}
