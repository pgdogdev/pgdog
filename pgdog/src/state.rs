//! Connection state.

/// Client/server state.
#[derive(Debug, PartialEq, Default, Copy, Clone)]
pub enum State {
    /// Waiting for work.
    #[default]
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
    /// An error occurred.
    Error,
    /// Parse complete.
    ParseComplete,
    /// Prepared statement error.
    PreparedStatementError,
    /// Processing server reply.
    ReceivingData,
    /// Copy started
    CopyMode,
    /// Just close the connection.
    ForceClose,
}

impl std::fmt::Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use State::*;
        match self {
            Idle => write!(f, "idle"),
            Active => write!(f, "active"),
            IdleInTransaction => write!(f, "idle in transaction"),
            TransactionError => write!(f, "transaction error"),
            Waiting => write!(f, "waiting"),
            Disconnected => write!(f, "disconnected"),
            Error => write!(f, "error"),
            ParseComplete => write!(f, "parse complete"),
            PreparedStatementError => write!(f, "prepared statement error"),
            ReceivingData => write!(f, "receiving data"),
            CopyMode => write!(f, "copy mode"),
            ForceClose => write!(f, "force close"),
        }
    }
}
