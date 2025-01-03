//! Statistics.

/// Connection statistics.
#[derive(Debug, Default)]
pub struct ConnStats {
    /// Number of bytes sent via the connection.
    pub bytes_sent: usize,
    /// Number of bytes received via the connection.
    pub bytes_received: usize,
    /// Number of queries executed.
    pub queries: usize,
    /// Number of transactions executed.
    pub transactions: usize,
}

/// Pool statistics.
#[derive(Default, Debug)]
pub struct PoolStats {
    /// Clients active.
    pub active: usize,
    /// Clients waiting.
    pub waiting: usize,
    /// Servers performing login.
    pub login: usize,
}
