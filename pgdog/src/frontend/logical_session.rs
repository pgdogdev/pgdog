//! # Logical Session Management in PgDog
//!
//! PgDog hides your shard topology and presents a single Postgres endpoint.
//!
//! When the client issues session commands (e.g. `SET`), `LogicalSession` records
//! them to facilitate the replication of each command to every true server's
//! session before routing queries.

// -----------------------------------------------------------------------------
// ----- LogicalSession --------------------------------------------------------

#[derive(Debug)]
pub struct LogicalSession {}

impl LogicalSession {
    pub fn new() -> Self {
        LogicalSession {}
    }
}

// -----------------------------------------------------------------------------
// ----- LogicalSession : Public methods ---------------------------------------

impl LogicalSession {}

// -----------------------------------------------------------------------------
// ----- LogicalSession : Private methods --------------------------------------

impl LogicalSession {}

// -----------------------------------------------------------------------------
// ----- SessionError ----------------------------------------------------------

#[derive(Debug)]
pub enum LogicalSessionError {}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
