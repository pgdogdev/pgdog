//! Context passed to and from the plugins.

use crate::{PdQuery, PdRoute, bindings::PdRouterContext};

impl PdRouterContext {
    /// Get reference to the AST parsed by `pg_query`.
    pub fn statement(&self) -> PdQuery {
        self.query
    }

    /// Is the database cluster read-only, i.e., no primary?
    pub fn read_only(&self) -> bool {
        self.has_primary == 0
    }

    /// Is the database cluster write-only, i.e., no replicas?
    pub fn write_only(&self) -> bool {
        self.has_replicas == 0
    }

    /// Does the database cluster have replicas?
    pub fn has_replicas(&self) -> bool {
        !self.write_only()
    }

    /// Does the database cluster have a primary?
    pub fn has_primary(&self) -> bool {
        !self.read_only()
    }

    /// How many shards are in the database cluster.
    pub fn shards(&self) -> usize {
        self.shards as usize
    }

    /// Is the cluster sharded?
    pub fn sharded(&self) -> bool {
        self.shards() > 1
    }

    /// PgDog thinks you really should send this to the primary.
    pub fn write_override(&self) -> bool {
        self.write_override == 1
    }
}

/// What shard, if any, the statement should be routed to.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Shard {
    Direct(usize),
    All,
    Unknown,
}

impl From<Shard> for i64 {
    fn from(value: Shard) -> Self {
        match value {
            Shard::Direct(value) => value as i64,
            Shard::All => -1,
            Shard::Unknown => -2,
        }
    }
}

impl TryFrom<i64> for Shard {
    type Error = ();
    fn try_from(value: i64) -> Result<Self, Self::Error> {
        Ok(if value == -1 {
            Shard::All
        } else if value == -2 {
            Shard::Unknown
        } else if value >= 0 {
            Shard::Direct(value as usize)
        } else {
            return Err(());
        })
    }
}

impl TryFrom<u8> for ReadWrite {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(if value == 0 {
            ReadWrite::Write
        } else if value == 1 {
            ReadWrite::Read
        } else if value == 2 {
            ReadWrite::Unknown
        } else {
            return Err(());
        })
    }
}

/// Should the statement be routed to a replica or a primary?
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ReadWrite {
    Read,
    Write,
    Unknown,
}

impl From<ReadWrite> for u8 {
    fn from(value: ReadWrite) -> Self {
        match value {
            ReadWrite::Write => 0,
            ReadWrite::Read => 1,
            ReadWrite::Unknown => 2,
        }
    }
}

impl Default for PdRoute {
    fn default() -> Self {
        Self::unknown()
    }
}

impl PdRoute {
    /// Don't use this plugin's output for routing.
    pub fn unknown() -> PdRoute {
        PdRoute {
            shard: -2,
            read_write: 2,
        }
    }

    /// Assign this route to the statement.
    ///
    /// # Arguments
    ///
    /// * `shard`: Which shard, if any, the statement should go to.
    /// * `read_write`: Should the statement go to a replica or the primary?
    ///
    pub fn new(shard: Shard, read_write: ReadWrite) -> PdRoute {
        PdRoute {
            shard: shard.into(),
            read_write: read_write.into(),
        }
    }
}
