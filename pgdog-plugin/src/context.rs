//! Context passed to and from the plugins.

use std::ops::Deref;

use crate::{bindings::PdRouterContext, PdRoute, PdStatement};

/// PostgreSQL statement, parsed by [`pg_query`].
///
/// Implements [`Deref`] on [`PdStatement`], which is passed
/// in using the FFI interface.
/// Use the [`PdStatement::protobuf`] method to obtain a reference
/// to the Abstract Syntax Tree.
///
/// ### Example
///
/// ```no_run
/// let ast = statement.protobuf();
/// println!("{:#?}", ast);
/// ```
pub struct Statement {
    ffi: PdStatement,
}

impl Deref for Statement {
    type Target = PdStatement;

    fn deref(&self) -> &Self::Target {
        &self.ffi
    }
}

pub struct Context {
    ffi: PdRouterContext,
}

impl From<PdRouterContext> for Context {
    fn from(value: PdRouterContext) -> Self {
        Self { ffi: value }
    }
}

impl Context {
    /// Get reference to the AST parsed by [`pg_query`].
    ///
    /// # Example
    ///
    /// ```no_run
    /// let ast = context.statement().protobuf();
    /// let nodes = ast.nodes();
    /// ```
    pub fn statement(&self) -> Statement {
        Statement {
            ffi: self.ffi.query,
        }
    }

    /// Is the database cluster read-only, i.e., has no primary database.
    ///
    /// # Example
    ///
    /// ```no_run
    /// let read_only = context.read_only();
    ///
    /// if read_only {
    ///     println!("Database cluster doesn't have a primary, only replicas.");
    /// }
    /// ```
    pub fn read_only(&self) -> bool {
        self.ffi.has_primary == 0
    }

    /// Is the database cluster write-only, i.e., no replicas and only has a primary.
    ///
    /// # Example
    ///
    /// ```no_run
    /// let write_only = context.write_only();
    ///
    /// if write_only {
    ///     println!("Database cluster doesn't have replicas.")
    /// }
    /// ```
    pub fn write_only(&self) -> bool {
        self.ffi.has_replicas == 0
    }

    /// The database cluster has replicas. Opposite of [`Self::read_only`].
    pub fn has_replicas(&self) -> bool {
        !self.write_only()
    }

    /// Does the database cluster has a primary. Opposite of [`Self::write_only`].
    pub fn has_primary(&self) -> bool {
        !self.read_only()
    }

    /// How many shards are in the database cluster.
    ///
    /// # Example
    ///
    /// ```no_run
    /// let shards = context.shards();
    ///
    /// if shards > 1 {
    ///     println!("Plugin should consider which shard to route the query to.");
    /// }
    /// ```
    pub fn shards(&self) -> usize {
        self.ffi.shards as usize
    }

    /// Is the cluster sharded, i.e., has more than 1 shard.
    ///
    /// # Example
    ///
    /// ```no_run
    /// let sharded = context.sharded();
    /// let shards = context.shards();
    ///
    /// if sharded {
    ///     assert!(shards > 1);
    /// } else {
    ///     assert_eq!(shards, 1);
    /// }
    /// ```
    pub fn sharded(&self) -> bool {
        self.shards() > 1
    }

    /// PgDog strongly believes this statement should go to a primary. This happens if the statement is not a `SELECT`
    /// or the `SELECT` statement contains components that could trigger a write to the database.
    ///
    /// # Example
    ///
    /// ```no_run
    /// if context.write_override() {
    ///     println!("We should really send this query to the primary.");
    /// }
    /// ```
    pub fn write_override(&self) -> bool {
        self.ffi.write_override == 1
    }
}

/// What shard, if any, the statement should be routed to.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Shard {
    /// Direct-to-shard with the shard number.
    Direct(usize),
    /// Route statement to all shards.
    All,
    /// Not clear which shard it should go to, let PgDog decide.
    /// Use this if you don't want to handle sharding inside the plugin.
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
    /// Route the statement to a replica.
    Read,
    /// Route the statement to a primary.
    Write,
    /// Plugin doesn't know if the statement is a read or write. Let's PgDog decide.
    /// Use this if you don't want to make this decision in the plugin.
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
        Route::unknown().ffi
    }
}

pub struct Route {
    ffi: PdRoute,
}

impl Deref for Route {
    type Target = PdRoute;

    fn deref(&self) -> &Self::Target {
        &self.ffi
    }
}

impl From<PdRoute> for Route {
    fn from(value: PdRoute) -> Self {
        Self { ffi: value }
    }
}

impl From<Route> for PdRoute {
    fn from(value: Route) -> Self {
        value.ffi
    }
}

impl Route {
    /// Don't use this plugin's output for routing.
    pub fn unknown() -> Route {
        Self {
            ffi: PdRoute {
                shard: -2,
                read_write: 2,
            },
        }
    }

    /// Assign this route to the statement.
    ///
    /// # Arguments
    ///
    /// * `shard`: Which shard, if any, the statement should go to.
    /// * `read_write`: Should the statement go to a replica or the primary?
    ///
    pub fn new(shard: Shard, read_write: ReadWrite) -> Route {
        Self {
            ffi: PdRoute {
                shard: shard.into(),
                read_write: read_write.into(),
            },
        }
    }
}
