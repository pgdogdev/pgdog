//! Context passed to and from the plugins.

use std::ops::Deref;

use crate::{
    bindings::PdRouterContext, parameters::Parameters, PdParameters, PdRoute, PdStatement,
};

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
/// # use pgdog_plugin::Context;
/// # let context = unsafe { Context::doc_test() };
/// # let statement = context.statement();
/// use pgdog_plugin::pg_query::NodeEnum;
///
/// let ast = statement.protobuf();
/// let root = ast
///     .stmts
///     .first()
///     .unwrap()
///     .stmt
///     .as_ref()
///     .unwrap()
///     .node
///     .as_ref();
///
/// if let Some(NodeEnum::SelectStmt(stmt)) = root {
///     println!("SELECT statement: {:#?}", stmt);
/// }
///
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

/// Context information provided by PgDog to the plugin at statement execution. It contains the actual statement and several metadata about
/// the state of the database cluster:
///
/// - Number of shards
/// - Does it have replicas
/// - Does it have a primary
///
/// ### Example
///
/// ```
/// use pgdog_plugin::{Context, Route, macros, Shard, ReadWrite};
///
/// #[macros::route]
/// fn route(context: Context) -> Route {
///     let shards = context.shards();
///     let read_only = context.read_only();
///     let ast = context.statement().protobuf();
///
///     println!("shards: {} (read_only: {})", shards, read_only);
///     println!("ast: {:#?}", ast);
///
///     let read_write = if read_only {
///         ReadWrite::Read
///     } else {
///         ReadWrite::Write
///     };
///
///     Route::new(Shard::Direct(0), read_write)
/// }
/// ```
///
pub struct Context {
    ffi: PdRouterContext,
}

impl From<PdRouterContext> for Context {
    fn from(value: PdRouterContext) -> Self {
        Self { ffi: value }
    }
}

impl Context {
    /// Returns a reference to the Abstract Syntax Tree (AST) created by [`pg_query`].
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use pgdog_plugin::Context;
    /// # let context = unsafe { Context::doc_test() };
    /// # let statement = context.statement();
    /// let ast = context.statement().protobuf();
    /// let nodes = ast.nodes();
    /// ```
    pub fn statement(&self) -> Statement {
        Statement {
            ffi: self.ffi.query,
        }
    }

    /// Returns true if the database cluster doesn't have a primary database and can only serve
    /// read queries.
    ///
    /// # Example
    ///
    /// ```
    /// # use pgdog_plugin::Context;
    /// # let context = unsafe { Context::doc_test() };
    ///
    /// let read_only = context.read_only();
    ///
    /// if read_only {
    ///     println!("Database cluster doesn't have a primary, only replicas.");
    /// }
    /// ```
    pub fn read_only(&self) -> bool {
        self.ffi.has_primary == 0
    }

    /// Returns true if the database cluster has replica databases.
    ///
    /// # Example
    ///
    /// ```
    /// # use pgdog_plugin::Context;
    /// # let context = unsafe { Context::doc_test() };
    /// let has_replicas = context.has_replicas();
    ///
    /// if has_replicas {
    ///     println!("Database cluster can load balance read queries.")
    /// }
    /// ```
    pub fn has_replicas(&self) -> bool {
        self.ffi.has_replicas == 1
    }

    /// Returns true if the database cluster has a primary database and can serve write queries.
    ///
    /// # Example
    ///
    /// ```
    /// # use pgdog_plugin::Context;
    /// # let context = unsafe { Context::doc_test() };
    /// let has_primary = context.has_primary();
    ///
    /// if has_primary {
    ///     println!("Database cluster can serve write queries.");
    /// }
    /// ```
    pub fn has_primary(&self) -> bool {
        !self.read_only()
    }

    /// Returns the number of shards in the database cluster.
    ///
    /// # Example
    ///
    /// ```
    /// # use pgdog_plugin::Context;
    /// # let context = unsafe { Context::doc_test() };
    /// let shards = context.shards();
    ///
    /// if shards > 1 {
    ///     println!("Plugin should consider which shard to route the query to.");
    /// }
    /// ```
    pub fn shards(&self) -> usize {
        self.ffi.shards as usize
    }

    /// Returns true if the database cluster has more than one shard.
    ///
    /// # Example
    ///
    /// ```
    /// # use pgdog_plugin::Context;
    /// # let context = unsafe { Context::doc_test() };
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

    /// Returns true if PgDog strongly believes the statement should be sent to a primary. This indicates
    /// that the statement is **not** a `SELECT` (e.g. `UPDATE`, `DELETE`, etc.), or a `SELECT` that is very likely to write data to the database, e.g.:
    ///
    /// ```sql
    /// WITH users AS (
    ///     INSERT INTO users VALUES (1, 'test@acme.com') RETURNING *
    /// )
    /// SELECT * FROM users;
    /// ```
    ///
    /// # Example
    ///
    /// ```
    /// # use pgdog_plugin::Context;
    /// # let context = unsafe { Context::doc_test() };
    /// if context.write_override() {
    ///     println!("We should really send this query to the primary.");
    /// }
    /// ```
    pub fn write_override(&self) -> bool {
        self.ffi.write_override == 1
    }

    /// Returns a list of parameters bound on the statement. If using the simple protocol,
    /// this is going to be empty and parameters will be in the actual query text.
    ///
    /// # Example
    ///
    /// ```
    /// use pgdog_plugin::prelude::*;
    /// # let context = unsafe { Context::doc_test() };
    /// let params = context.parameters();
    /// if let Some(param) = params.get(0) {
    ///     let value = param.decode(params.parameter_format(0));
    ///     println!("{:?}", value);
    /// }
    /// ```
    pub fn parameters(&self) -> Parameters {
        self.ffi.params.into()
    }
}

impl Context {
    /// Used for doc tests only. **Do not use**.
    ///
    /// # Safety
    ///
    /// Not safe, don't use. We use it for doc tests only.
    ///
    pub unsafe fn doc_test() -> Context {
        use std::{os::raw::c_void, ptr::null};

        Context {
            ffi: PdRouterContext {
                shards: 1,
                has_replicas: 1,
                has_primary: 1,
                in_transaction: 0,
                write_override: 0,
                query: PdStatement {
                    version: 1,
                    len: 0,
                    data: null::<c_void>() as *mut c_void,
                },
                params: PdParameters::default(),
            },
        }
    }
}

/// What shard, if any, the statement should be sent to.
///
/// ### Example
///
/// ```
/// use pgdog_plugin::Shard;
///
/// // Send query to shard 2.
/// let direct = Shard::Direct(2);
///
/// // Send query to all shards.
/// let cross_shard = Shard::All;
///
/// // Let PgDog handle sharding.
/// let unknown = Shard::Unknown;
/// ```
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Shard {
    /// Direct-to-shard statement, sent to the specified shard only.
    Direct(usize),
    /// Send statement to all shards and let PgDog collect and transform the results.
    All,
    /// Not clear which shard it should go to, so let PgDog decide.
    /// Use this if you don't want to handle sharding inside the plugin.
    Unknown,
    /// The statement is blocked from executing.
    Blocked,
}

impl From<Shard> for i64 {
    fn from(value: Shard) -> Self {
        match value {
            Shard::Direct(value) => value as i64,
            Shard::All => -1,
            Shard::Unknown => -2,
            Shard::Blocked => -3,
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
        } else if value == -3 {
            Shard::Blocked
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

/// Indicates if the statement is a read or a write. Read statements are sent to a replica, if one is configured.
/// Write statements are sent to the primary.
///
/// ### Example
///
/// ```
/// use pgdog_plugin::ReadWrite;
///
/// // The statement should go to a replica.
/// let read = ReadWrite::Read;
///
/// // The statement should go the primary.
/// let write = ReadWrite::Write;
///
/// // Skip and let PgDog decide.
/// let unknown = ReadWrite::Unknown;
/// ```
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ReadWrite {
    /// Send the statement to a replica, if any are configured.
    Read,
    /// Send the statement to the primary.
    Write,
    /// Plugin doesn't know if the statement is a read or write. This let's PgDog decide.
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

/// Statement route.
///
/// PgDog uses this to decide where a query should be sent to. Read statements are sent to a replica,
/// while write ones are sent the primary. If the cluster has more than one shard, the statement can be
/// sent to a specific database, or all of them.
///
/// ### Example
///
/// ```
/// use pgdog_plugin::{Shard, ReadWrite, Route};
///
/// // This sends the query to the primary database of shard 0.
/// let route = Route::new(Shard::Direct(0), ReadWrite::Write);
///
/// // This sends the query to all shards, routing them to a replica
/// // of each shard, if any are configured.
/// let route = Route::new(Shard::All, ReadWrite::Read);
///
/// // No routing information is available. PgDog will ignore it
/// // and make its own decision.
/// let route = Route::unknown();
pub struct Route {
    ffi: PdRoute,
}

impl Default for Route {
    fn default() -> Self {
        Self::unknown()
    }
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
    /// Create new route.
    ///
    /// # Arguments
    ///
    /// * `shard`: Which shard the statement should be sent to.
    /// * `read_write`: Does the statement read or write data. Read statements are sent to a replica. Write statements are sent to the primary.
    ///
    pub fn new(shard: Shard, read_write: ReadWrite) -> Route {
        Self {
            ffi: PdRoute {
                shard: shard.into(),
                read_write: read_write.into(),
            },
        }
    }

    /// Create new route with no sharding or read/write information.
    /// Use this if you don't want your plugin to do query routing.
    /// Plugins that do something else with queries, e.g., logging, metrics,
    /// can return this route.
    pub fn unknown() -> Route {
        Self {
            ffi: PdRoute {
                shard: -2,
                read_write: 2,
            },
        }
    }

    /// Block the query from being sent to a database. PgDog will abort the query
    /// and return an error to the client, telling them which plugin blocked it.
    pub fn block() -> Route {
        Self {
            ffi: PdRoute {
                shard: -3,
                read_write: 2,
            },
        }
    }
}
