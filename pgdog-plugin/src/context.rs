//! Context passed to and from the plugins.

use crate::parameters::Parameters;

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
/// use pgdog_plugin::{Context, Route, Shard, ReadWrite};
/// # use pgdog_plugin::{Plugin, PdStr};
///
/// # pgdog_plugin::plugin!(MyPlugin);
/// # struct MyPlugin;
/// # impl Plugin for MyPlugin {
/// #     extern "C-unwind" fn version() -> PdStr<'static> { "0".into() }
/// fn route(context: Context<'_>) -> Route {
///     let shards = context.shards();
///     let read_only = context.read_only();
///     let ast = context.query;
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
/// # }
/// ```
///
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct Context<'a> {
    /// How many shards are configured.
    pub shards: u64,
    /// Does the database cluster have replicas?
    pub has_replicas: bool,
    /// Does the database cluster have a primary?
    pub has_primary: bool,
    /// Is the query being executed inside a transaction?
    pub in_transaction: bool,
    /// PgDog strongly believes this statement should go to a primary.
    pub write_override: bool,
    /// pg_query generated Abstract Syntax Tree of the statement.
    // Note: This is not at all FFI safe, and is UB across an FFI boundary.
    // We are relying on an implemenation detail of the compiler that could
    // change at any time to make this work. There is no safe way to pass
    // this type, but this won't be an issue with the new parser
    #[cfg(not(feature = "new_parser"))]
    pub query: &'a pg_query::protobuf::ParseResult,
    #[cfg(feature = "new_parser")]
    /// The parsed Abstract Syntax Tree of the statement(s).
    pub query: &'a pg_raw_parse::StmtList,
    /// Bound parameters.
    pub params: Parameters<'a>,
}

impl Context<'_> {
    /// Returns true if the database cluster doesn't have a primary database and can only serve
    /// read queries.
    ///
    /// # Example
    ///
    /// ```
    /// # use pgdog_plugin::Context;
    /// # let context = Context::doc_test();
    ///
    /// let read_only = context.read_only();
    ///
    /// if read_only {
    ///     println!("Database cluster doesn't have a primary, only replicas.");
    /// }
    /// ```
    pub fn read_only(&self) -> bool {
        !self.has_primary
    }

    /// Returns true if the database cluster has replica databases.
    ///
    /// # Example
    ///
    /// ```
    /// # use pgdog_plugin::Context;
    /// # let context = Context::doc_test();
    /// let has_replicas = context.has_replicas();
    ///
    /// if has_replicas {
    ///     println!("Database cluster can load balance read queries.")
    /// }
    /// ```
    pub fn has_replicas(&self) -> bool {
        self.has_replicas
    }

    /// Returns true if the database cluster has a primary database and can serve write queries.
    ///
    /// # Example
    ///
    /// ```
    /// # use pgdog_plugin::Context;
    /// # let context = Context::doc_test();
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
    /// # let context = Context::doc_test();
    /// let shards = context.shards();
    ///
    /// if shards > 1 {
    ///     println!("Plugin should consider which shard to route the query to.");
    /// }
    /// ```
    pub fn shards(&self) -> usize {
        self.shards as usize
    }

    /// Returns true if the database cluster has more than one shard.
    ///
    /// # Example
    ///
    /// ```
    /// # use pgdog_plugin::Context;
    /// # let context = Context::doc_test();
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
    /// # let context = Context::doc_test();
    /// if context.write_override() {
    ///     println!("We should really send this query to the primary.");
    /// }
    /// ```
    pub fn write_override(&self) -> bool {
        self.write_override
    }

    /// Returns a list of parameters bound on the statement. If using the simple protocol,
    /// this is going to be empty and parameters will be in the actual query text.
    ///
    /// # Example
    ///
    /// ```
    /// use pgdog_plugin::prelude::*;
    /// # let context = Context::doc_test();
    /// let params = context.parameters();
    /// if let Some(param) = params.parameters.get(0) {
    ///     let value = param.decode(params.parameter_format(0));
    ///     println!("{:?}", value);
    /// }
    /// ```
    pub fn parameters(&self) -> Parameters<'_> {
        self.params
    }
}

impl Context<'_> {
    #[doc(hidden)]
    pub fn doc_test() -> Self {
        #[cfg(not(feature = "new_parser"))]
        use pg_query::protobuf::ParseResult;
        #[cfg(not(feature = "new_parser"))]
        static EMPTY_PARSE_RESULT: ParseResult = ParseResult {
            version: 0,
            stmts: Vec::new(),
        };
        Context {
            shards: 1,
            has_replicas: true,
            has_primary: true,
            in_transaction: false,
            write_override: false,
            #[cfg(not(feature = "new_parser"))]
            query: &EMPTY_PARSE_RESULT,
            #[cfg(feature = "new_parser")]
            query: pg_raw_parse::list::empty_list(),
            params: Parameters::default(),
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
#[repr(C)]
pub struct Route {
    /// Which shard the query should go to.
    ///
    /// `-1` for all shards, `-2` for unknown, this setting is ignored.
    pub shard: i64,
    /// Is the query a read and should go to a replica?
    ///
    /// `1` for `true`, `0` for `false`, `2` for unknown, this setting is ignored.
    pub read_write: u8,
}

impl Default for Route {
    fn default() -> Self {
        Self::unknown()
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
            shard: shard.into(),
            read_write: read_write.into(),
        }
    }

    /// Create new route with no sharding or read/write information.
    /// Use this if you don't want your plugin to do query routing.
    /// Plugins that do something else with queries, e.g., logging, metrics,
    /// can return this route.
    pub fn unknown() -> Route {
        Self {
            shard: -2,
            read_write: 2,
        }
    }

    /// Block the query from being sent to a database. PgDog will abort the query
    /// and return an error to the client, telling them which plugin blocked it.
    pub fn block() -> Route {
        Self {
            shard: -3,
            read_write: 2,
        }
    }
}
