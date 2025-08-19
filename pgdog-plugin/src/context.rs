//! Context passed to and from the plugins.

use std::{io::Cursor, ops::Deref, os::raw::c_void, ptr::null, slice::from_raw_parts};

use bytes::{Buf, Bytes};

use crate::{bindings::PdRouterContext, PdParameters, PdRoute, PdStatement};

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
                parameters: Parameters::docs_test().ffi,
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
}

/// Parameters passed by the client when executing a prepared statement.
/// They are encoded in their original format. You need to decode them accordingly,
/// depending on the data type & format code.
///
/// # Example
///
/// ```no_run
/// # use pgdog_plugin::Parameters;
/// # let parameters = Parameters::default();
/// use std::str::from_utf8;
///
/// // Format code for parameter $1.
/// let format_code = parameters.format_code(0);
///
/// // Text encoding, likely UTF-8.
/// if format_code == 0 {
///     let parameter = parameters
///         .get(0)
///         .map(|p| from_utf8(p).ok()).flatten();
/// } else {
///     // Binary encoding, data type specific.
///     let parameter = parameters.get(0);
/// }
/// ```
pub struct Parameters {
    ffi: PdParameters,
}

impl Default for Parameters {
    fn default() -> Self {
        Self {
            ffi: PdParameters {
                num_format_codes: 0,
                format_codes: null::<c_void>() as *mut c_void,
                num_params: 0,
                params: null::<c_void>() as *mut c_void,
            },
        }
    }
}

impl Parameters {
    /// Returns a list of parameter format codes, encoded in network-byte order.
    pub fn format_codes(&self) -> &[Bytes] {
        unsafe {
            from_raw_parts(
                self.ffi.format_codes as *const Bytes,
                self.ffi.num_format_codes,
            )
        }
    }

    /// Returns the format code (1 for binary, 0 for text) for the
    /// given parameter number.
    ///
    /// # Note
    ///
    /// Postgres statements encode parameters starting at 1, e.g., `$1`. We use zero offsets,
    /// so the parameter number for the first parameter accepted by this function is **`0`**, not `1`.
    ///
    pub fn format_code(&self, parameter: usize) -> i16 {
        let format_codes = self.format_codes();

        match format_codes.len() {
            0 => 0, // Text is default.
            1 => {
                let mut cursor = Cursor::new(&format_codes[0]);
                cursor.get_i16()
            }
            _ => {
                if let Some(code) = format_codes.get(parameter) {
                    let mut cursor = Cursor::new(code);
                    cursor.get_i16()
                } else {
                    0 // Text is default
                }
            }
        }
    }

    /// Returns a list of parameters. Depending on format code and data type,
    /// they are encoded in text (UTF-8, likely), or in network-byte order.
    pub fn parameters(&self) -> &[Bytes] {
        unsafe { from_raw_parts(self.ffi.params as *const Bytes, self.ffi.num_params) }
    }

    /// Returns parameter value, as binary data. Depending on the format code,
    /// it can be decoded as text (using [`std::str::from_utf8`] for example) or as binary encoding,
    /// depending on the data type.
    ///
    /// # Note
    ///
    /// Postgres statements encode parameters starting at 1, e.g., `$1`. We use zero offsets,
    /// so the parameter number for the first parameter accepted by this function is **`0`**, not `1`.
    ///
    pub fn get(&self, parameter: usize) -> Option<&[u8]> {
        if let Some(bytes) = self.parameters().get(parameter) {
            Some(&bytes[..])
        } else {
            None
        }
    }

    /// # Safety
    ///
    /// **Do not use.** Only for doc tests.
    ///
    pub unsafe fn docs_test() -> Self {
        Self {
            ffi: PdParameters {
                num_format_codes: 0,
                format_codes: null::<c_void>() as *mut c_void,
                num_params: 0,
                params: null::<c_void>() as *mut c_void,
            },
        }
    }

    /// Create new parameters.
    ///
    /// # Safety
    ///
    /// Caller is responsible for ensuring the parameter reference live long enough.
    /// This is not checked by the compiler.
    pub unsafe fn new(format_codes: &[Bytes], params: &[Bytes]) -> Self {
        Self {
            ffi: PdParameters {
                num_format_codes: format_codes.len(),
                format_codes: format_codes.as_ptr() as *mut c_void,
                num_params: params.len(),
                params: params.as_ptr() as *mut c_void,
            },
        }
    }

    /// Return the raw data.
    ///
    /// # Safety
    ///
    /// Caller is responsible for ensuring `self` lives long enough.
    ///
    pub unsafe fn ffi(&self) -> PdParameters {
        self.ffi
    }
}
