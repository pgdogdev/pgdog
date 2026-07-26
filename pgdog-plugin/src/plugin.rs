//! PgDog's plugin interface.
//!
//! This loads the shared library using [`libloading`] and exposes
//! a safe interface to the plugin's methods.
//!

use std::ffi::c_void;
use std::path::Path;

use crate::{
    Config, Context, PdStr, Route,
    auth::{AuthContext, AuthDecision, AuthField, AuthOutcome, AuthSink, read_only_code},
    parameters::{Parameters, RawParameters},
};
use libloading::{Library, Symbol, library_filename};

/// Plugin interface.
///
/// This is the vtable of a struct which implements [`Plugin`]
// !IMPORTANT!
// Any changes to this struct *must* come with a change to the crate's minor
// version
#[derive(Debug)]
#[repr(C)]
pub struct PluginVtable {
    // SAFETY: It is critically important that this continue to be the first
    // field on this struct, so that regardless of other breakign changes in
    // the future, we can detect the use of an outdated version
    /// Plugin API version.
    pgdog_plugin_api_version: extern "C-unwind" fn() -> PdStr<'static>,
    /// Compiler version.
    rustc_version: extern "C-unwind" fn() -> PdStr<'static>,
    /// Plugin version
    plugin_version: extern "C-unwind" fn() -> PdStr<'static>,
    /// Initialization routine.
    init: extern "C-unwind" fn(),
    /// Shutdown routine.
    fini: extern "C-unwind" fn(),
    /// Configure plugin.
    config: extern "C-unwind" fn(Config<'_>) -> bool,
    /// Route query.
    #[cfg(feature = "new_parser")]
    route: extern "C-unwind" fn(
        u64,
        bool,
        bool,
        bool,
        bool,
        &pg_raw_parse::StmtList,
        RawParameters<'_>,
    ) -> Route,
    #[cfg(not(feature = "new_parser"))]
    route: extern "C-unwind" fn(
        u64,
        bool,
        bool,
        bool,
        bool,
        &pg_query::protobuf::ParseResult,
        RawParameters<'_>,
    ) -> Route,
    /// Logging initialization.
    logging_init: extern "C-unwind" fn(Config<'_>),
    /// Authenticate a client connection. Streams grant/deny strings to the
    /// sink callback and returns the decision as POD.
    authenticate: extern "C-unwind" fn(AuthContext<'_>, *mut c_void, AuthSink) -> AuthOutcome,
}

pub trait Plugin {
    /// The version of your plugin. You can simply return
    /// `env!("CARGO_PKG_VERSION").into()`
    extern "C-unwind" fn version() -> PdStr<'static>;

    /// Execute plugin's initialization routine.
    /// Returns true if the route exists and was executed, false otherwise.
    extern "C-unwind" fn init() {}
    /// Execute plugin's shutdown routine.
    extern "C-unwind" fn fini() {}
    /// Pass configuration to the plugin. Return false if loading configuration
    /// failed
    extern "C-unwind" fn config(_config: Config<'_>) -> bool {
        true
    }
    /// Execute plugin's route routine. Determines where a statement should be sent.
    /// Returns a route if the routine is defined, or `None` if not.
    ///
    /// ### Arguments
    ///
    /// * `context`: Statement context created by PgDog's query router.
    fn route(_context: Context<'_>) -> Route {
        Route::unknown()
    }

    #[doc(hidden)]
    extern "C-unwind" fn route_raw(
        shards: u64,
        has_replicas: bool,
        has_primary: bool,
        in_transaction: bool,
        write_override: bool,
        #[cfg(not(feature = "new_parser"))] query: &pg_query::protobuf::ParseResult,
        #[cfg(feature = "new_parser")] query: &pg_raw_parse::StmtList,
        params: RawParameters<'_>,
    ) -> Route {
        let context = Context {
            shards,
            has_replicas,
            has_primary,
            in_transaction,
            write_override,
            query,
            params: Parameters::from_raw(params),
        };
        Self::route(context)
    }

    /// Returns the Rust compiler version used to build the plugin.
    /// This version must match the compiler version used to build
    /// PgDog, or the plugin won't be loaded.
    extern "C-unwind" fn rustc_version() -> PdStr<'static> {
        crate::RUSTC_VERSION.into()
    }

    /// Get plugin API version based on `pgdog-plugin` crate version.
    /// This version must match the version used when building pgdog main executable,
    /// otherwise the plugin won't be loaded.
    extern "C-unwind" fn plugin_api_version() -> PdStr<'static> {
        crate::VERSION.into()
    }

    /// Initialize plugin logging with PgDog's log configuration.
    extern "C-unwind" fn logging_init(config: Config<'_>) {
        crate::logging::init(config)
    }

    /// Authenticate a client connection.
    ///
    /// Return [`AuthDecision::Skip`] (the default) to defer to the next plugin
    /// or to PgDog's configured authentication. [`AuthDecision::Allow`] accepts
    /// the client and may derive a role and provision a pool;
    /// [`AuthDecision::Deny`] rejects it (the reason is logged, never sent to
    /// the client).
    fn authenticate(_context: AuthContext<'_>) -> AuthDecision {
        AuthDecision::Skip
    }

    #[doc(hidden)]
    extern "C-unwind" fn authenticate_raw(
        context: AuthContext<'_>,
        sink_ctx: *mut c_void,
        sink: AuthSink,
    ) -> AuthOutcome {
        // Catch a panic here, inside the plugin, before it can unwind across
        // the FFI boundary. A panic that crosses `extern "C-unwind"` becomes a
        // "foreign exception" the host cannot catch, which aborts the whole
        // process; catching it here turns a buggy auth plugin into a denial
        // instead. `AuthContext` is Copy, so the closure captures it by value.
        let decision =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| Self::authenticate(context)))
                .unwrap_or_else(|_| AuthDecision::Deny("authentication plugin panicked".into()));

        // The owned decision lives here, on the plugin's stack, for the whole
        // call. We stream each field to the host as a borrowed PdStr; nothing
        // owned crosses the FFI boundary.
        match decision {
            AuthDecision::Skip => AuthOutcome::skip(),
            AuthDecision::Deny(reason) => {
                sink(sink_ctx, AuthField::Error, PdStr::from(reason.as_str()));
                AuthOutcome {
                    tag: crate::auth::AuthDecisionTag::Deny,
                    read_only: 2,
                    provision: false,
                }
            }
            AuthDecision::Allow(grant) => {
                if let Some(value) = grant.derived_user.as_deref() {
                    sink(sink_ctx, AuthField::DerivedUser, PdStr::from(value));
                }
                if let Some(value) = grant.server_role.as_deref() {
                    sink(sink_ctx, AuthField::ServerRole, PdStr::from(value));
                }
                if let Some(value) = grant.server_user.as_deref() {
                    sink(sink_ctx, AuthField::ServerUser, PdStr::from(value));
                }
                if let Some(value) = grant.server_password.as_deref() {
                    sink(sink_ctx, AuthField::ServerPassword, PdStr::from(value));
                }
                AuthOutcome {
                    tag: crate::auth::AuthDecisionTag::Allow,
                    read_only: read_only_code(grant.read_only),
                    provision: grant.provision,
                }
            }
        }
    }
}

impl PluginVtable {
    #[doc(hidden)]
    pub const fn from_plugin<T: Plugin>() -> Self {
        Self {
            init: T::init,
            fini: T::fini,
            config: T::config,
            route: T::route_raw,
            rustc_version: T::rustc_version,
            plugin_version: T::version,
            pgdog_plugin_api_version: T::plugin_api_version,
            logging_init: T::logging_init,
            authenticate: T::authenticate_raw,
        }
    }

    /// Load plugin's shared library using a cross-platform naming convention.
    ///
    /// Plugin has to be in `LD_LIBRARY_PATH`, in a standard location
    /// for the operating system, or be provided as an absolute or relative path,
    /// including the platform-specific extension.
    ///
    /// ### Example
    ///
    /// ```no_run
    /// use pgdog_plugin::PluginVtable;
    ///
    /// let plugin_lib = PluginVtable::library("/home/pgdog/plugin.so").unwrap();
    /// let plugin_lib = PluginVtable::library("plugin.so").unwrap();
    /// ```
    ///
    pub fn library<P: AsRef<Path>>(name: P) -> Result<Library, libloading::Error> {
        if name.as_ref().extension().is_some() {
            let name = name.as_ref().display().to_string();
            unsafe { Library::new(&name) }
        } else {
            let name = library_filename(name.as_ref());
            unsafe { Library::new(name) }
        }
    }

    /// Load standard plugin methods from the plugin library. Returns None
    /// if the vtable symbol was not defined (e.g. it was compiled for
    /// pgdog_plugin 0.3.0 or earlier, or did not call the plugin macro)
    pub fn load<'a>(library: &'a Library) -> Option<&'a Self> {
        // SAFETY: This symbol should have been generated by our macro, meaning
        // it is never visible to outside code
        unsafe {
            let symbol: Symbol<'a, *const Self> = library.get(b"PGDOG_PLUGIN_VTABLE\0").ok()?;
            Some(&**symbol)
        }
    }

    pub fn init(&self) {
        (self.init)()
    }

    pub fn fini(&self) {
        (self.fini)()
    }

    pub fn config(&self, config: Config<'_>) -> bool {
        (self.config)(config)
    }

    pub fn route(&self, context: Context<'_>) -> Route {
        (self.route)(
            context.shards,
            context.has_replicas,
            context.has_primary,
            context.in_transaction,
            context.write_override,
            context.query,
            context.params.as_raw(),
        )
    }

    pub fn rustc_version(&self) -> PdStr<'static> {
        (self.rustc_version)()
    }

    pub fn plugin_version(&self) -> PdStr<'static> {
        (self.plugin_version)()
    }

    pub fn pgdog_plugin_api_version(&self) -> PdStr<'static> {
        (self.pgdog_plugin_api_version)()
    }

    pub fn logging_init(&self, config: Config<'_>) {
        (self.logging_init)(config)
    }

    /// Authenticate a client. `on_field` is invoked with each grant/deny string
    /// the plugin reports (borrowed for the duration of the call); the caller
    /// copies what it needs into owned storage.
    pub fn authenticate<F: FnMut(AuthField, &str)>(
        &self,
        context: AuthContext<'_>,
        mut on_field: F,
    ) -> AuthOutcome {
        extern "C-unwind" fn trampoline<F: FnMut(AuthField, &str)>(
            ctx: *mut c_void,
            field: AuthField,
            value: PdStr<'_>,
        ) {
            // SAFETY: `ctx` is the `&mut F` passed below. The plugin only calls
            // this synchronously, on this thread, before `authenticate`
            // returns, so the borrow is live and unaliased.
            let on_field = unsafe { &mut *(ctx as *mut F) };
            on_field(field, &value);
        }

        (self.authenticate)(
            context,
            &mut on_field as *mut F as *mut c_void,
            trampoline::<F>,
        )
    }
}
