//! Client authentication hook.
//!
//! Plugins can validate the credential a client presents at login (a password,
//! a JWT, an API key, ...) and, on success, derive the Postgres role the
//! session should run as and how its pool should connect to the backend.
//!
//! # No ownership crosses the FFI boundary
//!
//! [`AuthDecision`] and [`AuthGrant`] are ordinary owned Rust values used by
//! plugin authors. They never cross FFI. The generated bridge keeps the owned
//! decision alive on the plugin's stack and streams each string field to the
//! host as a borrowed [`PdStr`] through the [`AuthSink`] callback, returning
//! only the POD [`AuthOutcome`] by value. This mirrors how [`crate::Config`]
//! hands borrowed strings the other way.

use crate::PdStr;
use std::ffi::c_void;

/// Context for a single client authentication attempt.
///
/// All strings are borrowed and only valid for the duration of the
/// [`Plugin::authenticate`](crate::Plugin::authenticate) call.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AuthContext<'a> {
    /// User from the startup packet.
    pub user: PdStr<'a>,
    /// Database from the startup packet.
    pub database: PdStr<'a>,
    /// Credential the client presented (password, JWT, token, ...).
    pub credential: PdStr<'a>,
    /// Client socket address, e.g. `"10.0.0.1:54321"`.
    pub client_addr: PdStr<'a>,
    /// TLS certificate identity; empty when the connection has none.
    pub tls_identity: PdStr<'a>,
    /// Whether the connection uses TLS.
    pub tls: bool,
}

/// Backend and pool details returned when a plugin authenticates a client.
///
/// A plain owned Rust value; it never crosses the FFI boundary.
#[derive(Debug, Default, Clone)]
pub struct AuthGrant {
    /// Postgres role the pool runs as; `None` keeps the startup-packet user.
    pub derived_user: Option<String>,
    /// Role assumed on the backend via the `role` startup parameter.
    pub server_role: Option<String>,
    /// Backend user for an auto-provisioned pool.
    pub server_user: Option<String>,
    /// Backend password for an auto-provisioned pool.
    pub server_password: Option<String>,
    /// Whether the provisioned pool is read-only. `None` leaves it unset.
    pub read_only: Option<bool>,
    /// Auto-provision a pool for `derived_user` when one does not exist.
    pub provision: bool,
}

/// A plugin's verdict on a client authentication attempt.
#[derive(Debug, Clone)]
pub enum AuthDecision {
    /// Not this plugin's credential; consult the next plugin.
    Skip,
    /// Authenticated; optionally derive a role and provision a pool.
    Allow(AuthGrant),
    /// Rejected. The reason is logged by PgDog, never sent to the client.
    Deny(String),
}

/// FFI tag for an [`AuthDecision`], returned by value across the boundary.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AuthDecisionTag {
    /// Defer to the next plugin.
    Skip = 0,
    /// Client authenticated.
    Allow = 1,
    /// Client rejected.
    Deny = 2,
}

/// Which [`AuthGrant`] field (or deny reason) a plugin is reporting through the
/// [`AuthSink`] callback.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AuthField {
    /// [`AuthGrant::derived_user`].
    DerivedUser = 0,
    /// [`AuthGrant::server_role`].
    ServerRole = 1,
    /// [`AuthGrant::server_user`].
    ServerUser = 2,
    /// [`AuthGrant::server_password`].
    ServerPassword = 3,
    /// [`AuthDecision::Deny`] reason.
    Error = 4,
}

/// POD result of an FFI authenticate call.
///
/// String fields are not carried here; they are streamed to the host through
/// the [`AuthSink`] while the plugin-owned [`AuthDecision`] is still alive, so
/// no ownership crosses FFI.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AuthOutcome {
    /// The decision kind.
    pub tag: AuthDecisionTag,
    /// `0` = read-write, `1` = read-only, `2` = unset.
    pub read_only: u8,
    /// Auto-provision the derived user's pool.
    pub provision: bool,
}

impl AuthOutcome {
    /// The neutral "defer to the next plugin" outcome.
    pub(crate) const fn skip() -> Self {
        Self {
            tag: AuthDecisionTag::Skip,
            read_only: 2,
            provision: false,
        }
    }
}

/// Encode an `Option<bool>` read-only flag as its [`AuthOutcome::read_only`] code.
pub(crate) fn read_only_code(value: Option<bool>) -> u8 {
    match value {
        None => 2,
        Some(false) => 0,
        Some(true) => 1,
    }
}

/// Callback the plugin invokes to hand a borrowed field value to the host.
///
/// The first argument is an opaque host pointer passed straight back; the host
/// side reconstructs its closure from it. Only ever called synchronously from
/// within the authenticate call, on the same thread.
pub type AuthSink = extern "C-unwind" fn(*mut c_void, AuthField, PdStr<'_>);
