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
    /// Read-only flag, encoded as [`AuthOutcome::READ_ONLY_FALSE`],
    /// [`AuthOutcome::READ_ONLY_TRUE`] or [`AuthOutcome::READ_ONLY_UNSET`].
    /// Decode it with [`AuthOutcome::read_only_flag`].
    pub read_only: u8,
    /// Auto-provision the derived user's pool.
    pub provision: bool,
}

impl AuthOutcome {
    /// [`AuthOutcome::read_only`] code for a read-write pool.
    pub const READ_ONLY_FALSE: u8 = 0;
    /// [`AuthOutcome::read_only`] code for a read-only pool.
    pub const READ_ONLY_TRUE: u8 = 1;
    /// [`AuthOutcome::read_only`] code for a flag the plugin left unset.
    pub const READ_ONLY_UNSET: u8 = 2;

    /// The neutral "defer to the next plugin" outcome.
    pub(crate) const fn skip() -> Self {
        Self {
            tag: AuthDecisionTag::Skip,
            read_only: Self::READ_ONLY_UNSET,
            provision: false,
        }
    }

    /// Decode [`AuthOutcome::read_only`] into the flag the plugin returned in
    /// its [`AuthGrant`].
    ///
    /// Codes this crate does not define decode to `None`, so a plugin built
    /// against a newer version cannot make the host act on a value it does not
    /// understand.
    pub const fn read_only_flag(&self) -> Option<bool> {
        match self.read_only {
            Self::READ_ONLY_FALSE => Some(false),
            Self::READ_ONLY_TRUE => Some(true),
            _ => None,
        }
    }
}

/// Encode an `Option<bool>` read-only flag as its [`AuthOutcome::read_only`]
/// code. The inverse of [`AuthOutcome::read_only_flag`].
pub(crate) const fn read_only_code(value: Option<bool>) -> u8 {
    match value {
        None => AuthOutcome::READ_ONLY_UNSET,
        Some(false) => AuthOutcome::READ_ONLY_FALSE,
        Some(true) => AuthOutcome::READ_ONLY_TRUE,
    }
}

/// Callback the plugin invokes to hand a borrowed field value to the host.
///
/// The first argument is an opaque host pointer passed straight back; the host
/// side reconstructs its closure from it. Only ever called synchronously from
/// within the authenticate call, on the same thread.
///
/// The host implementation must not panic: it is reached from the plugin
/// through an `extern "C-unwind"` call, and an unwind back out of the shared
/// library is a foreign exception the host cannot catch. See
/// [`PluginVtable::authenticate`](crate::PluginVtable::authenticate).
pub type AuthSink = extern "C-unwind" fn(*mut c_void, AuthField, PdStr<'_>);

#[cfg(test)]
mod test {
    use super::*;

    fn outcome(read_only: u8) -> AuthOutcome {
        AuthOutcome {
            tag: AuthDecisionTag::Allow,
            read_only,
            provision: false,
        }
    }

    #[test]
    fn test_read_only_round_trip() {
        for value in [None, Some(true), Some(false)] {
            assert_eq!(outcome(read_only_code(value)).read_only_flag(), value);
        }
    }

    #[test]
    fn test_unknown_read_only_code_is_unset() {
        assert_eq!(outcome(7).read_only_flag(), None);
        assert_eq!(AuthOutcome::skip().read_only_flag(), None);
    }
}
