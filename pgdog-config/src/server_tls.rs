use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::TlsVerifyMode;

/// TLS settings for connections from PgDog to a Postgres server. On a
/// database, unset fields fall back to the `[general]` settings of the
/// same name.
#[derive(
    Serialize, Deserialize, Debug, Clone, PartialEq, Default, JsonSchema, PartialOrd, Eq, Ord, Hash,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ServerTls {
    /// Overrides the `tls_verify` setting for connections to this database.
    /// Useful when servers in the same cluster require different TLS settings,
    /// e.g., managed databases where each instance has its own CA.
    pub tls_verify: Option<TlsVerifyMode>,
    /// Overrides the `tls_server_ca_certificate` setting: the CA bundle used to
    /// verify this server's certificate.
    pub tls_server_ca_certificate: Option<PathBuf>,
    /// Overrides the `tls_server_certificate` setting: the client certificate PgDog
    /// presents to this server (mTLS). Must be set together with `tls_server_private_key`;
    /// when set, the pair replaces the `[general]` pair for this database.
    pub tls_server_certificate: Option<PathBuf>,
    /// Overrides the `tls_server_private_key` setting: the private key for
    /// `tls_server_certificate`. Must be set together with `tls_server_certificate`.
    pub tls_server_private_key: Option<PathBuf>,
}
