//! PostgreSQL session identity: `SET SESSION AUTHORIZATION` and `SET ROLE`.
//!
//! PostgreSQL marks both settings `GUC_NO_RESET_ALL`, so `RESET ALL` leaves
//! them alone. Changing the session authorization also clears the role,
//! which means it has to be applied first.

/// Changed by `SET SESSION AUTHORIZATION`.
pub(crate) const SESSION_AUTHORIZATION: &str = "session_authorization";
/// Changed by `SET ROLE`.
pub(crate) const ROLE: &str = "role";

/// Is this parameter part of the session identity?
pub(crate) fn is_session_identity(name: &str) -> bool {
    is_session_authorization(name) || name.eq_ignore_ascii_case(ROLE)
}

/// Is this `session_authorization`?
pub(crate) fn is_session_authorization(name: &str) -> bool {
    name.eq_ignore_ascii_case(SESSION_AUTHORIZATION)
}
