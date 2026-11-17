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
    name.eq_ignore_ascii_case(SESSION_AUTHORIZATION) || name.eq_ignore_ascii_case(ROLE)
}

/// Sort key that applies `session_authorization` before `role`,
/// and both before everything else.
pub(super) fn sync_order(name: &str) -> u8 {
    if name.eq_ignore_ascii_case(SESSION_AUTHORIZATION) {
        0
    } else if name.eq_ignore_ascii_case(ROLE) {
        1
    } else {
        2
    }
}
