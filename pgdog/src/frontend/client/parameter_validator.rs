//! Validate that the params the client attempts
//! to SET are valid without talking to Postgres.

use pg_internals::guc::GUC;

use crate::frontend::SetParam;

pub(super) struct ParameterValidator<'a> {
    params: &'a [SetParam],
}
