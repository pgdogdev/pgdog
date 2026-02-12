use crate::backend::{databases::User, Schema};

pub(crate) fn schema_changed_hook(_schema: &Schema, _user: &User) {}
