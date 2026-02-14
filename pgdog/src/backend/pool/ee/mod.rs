use crate::backend::{databases::User, Schema, Shard};

pub(crate) fn schema_changed_hook(_schema: &Schema, _user: &User, _shard: &Shard) {}
