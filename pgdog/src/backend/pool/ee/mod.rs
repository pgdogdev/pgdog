use crate::backend::{Schema, Shard, databases::User};

pub(crate) fn schema_changed_hook(_schema: &Schema, _user: &User, _shard: &Shard) {}
