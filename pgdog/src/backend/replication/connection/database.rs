use crate::backend::pool::Address;

use super::Shard;

pub struct Database {
    pub(super) shards: Vec<Shard>,
}
