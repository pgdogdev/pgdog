use crate::backend::pool::Address;

pub struct Shard {
    pub(super) primary: Option<Address>,
    pub(super) replicas: Vec<Address>,
}
