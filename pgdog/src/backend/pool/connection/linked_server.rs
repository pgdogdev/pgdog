use super::Guard;
use std::ops::{Deref, DerefMut};

#[derive(Debug)]
pub(crate) struct LinkedServer {
    pub(super) server: Guard,
    // Shard number.
    pub(super) shard: usize,
    // Parameters were sync'ed.
    pub(super) linked: bool,
}

impl Deref for LinkedServer {
    type Target = Guard;

    fn deref(&self) -> &Self::Target {
        &self.server
    }
}

impl DerefMut for LinkedServer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.server
    }
}
