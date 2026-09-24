use std::ops::{Deref, DerefMut};

use crate::backend::pool::Guard;

#[derive(Debug)]
pub(crate) struct MultiBinding {
    guard: Guard,
    shard: usize,
}

impl MultiBinding {
    pub(crate) fn new(guard: Guard, shard: usize) -> Self {
        Self { guard, shard }
    }

    pub(crate) fn shard(&self) -> usize {
        self.shard
    }
}

impl Deref for MultiBinding {
    type Target = Guard;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl DerefMut for MultiBinding {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}
