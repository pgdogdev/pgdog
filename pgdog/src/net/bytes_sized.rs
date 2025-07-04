use std::ops::{Deref, DerefMut};

use bytes::BytesMut;
use datasize::DataSize;
use uuid::Bytes;

#[derive(Debug, Clone)]
pub struct BytesMutSized(BytesMut);

impl BytesMutSized {
    pub fn new() -> Self {
        Self(BytesMut::new())
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self(BytesMut::with_capacity(capacity))
    }
}

impl Deref for BytesMutSized {
    type Target = BytesMut;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for BytesMutSized {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl DataSize for BytesMutSized {
    const IS_DYNAMIC: bool = true;
    const STATIC_HEAP_SIZE: usize = 0;

    fn estimate_heap_size(&self) -> usize {
        self.0.capacity()
    }
}

pub struct BytesSized(Bytes);

impl Deref for BytesSized {
    type Target = Bytes;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DataSize for BytesSized {
    const IS_DYNAMIC: bool = false;
    const STATIC_HEAP_SIZE: usize = 0;

    fn estimate_heap_size(&self) -> usize {
        // It's a smart pointer.
        0
    }
}
