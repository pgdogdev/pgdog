use std::ops::Deref;

use super::BufferWithDelay;

#[derive(Clone, Debug)]
pub struct MirrorRequest {
    pub(super) buffer: Vec<BufferWithDelay>,
}

impl Deref for MirrorRequest {
    type Target = Vec<BufferWithDelay>;

    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}
