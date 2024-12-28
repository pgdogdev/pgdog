//! Bytes wrapper to make sure we create payloads
//! with the correct length.

use bytes::{BufMut, Bytes, BytesMut};
use std::ops::{Deref, DerefMut};

/// Payload wrapper.
pub struct Payload {
    bytes: BytesMut,
}

impl Payload {
    /// Create new payload.
    pub fn new() -> Self {
        Self {
            bytes: BytesMut::new(),
        }
    }

    /// Finish assembly and return final bytes array.
    pub fn freeze(self) -> Bytes {
        use super::ToBytes;
        self.to_bytes().unwrap()
    }
}

impl Deref for Payload {
    type Target = BytesMut;

    fn deref(&self) -> &Self::Target {
        &self.bytes
    }
}

impl DerefMut for Payload {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.bytes
    }
}

impl super::ToBytes for Payload {
    fn to_bytes(&self) -> Result<bytes::Bytes, crate::net::Error> {
        let mut buf = BytesMut::new();
        let len = self.bytes.len() as i32 + 4; // self

        buf.put_i32(len);
        buf.put_slice(&self.bytes);

        Ok(buf.freeze())
    }
}
