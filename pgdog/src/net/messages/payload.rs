//! Bytes wrapper to make sure we create payloads
//! with the correct length.

use bytes::{BufMut, Bytes, BytesMut};
use std::ops::{Deref, DerefMut};

/// Payload wrapper.
pub struct Payload {
    bytes: BytesMut,
    name: Option<char>,
    with_len: bool,
}

impl Default for Payload {
    fn default() -> Self {
        Self::new()
    }
}

impl Payload {
    /// Create new payload.
    pub fn new() -> Self {
        Self {
            bytes: BytesMut::new(),
            name: None,
            with_len: true,
        }
    }

    pub(crate) fn reserve(&mut self, capacity: usize) {
        self.bytes.reserve(capacity);
    }

    /// Create new named payload.
    pub fn named(name: char) -> Self {
        Self {
            bytes: BytesMut::new(),
            name: Some(name),
            with_len: true,
        }
    }

    pub fn wrapped(name: char) -> Self {
        Self {
            bytes: BytesMut::new(),
            name: Some(name),
            with_len: false,
        }
    }

    /// Finish assembly and return final bytes array.
    pub fn freeze(self) -> Bytes {
        use super::ToBytes;
        self.to_bytes().unwrap()
    }

    /// Add a C-style string to the payload. It will be NULL-terminated
    /// automatically.
    pub fn put_string(&mut self, string: &str) {
        self.bytes.reserve(string.len() + 1);
        self.bytes.put_slice(string.as_bytes());
        self.bytes.put_u8(0);
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
        let len = if self.with_len {
            Some(self.bytes.len() as i32 + 4) // self
        } else {
            None
        };

        let mut buf = BytesMut::with_capacity(match len {
            Some(len) => len as usize + 5,
            None => 15,
        });

        if let Some(name) = self.name {
            buf.put_u8(name as u8);
        }

        if let Some(len) = len {
            buf.put_i32(len);
        }
        buf.put_slice(&self.bytes);

        Ok(buf.freeze())
    }
}
