//! Bytes wrapper to make sure we create payloads
//! with the correct length.

use bytes::{BufMut, Bytes, BytesMut};
use std::ops::{Deref, DerefMut};

/// Payload wrapper.
pub struct Payload {
    bytes: BytesMut,
    /// Bytes of header already written into `bytes` (name byte, if any,
    /// plus the length placeholder, if any).
    header_len: usize,
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
        let mut bytes = BytesMut::with_capacity(4);
        bytes.put_i32(0); // Length placeholder, patched in on freeze().

        Self {
            bytes,
            header_len: 4,
            with_len: true,
        }
    }

    pub(crate) fn reserve(&mut self, capacity: usize) {
        self.bytes.reserve(capacity);
    }

    /// Create new named payload.
    pub fn named(name: char) -> Self {
        let mut bytes = BytesMut::with_capacity(5);
        bytes.put_u8(name as u8);
        bytes.put_i32(0); // Length placeholder, patched in on freeze().

        Self {
            bytes,
            header_len: 5,
            with_len: true,
        }
    }

    pub fn wrapped(name: char) -> Self {
        let mut bytes = BytesMut::with_capacity(1);
        bytes.put_u8(name as u8);

        Self {
            bytes,
            header_len: 1,
            with_len: false,
        }
    }

    pub fn raw() -> Self {
        Self {
            bytes: BytesMut::new(),
            header_len: 0,
            with_len: false,
        }
    }

    /// Finish assembly and return final bytes array.
    ///
    /// The header (name byte and/or length) was already written in place
    /// when the payload was created, so this just patches the length
    /// field, if any, and freezes the buffer. No extra allocation or copy.
    pub fn freeze(mut self) -> Bytes {
        if self.with_len {
            let name_len = self.header_len - 4;
            let len = (self.bytes.len() - name_len) as i32;
            self.bytes[name_len..self.header_len].copy_from_slice(&len.to_be_bytes());
        }

        self.bytes.freeze()
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
