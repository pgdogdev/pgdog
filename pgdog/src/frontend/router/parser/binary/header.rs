use std::ops::Deref;

use bytes::{Buf, BufMut, BytesMut};
use once_cell::sync::Lazy;

use crate::net::messages::ToBytes;

use super::super::Error;

static SIGNATURE: Lazy<Vec<u8>> = Lazy::new(|| {
    let mut expected = b"PGCOPY\n".to_vec();
    expected.push(255); // Not sure how to escape these.
    expected.push(b'\r');
    expected.push(b'\n');
    expected.push(b'\0');

    expected
});

/// Get binary COPY signature.
pub fn binary_signature() -> &'static Vec<u8> {
    SIGNATURE.deref()
}

/// Total bytes required for a complete header.
const HEADER_SIZE: usize = 11 + 4 + 4; // signature + flags + extension

#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub struct Header {
    pub(super) flags: i32,
    pub(super) has_oid: bool,
    pub(super) header_extension: i32,
}

impl Header {
    pub(super) fn read(buf: &mut impl Buf) -> Result<Option<Self>, Error> {
        if buf.remaining() < HEADER_SIZE {
            return Ok(None);
        }

        let signature: Vec<u8> = buf.copy_to_bytes(SIGNATURE.len()).to_vec();

        if signature != *SIGNATURE {
            return Err(Error::BinaryMissingHeader);
        }

        let flags = buf.get_i32();
        let header_extension = buf.get_i32();
        let has_oids = (flags | 0b0000_0000_0000_0000_1000_0000_0000_0000) == flags;

        if header_extension != 0 {
            return Err(Error::BinaryHeaderExtension);
        }

        Ok(Some(Self {
            flags,
            has_oid: has_oids,
            header_extension,
        }))
    }

    pub(super) fn bytes_read(&self) -> usize {
        SIGNATURE.len() + std::mem::size_of::<i32>() * 2
    }

    pub fn new() -> Self {
        Self {
            flags: 0,
            has_oid: false,
            header_extension: 0,
        }
    }
}

impl ToBytes for Header {
    fn to_bytes(&self) -> Result<bytes::Bytes, crate::net::Error> {
        let mut payload = BytesMut::new();
        payload.extend(SIGNATURE.iter());
        payload.put_i32(self.flags);
        payload.put_i32(self.header_extension);

        Ok(payload.freeze())
    }
}
