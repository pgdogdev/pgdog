use std::io::Read;

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

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Header {
    pub(super) flags: i32,
    pub(super) has_oid: bool,
    pub(super) header_extension: i32,
}

impl Header {
    pub(super) fn read(buf: &mut impl Buf) -> Result<Self, Error> {
        let mut signature = vec![0u8; SIGNATURE.len()];
        buf.reader().read_exact(&mut signature)?;

        if signature != *SIGNATURE {
            return Err(Error::BinaryMissingHeader);
        }

        let flags = buf.get_i32();
        let header_extension = buf.get_i32();
        let has_oids = (flags | 0b0000_0000_0000_0000_1000_0000_0000_0000) == flags;

        if header_extension != 0 {
            return Err(Error::BinaryHeaderExtension);
        }

        Ok(Self {
            flags,
            has_oid: has_oids,
            header_extension,
        })
    }

    pub(super) fn bytes_read(&self) -> usize {
        SIGNATURE.len() + std::mem::size_of::<i32>() * 2
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
