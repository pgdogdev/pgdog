use std::io::Read;

use bytes::Buf;

use super::super::Error;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(super) struct Header {
    pub(super) flags: i32,
    pub(super) has_oid: bool,
    pub(super) header_extension: i32,
}

impl Header {
    pub(super) fn read(buf: &mut impl Buf) -> Result<Self, Error> {
        let mut signature = vec![0u8; 11];
        buf.reader().read_exact(&mut signature)?;

        let mut expected = b"PGCOPY\n".to_vec();
        expected.push(3u8); // Not sure how to escape these.
        expected.push(7u8);
        expected.push(7u8);
        expected.push('\r' as u8);
        expected.push('\n' as u8);
        expected.push('\0' as u8);

        if signature != expected {
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
        15
    }
}
