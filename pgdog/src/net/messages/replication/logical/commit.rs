use bytes::BytesMut;

use super::super::super::code;
use super::super::super::prelude::*;

#[derive(Debug, Clone)]
pub struct Commit {
    pub flags: i8,
    pub commit_lsn: i64,
    pub end_lsn: i64,
    pub commit_timestamp: i64,
}

impl FromBytes for Commit {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'C');
        Ok(Self {
            flags: bytes.get_i8(),
            commit_lsn: bytes.get_i64(),
            end_lsn: bytes.get_i64(),
            commit_timestamp: bytes.get_i64(),
        })
    }
}

impl ToBytes for Commit {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let mut bytes = BytesMut::new();
        bytes.put_u8(self.code() as u8);
        bytes.put_i8(self.flags);
        bytes.put_i64(self.commit_lsn);
        bytes.put_i64(self.end_lsn);
        bytes.put_i64(self.commit_timestamp);

        Ok(bytes.freeze())
    }
}

impl Protocol for Commit {
    fn code(&self) -> char {
        'C'
    }
}
