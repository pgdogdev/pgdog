use super::super::code;
use super::super::prelude::*;

#[derive(Debug, Clone)]
pub struct KeepAlive {
    pub wal_end: i64,
    pub system_clock: i64,
    pub reply: u8,
}

impl FromBytes for KeepAlive {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'k');
        Ok(Self {
            wal_end: bytes.get_i64(),
            system_clock: bytes.get_i64(),
            reply: bytes.get_u8(),
        })
    }
}
