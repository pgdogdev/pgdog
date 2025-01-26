use super::super::code;
use super::super::prelude::*;

#[derive(Debug, Clone)]
pub struct StatusUpdate {
    pub last_written: i64,
    pub last_flushed: i64,
    pub last_applied: i64,
    pub system_clock: i64,
    pub reply: u8,
}

impl FromBytes for StatusUpdate {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'r');

        Ok(Self {
            last_written: bytes.get_i64(),
            last_flushed: bytes.get_i64(),
            last_applied: bytes.get_i64(),
            system_clock: bytes.get_i64(),
            reply: bytes.get_u8(),
        })
    }
}
