use bytes::BytesMut;

use crate::net::replication::ReplicationMeta;
use crate::net::CopyData;

use super::super::code;
use super::super::prelude::*;

#[derive(Debug, Clone)]
pub struct KeepAlive {
    pub wal_end: i64,
    pub system_clock: i64,
    pub reply: u8,
}

impl KeepAlive {
    pub fn wrapped(self) -> Result<CopyData, Error> {
        Ok(CopyData::new(&ReplicationMeta::KeepAlive(self).to_bytes()?))
    }

    /// Origin expects reply.
    pub fn reply(&self) -> bool {
        self.reply == 1
    }
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

impl ToBytes for KeepAlive {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let mut payload = BytesMut::new();
        payload.put_u8('k' as u8);
        payload.put_i64(self.wal_end);
        payload.put_i64(self.system_clock);
        payload.put_u8(self.reply);

        Ok(payload.freeze())
    }
}
