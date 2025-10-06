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
        payload.put_u8(b'k');
        payload.put_i64(self.wal_end);
        payload.put_i64(self.system_clock);
        payload.put_u8(self.reply);

        Ok(payload.freeze())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keep_alive_roundtrip_and_reply_flag() {
        let ka = KeepAlive {
            wal_end: 9876,
            system_clock: 5432,
            reply: 1,
        };

        assert!(ka.reply());

        let bytes = ka.to_bytes().expect("serialize keepalive");
        let decoded = KeepAlive::from_bytes(bytes).expect("decode keepalive");

        assert_eq!(decoded.wal_end, 9876);
        assert_eq!(decoded.system_clock, 5432);
        assert_eq!(decoded.reply, 1);
        assert!(decoded.reply());

        let wrapped = decoded.wrapped().expect("wrap keepalive copydata");
        let meta = wrapped.replication_meta().expect("decode replication meta");
        matches!(meta, ReplicationMeta::KeepAlive(_))
            .then_some(())
            .expect("should be keepalive meta");
    }
}
