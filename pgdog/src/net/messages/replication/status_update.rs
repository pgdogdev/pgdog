use bytes::BytesMut;

use crate::backend::replication::publisher::Lsn;
use crate::net::replication::KeepAlive;
use crate::net::replication::ReplicationMeta;
use crate::net::CopyData;
use crate::util::postgres_now;

use super::super::code;
use super::super::prelude::*;

#[derive(Debug, Clone)]
pub(crate) struct StatusUpdate {
    pub(crate) last_written: i64,
    pub(crate) last_flushed: i64,
    pub(crate) last_applied: i64,
    pub(crate) system_clock: i64,
    pub(crate) reply: u8,
}

impl StatusUpdate {
    pub(crate) fn wrapped(self) -> Result<CopyData, Error> {
        Ok(CopyData::new(
            &ReplicationMeta::StatusUpdate(self).to_bytes()?,
        ))
    }

    /// Generate a request from peer to update me now
    /// with latest lsn.
    pub(crate) fn new_reply(lsn: Lsn) -> Self {
        Self {
            last_applied: lsn.lsn,
            last_flushed: lsn.lsn,
            last_written: lsn.lsn,
            system_clock: postgres_now(),
            reply: 1,
        }
    }
}

impl From<KeepAlive> for StatusUpdate {
    fn from(value: KeepAlive) -> Self {
        Self {
            last_written: value.wal_end,
            last_flushed: value.wal_end,
            last_applied: value.wal_end,
            system_clock: postgres_now(),
            reply: 0,
        }
    }
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

impl ToBytes for StatusUpdate {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let mut payload = BytesMut::new();
        payload.put_u8(b'r');

        payload.put_i64(self.last_written);
        payload.put_i64(self.last_flushed);
        payload.put_i64(self.last_applied);
        payload.put_i64(self.system_clock);
        payload.put_u8(self.reply);

        Ok(payload.freeze())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_status_update() {
        let su = StatusUpdate {
            last_applied: 1,
            last_flushed: 2,
            last_written: 3,
            system_clock: 4,
            reply: 5,
        };
        let su = StatusUpdate::from_bytes(su.to_bytes().unwrap()).unwrap();
        assert_eq!(su.last_applied, 1);
        assert_eq!(su.last_flushed, 2);
        assert_eq!(su.last_written, 3);
        assert_eq!(su.system_clock, 4);

        let cd = su.wrapped().unwrap();
        let su = cd.replication_meta().unwrap();
        match su {
            ReplicationMeta::StatusUpdate(su) => {
                assert_eq!(su.last_applied, 1);
                assert_eq!(su.last_flushed, 2);
                assert_eq!(su.last_written, 3);
                assert_eq!(su.system_clock, 4);
            }
            _ => panic!("not a status update"),
        }
    }
}
