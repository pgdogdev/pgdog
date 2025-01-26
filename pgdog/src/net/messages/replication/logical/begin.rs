use bytes::BytesMut;

use super::super::super::code;
use super::super::super::prelude::*;

#[derive(Debug, Clone)]
pub struct Begin {
    final_transaction_lsn: i64,
    commit_timestamp: i64,
    xid: i32,
}

impl FromBytes for Begin {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'B');
        Ok(Self {
            final_transaction_lsn: bytes.get_i64(),
            commit_timestamp: bytes.get_i64(),
            xid: bytes.get_i32(),
        })
    }
}

impl ToBytes for Begin {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let mut bytes = BytesMut::new();
        bytes.put_u8(self.code() as u8);
        bytes.put_i64(self.final_transaction_lsn);
        bytes.put_i64(self.commit_timestamp);
        bytes.put_i32(self.xid);

        Ok(bytes.freeze())
    }
}

impl Protocol for Begin {
    fn code(&self) -> char {
        'B'
    }
}
