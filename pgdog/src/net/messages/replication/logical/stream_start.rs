use crate::net::{code, FromBytes, ToBytes};
use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Debug, Clone)]
pub(crate) struct StreamStart {
    xid: i32,
    first: i8,
}

impl FromBytes for StreamStart {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, crate::net::Error> {
        code!(bytes, 'S');

        Ok(Self {
            xid: bytes.get_i32(),
            first: bytes.get_i8(),
        })
    }
}

impl ToBytes for StreamStart {
    fn to_bytes(&self) -> Result<Bytes, crate::net::Error> {
        let mut payload = BytesMut::new();
        payload.put_u8(b'S');
        payload.put_i32(self.xid);
        payload.put_i8(self.first);

        Ok(payload.freeze())
    }
}
