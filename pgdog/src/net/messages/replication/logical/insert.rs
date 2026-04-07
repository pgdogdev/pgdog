use bytes::BytesMut;
use pgdog_postgres_types::Oid;

use super::super::super::code;
use super::super::super::prelude::*;
use super::tuple_data::Column;
use super::tuple_data::TupleData;

#[derive(Debug, Clone)]
pub struct Insert {
    pub xid: Option<i32>,
    pub oid: Oid,
    pub tuple_data: TupleData,
}

impl Insert {
    /// Get column at index.
    pub fn column(&self, index: usize) -> Option<&Column> {
        self.tuple_data.columns.get(index)
    }
}

impl ToBytes for Insert {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let mut buf = BytesMut::new();
        buf.put_u8(b'I');
        buf.put_u32(self.oid.0);
        buf.put_u8(b'N');
        buf.put(self.tuple_data.to_bytes()?);
        Ok(buf.freeze())
    }
}

impl FromBytes for Insert {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'I');

        // Only sent in streaming replication.
        // We are parsing logical streams.
        // let xid = bytes.get_i32();

        let oid = Oid(bytes.get_u32());
        code!(bytes, 'N');
        let tuple_data = TupleData::from_bytes(bytes)?;

        Ok(Self {
            xid: None,
            oid,
            tuple_data,
        })
    }
}
