use bytes::BytesMut;
use pgdog_postgres_types::Oid;

use super::super::super::code;
use super::super::super::prelude::*;
use super::tuple_data::TupleData;

/// WAL INSERT record. Use with [`Table::insert`](crate::backend::replication::logical::publisher::Table::insert)
/// or [`Table::upsert`](crate::backend::replication::logical::publisher::Table::upsert).
#[derive(Debug, Clone)]
pub(crate) struct Insert {
    pub(crate) oid: Oid,
    pub(crate) tuple_data: TupleData,
}

impl ToBytes for Insert {
    fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(b'I');
        buf.put_u32(self.oid.0);
        buf.put_u8(b'N');
        buf.put(self.tuple_data.to_bytes());
        buf.freeze()
    }
}

impl FromBytes for Insert {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'I');

        let oid = Oid(bytes.get_u32());
        code!(bytes, 'N');
        let tuple_data = TupleData::from_bytes(bytes)?;

        Ok(Self { oid, tuple_data })
    }
}
