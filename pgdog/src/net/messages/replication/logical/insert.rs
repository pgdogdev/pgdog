use super::super::super::code;
use super::super::super::prelude::*;
use super::tuple_data::TupleData;

#[derive(Debug, Clone)]
pub struct Insert {
    pub xid: Option<i32>,
    pub oid: i32,
    pub tuple_data: TupleData,
}

impl FromBytes for Insert {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'I');

        // Only sent in streaming replication.
        // We are parsing logical streams.
        // let xid = bytes.get_i32();

        let oid = bytes.get_i32();
        code!(bytes, 'N');
        let tuple_data = TupleData::from_bytes(bytes)?;

        Ok(Self {
            xid: None,
            oid,
            tuple_data,
        })
    }
}
