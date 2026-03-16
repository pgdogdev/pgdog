use bytes::BytesMut;

use crate::net::replication::logical::tuple_data::Identifier;

use super::super::super::code;
use super::super::super::prelude::*;
use super::tuple_data::TupleData;

#[derive(Debug, Clone)]
pub struct Delete {
    pub oid: i32,
    pub key: Option<TupleData>,
    pub old: Option<TupleData>,
}

impl Delete {
    pub fn key_non_null(&self) -> Option<TupleData> {
        if let Some(ref key) = self.key {
            let columns = key
                .columns
                .clone()
                .into_iter()
                .filter(|column| column.identifier != Identifier::Null)
                .collect();

            Some(TupleData { columns })
        } else {
            None
        }
    }
}

impl ToBytes for Delete {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let mut buf = BytesMut::new();
        buf.put_u8(b'D');
        buf.put_i32(self.oid);
        if let Some(ref key) = self.key {
            buf.put_u8(b'K');
            buf.put(key.to_bytes()?);
        } else if let Some(ref old) = self.old {
            buf.put_u8(b'O');
            buf.put(old.to_bytes()?);
        }
        Ok(buf.freeze())
    }
}

impl FromBytes for Delete {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'D');
        let oid = bytes.get_i32();
        let identifier = bytes.get_u8() as char;

        let key = if identifier == 'K' {
            Some(TupleData::from_bytes(bytes.clone())?)
        } else {
            None
        };

        let old = if identifier == 'O' {
            Some(TupleData::from_bytes(bytes)?)
        } else {
            None
        };

        Ok(Self { oid, key, old })
    }
}
