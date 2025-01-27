use super::super::super::code;
use super::super::super::prelude::*;
use super::tuple_data::TupleData;

#[derive(Debug, Clone)]
pub struct Delete {
    pub oid: i32,
    pub key: Option<TupleData>,
    pub old: Option<TupleData>,
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
