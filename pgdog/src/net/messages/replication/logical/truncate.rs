use super::super::super::code;
use super::super::super::prelude::*;

#[derive(Debug, Clone)]
pub(crate) struct Truncate {
    pub(crate) num_relations: i32,
    pub(crate) options: i8,
    pub(crate) oid: i32,
}

impl FromBytes for Truncate {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'T');
        Ok(Self {
            num_relations: bytes.get_i32(),
            options: bytes.get_i8(),
            oid: bytes.get_i32(),
        })
    }
}
