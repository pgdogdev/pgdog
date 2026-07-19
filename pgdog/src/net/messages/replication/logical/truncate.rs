use pgdog_postgres_types::Oid;

use super::super::super::code;
use super::super::super::prelude::*;

#[derive(Debug, Clone)]
pub struct Truncate {
    pub num_relations: i32,
    pub options: i8,
    pub oid: Oid,
}

impl FromBytes for Truncate {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'T');
        Ok(Self {
            num_relations: bytes.get_i32(),
            options: bytes.get_i8(),
            oid: Oid(bytes.get_u32()),
        })
    }
}
