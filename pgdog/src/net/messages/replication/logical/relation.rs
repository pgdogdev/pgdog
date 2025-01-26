use crate::net::c_string_buf;

use super::super::super::code;
use super::super::super::prelude::*;

#[derive(Debug, Clone)]
pub struct Relation {
    pub xid: i32,
    pub oid: i32,
    pub namespace: String,
    pub name: String,
    pub replica_identity: i8,
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub flag: i8,
    pub name: String,
    pub oid: i32,
    pub type_modifier: i32,
}

impl FromBytes for Relation {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'R');
        let xid = bytes.get_i32();
        let oid = bytes.get_i32();
        let namespace = c_string_buf(&mut bytes);
        let name = c_string_buf(&mut bytes);
        let replica_identity = bytes.get_i8();
        let num_columns = bytes.get_i16();

        let mut columns = vec![];

        for _ in 0..num_columns {
            let flag = bytes.get_i8();
            let name = c_string_buf(&mut bytes);
            let oid = bytes.get_i32();
            let type_modifier = bytes.get_i32();

            columns.push(Column {
                flag,
                name,
                oid,
                type_modifier,
            });
        }

        Ok(Self {
            xid,
            oid,
            namespace,
            name,
            replica_identity,
            columns,
        })
    }
}
