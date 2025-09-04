use crate::net::c_string_buf;
use crate::net::messages::replication::logical::string::escape;

use super::super::super::code;
use super::super::super::prelude::*;

#[derive(Debug, Clone)]
pub(crate) struct Relation {
    pub(crate) oid: i32,
    pub(crate) namespace: String,
    pub(crate) name: String,
    pub(crate) replica_identity: i8,
    pub(crate) columns: Vec<Column>,
}

impl Relation {
    pub(crate) fn to_sql(&self) -> Result<String, Error> {
        Ok(format!(
            r#""{}"."{}""#,
            escape(&self.namespace, '"'),
            escape(&self.name, '"')
        ))
    }

    /// Columns in the order they appear in the table
    /// (and all subsequent data messages).
    pub(crate) fn columns(&self) -> Vec<&str> {
        self.columns
            .iter()
            .map(|column| column.name.as_str())
            .collect()
    }

    /// Table name.
    pub(crate) fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Column {
    pub(crate) flag: i8,
    pub(crate) name: String,
    pub(crate) oid: i32,
    pub(crate) type_modifier: i32,
}

impl Column {
    pub(crate) fn to_sql(&self) -> Result<String, Error> {
        Ok(format!(r#""{}""#, escape(&self.name, '"')))
    }
}

impl ToBytes for Relation {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let mut payload = Payload::wrapped('R');
        payload.put_i32(self.oid);
        payload.put_string(&self.namespace);
        payload.put_string(&self.name);
        payload.put_i8(self.replica_identity);
        payload.put_i16(self.columns.len() as i16);

        for column in &self.columns {
            payload.put_i8(column.flag);
            payload.put_string(&column.name);
            payload.put_i32(column.oid);
            payload.put_i32(column.type_modifier);
        }

        Ok(payload.freeze())
    }
}

impl FromBytes for Relation {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'R');
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
            oid,
            namespace,
            name,
            replica_identity,
            columns,
        })
    }
}
