use super::super::super::bind::Format;
use super::super::super::prelude::*;

#[derive(Debug, Clone)]
pub struct TupleData {
    pub columns: Vec<Column>,
}

/// Explains what's inside the column.
#[derive(Debug, Clone)]
pub enum Identifier {
    Format(Format),
    Null,
    Toasted,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub identifier: Identifier,
    pub len: i32,
    pub data: Bytes,
}

impl FromBytes for TupleData {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        let num_columns = bytes.get_i16();
        let mut columns = vec![];

        for _ in 0..num_columns {
            let ident = bytes.get_u8() as char;
            let identifier = match ident {
                'n' => Identifier::Null,
                'u' => Identifier::Toasted,
                't' => Identifier::Format(Format::Text),
                'b' => Identifier::Format(Format::Binary),
                other => return Err(Error::UnknownTupleDataIdentifier(other)),
            };
            let len = bytes.get_i32();
            let data = bytes.split_to(len as usize);

            columns.push(Column {
                identifier,
                len,
                data,
            });
        }

        Ok(Self { columns })
    }
}
