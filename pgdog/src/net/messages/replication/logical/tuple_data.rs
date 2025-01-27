use super::super::super::bind::Format;
use super::super::super::prelude::*;

#[derive(Debug, Clone)]
pub struct TupleData {
    pub columns: Vec<Column>,
}

impl TupleData {
    pub fn len(&self) -> usize {
        size_of::<i16>() + self.columns.iter().map(|c| c.len()).sum::<usize>()
    }

    pub fn from_buffer(bytes: &mut Bytes) -> Result<Self, Error> {
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

            let len = match identifier {
                Identifier::Null | Identifier::Toasted => 0,
                _ => bytes.get_i32(),
            };
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

impl Column {
    /// Size of the column in the message buffer.
    pub fn len(&self) -> usize {
        self.data.len() + size_of::<u8>() + size_of::<i32>()
    }
}

impl FromBytes for TupleData {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        Self::from_buffer(&mut bytes)
    }
}
