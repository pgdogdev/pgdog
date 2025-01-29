use std::str::from_utf8;

use super::super::super::bind::Format;
use super::super::super::prelude::*;
use super::string::unescape;

#[derive(Clone)]
pub struct TupleData {
    pub columns: Vec<Column>,
}

impl std::fmt::Debug for TupleData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.to_sql() {
            Ok(tuple) => f
                .debug_struct("TupleData")
                .field("columns", &tuple)
                .finish(),
            Err(_) => f
                .debug_struct("TupleData")
                .field("columns", &self.columns)
                .finish(),
        }
    }
}

impl TupleData {
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

    pub fn to_sql(&self) -> Result<String, Error> {
        let columns = self
            .columns
            .iter()
            .map(|s| s.to_sql())
            .collect::<Result<Vec<_>, Error>>()?
            .join(", ");
        Ok(format!("({})", columns))
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
    /// Convert column to SQL representation,
    /// if it's encoded with UTF-8 compatible encoding.
    pub fn to_sql(&self) -> Result<String, Error> {
        match self.identifier {
            Identifier::Null => Ok("NULL".into()),
            Identifier::Format(Format::Binary) => Err(Error::NotTextEncoding),
            Identifier::Toasted => Ok("NULL".into()),
            Identifier::Format(Format::Text) => match from_utf8(&self.data[..]) {
                Ok(text) => Ok(unescape(text)),
                Err(_) => Err(Error::NotTextEncoding),
            },
        }
    }

    /// Get UTF-8 representation of the data,
    /// if data is encoded with UTF-8.
    pub fn as_str(&self) -> Option<&str> {
        from_utf8(&self.data[..]).ok()
    }
}

impl FromBytes for TupleData {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        Self::from_buffer(&mut bytes)
    }
}
