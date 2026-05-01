use std::str::from_utf8;

use bytes::BytesMut;

use crate::net::bind::Parameter;
use crate::net::Bind;

use super::super::super::bind::Format;
use super::super::super::prelude::*;
use super::string::unescape;

#[derive(Clone, Default)]
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
        if bytes.remaining() < 2 {
            return Err(Error::UnexpectedPayload);
        }
        let num_columns = bytes.get_i16();
        if num_columns < 0 {
            return Err(Error::UnexpectedPayload);
        }
        let mut columns = vec![];

        for _ in 0..num_columns {
            if bytes.remaining() < 1 {
                return Err(Error::UnexpectedPayload);
            }
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
                _ => {
                    if bytes.remaining() < 4 {
                        return Err(Error::UnexpectedPayload);
                    }
                    let l = bytes.get_i32();
                    if l < 0 {
                        return Err(Error::UnexpectedPayload);
                    }
                    l
                }
            };

            if bytes.remaining() < len as usize {
                return Err(Error::UnexpectedPayload);
            }
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

    /// Create a [`Bind`] message from this tuple. Column index N maps to parameter `$N+1`.
    /// Used by [`Table`](crate::backend::replication::logical::publisher::Table) DML methods
    /// — the `$N` they emit must agree with the column ordering of the tuple passed here.
    pub fn to_bind(&self, name: &str) -> Bind {
        let params = self
            .columns
            .iter()
            .map(|c| {
                if c.identifier == Identifier::Null {
                    Parameter::new_null()
                } else {
                    Parameter::new(&c.data)
                }
            })
            .collect::<Vec<_>>();
        Bind::new_params(name, &params)
    }

    /// Does this tuple contain any unchanged-TOAST (`'u'`) column?
    pub fn has_toasted(&self) -> bool {
        self.columns
            .iter()
            .any(|c| c.identifier == Identifier::Toasted)
    }

    /// Are every column in this tuple unchanged-TOAST (`'u'`)?
    ///
    /// True when nothing changed — used to detect no-op UPDATEs before routing.
    pub fn all_toasted(&self) -> bool {
        self.columns
            .iter()
            .all(|c| c.identifier == Identifier::Toasted)
    }

    /// Return a copy with unchanged-TOAST (`'u'`) columns removed.
    pub fn without_toasted(&self) -> TupleData {
        TupleData {
            columns: self
                .columns
                .iter()
                .filter(|c| c.identifier != Identifier::Toasted)
                .cloned()
                .collect(),
        }
    }
}

/// Explains what's inside the column.
#[derive(Debug, Clone, PartialEq)]
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
            Identifier::Toasted => Ok("<unchanged toast>".into()),
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

impl ToBytes for TupleData {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let mut buf = BytesMut::new();
        buf.put_i16(self.columns.len() as i16);
        for col in &self.columns {
            match col.identifier {
                Identifier::Null => buf.put_u8(b'n'),
                Identifier::Toasted => buf.put_u8(b'u'),
                Identifier::Format(Format::Text) => {
                    buf.put_u8(b't');
                    buf.put_i32(col.len);
                    buf.put(col.data.clone());
                }
                Identifier::Format(Format::Binary) => {
                    buf.put_u8(b'b');
                    buf.put_i32(col.len);
                    buf.put(col.data.clone());
                }
            }
        }
        Ok(buf.freeze())
    }
}

#[cfg(test)]
pub(crate) fn text_col(s: &str) -> Column {
    Column {
        identifier: Identifier::Format(Format::Text),
        len: s.len() as i32,
        data: bytes::Bytes::copy_from_slice(s.as_bytes()),
    }
}

#[cfg(test)]
pub(crate) fn toasted_col() -> Column {
    Column {
        identifier: Identifier::Toasted,
        len: 0,
        data: bytes::Bytes::new(),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_null_conversion() {
        let data = TupleData {
            columns: vec![
                Column {
                    identifier: Identifier::Null,
                    len: 0,
                    data: Bytes::new(),
                },
                Column {
                    identifier: Identifier::Format(Format::Text),
                    len: 4,
                    data: Bytes::from(String::from("1234")),
                },
            ],
        };

        let bind = data.to_bind("__pgdog_1");
        assert_eq!(bind.statement(), "__pgdog_1");
        assert!(bind.parameter(0).unwrap().unwrap().is_null());
        assert!(!bind.parameter(1).unwrap().unwrap().is_null());
        assert_eq!(bind.parameter(1).unwrap().unwrap().bigint().unwrap(), 1234);
    }

    #[test]
    fn test_truncated_buffer_rejected() {
        // Empty buffer
        assert!(TupleData::from_bytes(Bytes::new()).is_err());
        // 1 byte (need 2 for column count)
        assert!(TupleData::from_bytes(Bytes::from_static(&[0])).is_err());
        // Negative column counts are malformed
        assert!(TupleData::from_bytes(Bytes::from_static(&[0xff, 0xff])).is_err());
        // Declares 1 column but no column data follows
        assert!(TupleData::from_bytes(Bytes::from_static(&[0, 1])).is_err());
        // Has identifier byte 't' but no length
        assert!(TupleData::from_bytes(Bytes::from_static(&[0, 1, b't'])).is_err());
        // Has identifier + length but data is truncated
        assert!(TupleData::from_bytes(Bytes::from_static(&[0, 1, b't', 0, 0, 0, 4, 1])).is_err());
    }

    #[test]
    fn has_toasted_detects_u() {
        let t = TupleData {
            columns: vec![text_col("1"), toasted_col(), text_col("x")],
        };
        assert!(t.has_toasted());
        let t2 = TupleData {
            columns: vec![text_col("1"), text_col("x")],
        };
        assert!(!t2.has_toasted());
    }

    #[test]
    fn to_sql_renders_toasted_marker() {
        let c = toasted_col();
        assert_eq!(c.to_sql().unwrap(), "<unchanged toast>");
    }
}
