//! RowDescription (B) message.

use crate::net::c_string_buf;

use super::{code, DataType};
use super::{prelude::*, Format};

/// Column field description.
#[derive(Clone, Debug, PartialEq)]
pub struct Field {
    /// Name of the field.
    pub name: String,
    /// Table OID.
    pub table_oid: i32,
    /// Column number.
    pub column: i16,
    /// Type OID.
    pub type_oid: i32,
    /// Type size.
    pub type_size: i16,
    /// Type modifier.
    pub type_modifier: i32,
    /// Format code.
    pub format: i16,
}

impl Field {
    /// Numeric field.
    pub fn numeric(name: &str) -> Self {
        Self {
            name: name.into(),
            table_oid: 0,
            column: 0,
            type_oid: 1700,
            type_size: -1,
            type_modifier: -1,
            format: 0, // We always use text format.
        }
    }

    /// Text field.
    pub fn text(name: &str) -> Self {
        Self {
            name: name.into(),
            table_oid: 0,
            column: 0,
            type_oid: 25,
            type_size: -1,
            type_modifier: -1,
            format: 0, // We always use text format.
        }
    }

    /// Boolean field.
    pub fn bool(name: &str) -> Self {
        Self {
            name: name.into(),
            table_oid: 0,
            column: 0,
            type_oid: 16,
            type_size: 1,
            type_modifier: -1,
            format: 0, // We always use text format.
        }
    }

    /// Encoded with text encoding.
    pub fn is_text_encoding(&self) -> bool {
        self.format == 0
    }

    /// Encoded with binary encoding.
    pub fn is_binary_encoding(&self) -> bool {
        !self.is_text_encoding()
    }

    /// Get the column data type.
    pub fn data_type(&self) -> DataType {
        match self.type_oid {
            16 => DataType::Bool,
            20 => DataType::Bigint,
            23 => DataType::Integer,
            21 => DataType::SmallInt,
            25 => DataType::Text,
            700 => DataType::Real,
            701 => DataType::DoublePrecision,
            1043 => DataType::Text,
            1114 => DataType::Timestamp,
            1184 => DataType::TimestampTz,
            1186 => DataType::Interval,
            2950 => DataType::Uuid,
            _ => DataType::Other(self.type_oid),
        }
    }

    pub fn format(&self) -> Format {
        match self.format {
            0 => Format::Text,
            _ => Format::Binary,
        }
    }

    /// This is an integer.
    pub fn is_int(&self) -> bool {
        use DataType::*;
        matches!(self.data_type(), Bigint | Integer | SmallInt)
    }

    /// This is a float.
    pub fn is_float(&self) -> bool {
        use DataType::*;
        matches!(self.data_type(), Real | DoublePrecision)
    }

    /// This is a varchar.
    pub fn is_varchar(&self) -> bool {
        use DataType::*;
        self.data_type() == Text
    }
}

/// RowDescription message.
#[derive(Debug, Clone, PartialEq)]
pub struct RowDescription {
    /// Fields.
    pub fields: Vec<Field>,
}

impl RowDescription {
    /// Create new row description from fields.
    pub fn new(fields: &[Field]) -> Self {
        Self {
            fields: fields.to_vec(),
        }
    }

    /// Get field info.
    pub fn field(&self, index: usize) -> Option<&Field> {
        self.fields.get(index)
    }

    /// Get field index name, O(n).
    pub fn field_index(&self, name: &str) -> Option<usize> {
        for (index, field) in self.fields.iter().enumerate() {
            if field.name == name {
                return Some(index);
            }
        }

        None
    }

    /// Check if the two row descriptions are materially the same.
    pub fn equivalent(&self, other: &RowDescription) -> bool {
        for (a, b) in self.fields.iter().zip(other.fields.iter()) {
            if a.name != b.name {
                return false;
            }

            if a.type_oid != b.type_oid {
                return false;
            }
        }

        true
    }
}

impl FromBytes for RowDescription {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'T');
        let _len = bytes.get_i32();

        let fields = (0..bytes.get_i16())
            .map(|_| Field {
                name: c_string_buf(&mut bytes),
                table_oid: bytes.get_i32(),
                column: bytes.get_i16(),
                type_oid: bytes.get_i32(),
                type_size: bytes.get_i16(),
                type_modifier: bytes.get_i32(),
                format: bytes.get_i16(),
            })
            .collect();

        Ok(Self { fields })
    }
}

impl ToBytes for RowDescription {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let mut payload = Payload::named(self.code());
        payload.put_i16(self.fields.len() as i16);

        for field in &self.fields {
            payload.put_string(&field.name);
            payload.put_i32(field.table_oid);
            payload.put_i16(field.column);
            payload.put_i32(field.type_oid);
            payload.put_i16(field.type_size);
            payload.put_i32(field.type_modifier);
            payload.put_i16(field.format);
        }

        Ok(payload.freeze())
    }
}

impl Protocol for RowDescription {
    fn code(&self) -> char {
        'T'
    }
}
