//! DataRow (B) message.

use super::code;
use super::prelude::*;

use bytes::BytesMut;

use std::str::from_utf8;

/// DataRow message.
#[derive(Debug, Clone)]
pub struct DataRow {
    columns: Vec<Bytes>,
}

/// Convert value to data row column
/// using text formatting.
pub trait ToDataRowColumn {
    fn to_data_row_column(&self) -> Bytes;
}

impl ToDataRowColumn for String {
    fn to_data_row_column(&self) -> Bytes {
        Bytes::copy_from_slice(self.as_bytes())
    }
}

impl ToDataRowColumn for &str {
    fn to_data_row_column(&self) -> Bytes {
        Bytes::copy_from_slice(self.as_bytes())
    }
}

impl ToDataRowColumn for i64 {
    fn to_data_row_column(&self) -> Bytes {
        Bytes::copy_from_slice(self.to_string().as_bytes())
    }
}

impl ToDataRowColumn for usize {
    fn to_data_row_column(&self) -> Bytes {
        Bytes::copy_from_slice(self.to_string().as_bytes())
    }
}

impl ToDataRowColumn for bool {
    fn to_data_row_column(&self) -> Bytes {
        Bytes::copy_from_slice(if *self { b"t" } else { b"f" })
    }
}

impl ToDataRowColumn for f64 {
    fn to_data_row_column(&self) -> Bytes {
        let number = format!("{:.5}", self);
        Bytes::copy_from_slice(number.as_bytes())
    }
}

impl Default for DataRow {
    fn default() -> Self {
        Self::new()
    }
}

impl DataRow {
    /// New data row.
    pub fn new() -> Self {
        Self { columns: vec![] }
    }

    /// Add a column to the data row.
    pub fn add(&mut self, value: impl ToDataRowColumn) -> &mut Self {
        self.columns.push(value.to_data_row_column());
        self
    }

    /// Create data row from columns.
    pub fn from_columns(columns: Vec<impl ToDataRowColumn>) -> Self {
        let mut dr = Self::new();
        for column in columns {
            dr.add(column);
        }
        dr
    }

    /// Get data for column at index.
    pub fn column(&self, index: usize) -> Option<Bytes> {
        self.columns.get(index).cloned()
    }

    /// Get integer at index with text/binary encoding.
    pub fn get_int(&self, index: usize, text: bool) -> Option<i64> {
        self.column(index)
            .map(|mut column| {
                if text {
                    from_utf8(&column[..])
                        .ok()
                        .map(|s| s.parse::<i64>().ok())
                        .flatten()
                } else {
                    match column.len() {
                        2 => Some(column.get_i16() as i64),
                        4 => Some(column.get_i32() as i64),
                        8 => Some(column.get_i64()),
                        _ => None,
                    }
                }
            })
            .flatten()
    }

    // Get integer at index with text/binary encoding.
    pub fn get_float(&self, index: usize, text: bool) -> Option<f64> {
        self.column(index)
            .map(|mut column| {
                if text {
                    from_utf8(&column[..])
                        .ok()
                        .map(|s| s.parse::<f64>().ok())
                        .flatten()
                } else {
                    match column.len() {
                        4 => Some(column.get_f32() as f64),
                        8 => Some(column.get_f64()),
                        _ => None,
                    }
                }
            })
            .flatten()
    }
}

impl FromBytes for DataRow {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'D');
        let _len = bytes.get_i32();
        let columns = (0..bytes.get_i16())
            .map(|_| {
                let len = bytes.get_i32() as usize;
                let mut column = BytesMut::new();
                for _ in 0..len {
                    column.put_u8(bytes.get_u8());
                }

                column.freeze()
            })
            .collect();

        Ok(Self { columns })
    }
}

impl ToBytes for DataRow {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let mut payload = Payload::named(self.code());
        payload.put_i16(self.columns.len() as i16);

        for column in &self.columns {
            payload.put_i32(column.len() as i32);
            payload.put(&column[..]);
        }

        Ok(payload.freeze())
    }
}

impl Protocol for DataRow {
    fn code(&self) -> char {
        'D'
    }
}
