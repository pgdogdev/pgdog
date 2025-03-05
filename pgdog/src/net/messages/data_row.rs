//! DataRow (B) message.

use super::{code, prelude::*, Datum, Format, FromDataType, Numeric, RowDescription};
use bytes::BytesMut;
use std::ops::Deref;

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

impl ToDataRowColumn for &String {
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

impl ToDataRowColumn for u64 {
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

impl ToDataRowColumn for u128 {
    fn to_data_row_column(&self) -> Bytes {
        Bytes::copy_from_slice(self.to_string().as_bytes())
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
    #[inline]
    pub fn column(&self, index: usize) -> Option<Bytes> {
        self.columns.get(index).cloned()
    }

    /// Get integer at index with text/binary encoding.
    pub fn get_int(&self, index: usize, text: bool) -> Option<i64> {
        self.get::<i64>(index, if text { Format::Text } else { Format::Binary })
    }

    // Get float at index with text/binary encoding.
    pub fn get_float(&self, index: usize, text: bool) -> Option<f64> {
        self.get::<Numeric>(index, if text { Format::Text } else { Format::Binary })
            .map(|numeric| *numeric.deref())
    }

    /// Get text value at index.
    pub fn get_text(&self, index: usize) -> Option<String> {
        self.get::<String>(index, Format::Text)
    }

    /// Get column at index given format encoding.
    pub fn get<T: FromDataType>(&self, index: usize, format: Format) -> Option<T> {
        self.column(index)
            .and_then(|col| T::decode(&col, format).ok())
    }

    /// Get column at index given row description.
    pub fn get_column<'a>(
        &self,
        index: usize,
        rd: &'a RowDescription,
    ) -> Result<Option<Column<'a>>, Error> {
        if let Some(field) = rd.field(index) {
            if let Some(data) = self.column(index) {
                return Ok(Some(Column {
                    name: field.name.as_str(),
                    value: Datum::new(&data, field.data_type(), field.format())?,
                }));
            }
        }

        Ok(None)
    }

    /// Render the data row.
    pub fn into_row<'a>(&self, rd: &'a RowDescription) -> Result<Vec<Column<'a>>, Error> {
        let mut row = vec![];

        for (index, field) in rd.fields.iter().enumerate() {
            if let Some(data) = self.column(index) {
                row.push(Column {
                    name: field.name.as_str(),
                    value: Datum::new(&data, field.data_type(), field.format())?,
                });
            }
        }

        Ok(row)
    }
}

/// Column with data type mapped to a Rust type.
#[derive(Debug, Clone)]
pub struct Column<'a> {
    /// Column name.
    pub name: &'a str,
    /// Column value.
    pub value: Datum,
}

impl FromBytes for DataRow {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'D');
        let _len = bytes.get_i32();
        let columns = (0..bytes.get_i16())
            .map(|_| {
                let len = bytes.get_i32() as isize; // NULL = -1
                let mut column = BytesMut::new();

                if len < 0 {
                    return column.freeze();
                }

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
