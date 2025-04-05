//! DataRow (B) message.

use crate::net::Decoder;

use super::{code, prelude::*, Datum, Format, FromDataType, Numeric, RowDescription};
use bytes::BytesMut;
use std::ops::{Deref, DerefMut};

#[derive(Debug, Clone)]
pub struct Data {
    data: Bytes,
    is_null: bool,
}

impl Deref for Data {
    type Target = Bytes;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl DerefMut for Data {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl From<Bytes> for Data {
    fn from(value: Bytes) -> Self {
        Self {
            data: value,
            is_null: false,
        }
    }
}

impl From<(Bytes, bool)> for Data {
    fn from(value: (Bytes, bool)) -> Self {
        Self {
            data: value.0,
            is_null: value.1,
        }
    }
}

impl Data {
    pub fn null() -> Self {
        Self {
            data: Bytes::new(),
            is_null: true,
        }
    }
}

/// DataRow message.
#[derive(Debug, Clone)]
pub struct DataRow {
    columns: Vec<Data>,
}

/// Convert value to data row column
/// using text formatting.
pub trait ToDataRowColumn {
    fn to_data_row_column(&self) -> Data;
}

impl ToDataRowColumn for Bytes {
    fn to_data_row_column(&self) -> Data {
        self.clone().into()
    }
}

impl ToDataRowColumn for String {
    fn to_data_row_column(&self) -> Data {
        Bytes::copy_from_slice(self.as_bytes()).into()
    }
}

impl ToDataRowColumn for &String {
    fn to_data_row_column(&self) -> Data {
        Bytes::copy_from_slice(self.as_bytes()).into()
    }
}

impl ToDataRowColumn for &str {
    fn to_data_row_column(&self) -> Data {
        Bytes::copy_from_slice(self.as_bytes()).into()
    }
}

impl ToDataRowColumn for i64 {
    fn to_data_row_column(&self) -> Data {
        Bytes::copy_from_slice(self.to_string().as_bytes()).into()
    }
}

impl ToDataRowColumn for usize {
    fn to_data_row_column(&self) -> Data {
        Bytes::copy_from_slice(self.to_string().as_bytes()).into()
    }
}

impl ToDataRowColumn for u64 {
    fn to_data_row_column(&self) -> Data {
        Bytes::copy_from_slice(self.to_string().as_bytes()).into()
    }
}

impl ToDataRowColumn for bool {
    fn to_data_row_column(&self) -> Data {
        Bytes::copy_from_slice(if *self { b"t" } else { b"f" }).into()
    }
}

impl ToDataRowColumn for f64 {
    fn to_data_row_column(&self) -> Data {
        Bytes::copy_from_slice(self.to_string().as_bytes()).into()
    }
}

impl ToDataRowColumn for u128 {
    fn to_data_row_column(&self) -> Data {
        Bytes::copy_from_slice(self.to_string().as_bytes()).into()
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

    /// Insert column at index. If row is smaller than index,
    /// columns will be prefilled with NULLs.
    pub fn insert(&mut self, index: usize, value: impl ToDataRowColumn) -> &mut Self {
        while self.columns.len() <= index {
            self.columns.push(Data::null());
        }
        self.columns[index] = value.to_data_row_column();
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
        self.columns.get(index).cloned().map(|d| d.data)
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
        decoder: &'a Decoder,
    ) -> Result<Option<Column<'a>>, Error> {
        if let Some(field) = decoder.rd().field(index) {
            if let Some(data) = self.column(index) {
                return Ok(Some(Column {
                    name: field.name.as_str(),
                    value: Datum::new(&data, field.data_type(), decoder.format(index))?,
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

    /// How many columns in the data row.
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// No columns.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
                    return (column.freeze(), true);
                }

                for _ in 0..len {
                    column.put_u8(bytes.get_u8());
                }

                (column.freeze(), false)
            })
            .map(Data::from)
            .collect();

        Ok(Self { columns })
    }
}

impl ToBytes for DataRow {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let mut payload = Payload::named(self.code());
        payload.put_i16(self.columns.len() as i16);

        for column in &self.columns {
            if column.is_null {
                payload.put_i32(-1);
            } else {
                payload.put_i32(column.len() as i32);
                payload.put(&column[..]);
            }
        }

        Ok(payload.freeze())
    }
}

impl Protocol for DataRow {
    fn code(&self) -> char {
        'D'
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_insert() {
        let mut dr = DataRow::new();
        dr.insert(4, "test");
        assert_eq!(dr.columns.len(), 5);
        assert_eq!(dr.get::<String>(4, Format::Text).unwrap(), "test");
        assert_eq!(dr.get::<String>(0, Format::Text).unwrap(), "");
    }
}
