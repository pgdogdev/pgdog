//! DataRow (B) message.

use crate::net::Decoder;

use super::{
    code, prelude::*, Datum, Double, Float, Format, FromDataType, Numeric, RowDescription,
};
pub use pgdog_postgres_types::{Data, ToDataRowColumn};

/// DataRow message.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct DataRow {
    columns: Vec<Data>,
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
    pub fn insert(
        &mut self,
        index: usize,
        value: impl ToDataRowColumn,
        is_null: bool,
    ) -> &mut Self {
        while self.columns.len() <= index {
            self.columns.push(Data::null());
        }
        let mut data = value.to_data_row_column();
        data.is_null = is_null;
        self.columns[index] = data;
        self
    }

    /// Drop columns by 0-based index, ignoring indexes out of bounds.
    pub fn drop_columns(&mut self, drop: &[usize]) {
        if drop.is_empty() {
            return;
        }

        let mut indices = drop.to_vec();
        indices.sort_unstable();
        indices.dedup();

        if indices.is_empty() {
            return;
        }

        let mut dropped = indices.into_iter().peekable();
        let mut retained = Vec::with_capacity(self.columns.len());

        for (idx, column) in self.columns.drain(..).enumerate() {
            match dropped.peek() {
                Some(&drop_idx) if drop_idx == idx => {
                    dropped.next();
                }
                _ => retained.push(column),
            }
        }

        // Any remaining indexes are beyond the current column count; ignore.
        self.columns = retained;
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
        self.get::<Float>(index, if text { Format::Text } else { Format::Binary })
            .map(|float| float.0 as f64)
    }

    // Get numeric at index with text/binary encoding.
    pub fn get_numeric(&self, index: usize, text: bool) -> Option<Numeric> {
        self.get::<Numeric>(index, if text { Format::Text } else { Format::Binary })
    }

    // Get double at index with text/binary encoding.
    pub fn get_double(&self, index: usize, text: bool) -> Option<f64> {
        self.get::<Double>(index, if text { Format::Text } else { Format::Binary })
            .map(|double| double.0)
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

    /// Get raw column data.
    pub(crate) fn get_raw(&self, index: usize) -> Option<&Data> {
        self.columns.get(index)
    }

    /// Get column at index given row description.
    pub fn get_column<'a>(
        &self,
        index: usize,
        decoder: &'a Decoder,
    ) -> Result<Option<Column<'a>>, Error> {
        if let Some(field) = decoder.rd().field(index) {
            if let Some(data) = self.columns.get(index) {
                return Ok(Some(Column {
                    name: field.name.as_str(),
                    value: Datum::new(
                        &data.data,
                        field.data_type(),
                        decoder.format(index),
                        data.is_null,
                    )?,
                }));
            }
        }

        Ok(None)
    }

    /// Render the data row.
    pub fn into_row<'a>(&self, rd: &'a RowDescription) -> Result<Vec<Column<'a>>, Error> {
        let mut row = vec![];

        for (index, field) in rd.fields.iter().enumerate() {
            if let Some(data) = self.columns.get(index) {
                row.push(Column {
                    name: field.name.as_str(),
                    value: Datum::new(&data.data, field.data_type(), field.format(), data.is_null)?,
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

                if len < 0 {
                    return (Bytes::new(), true);
                }

                let column = bytes.split_to(len as usize);
                (column, false)
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
        dr.insert(4, "test", false);
        assert_eq!(dr.columns.len(), 5);
        assert_eq!(dr.get::<String>(4, Format::Text).unwrap(), "test");
        assert_eq!(dr.get::<String>(0, Format::Text).unwrap(), "");
    }

    #[test]
    fn test_data_row_serde() {
        let mut dr = DataRow::new();
        dr.add("hello");
        dr.add(42i64);
        dr.add(true);
        dr.add(Data::null());
        dr.add("world");

        let serialized = dr.to_bytes().unwrap();
        let deserialized = DataRow::from_bytes(serialized).unwrap();

        assert_eq!(deserialized.len(), 5);
        assert_eq!(
            deserialized.get::<String>(0, Format::Text).unwrap(),
            "hello"
        );
        assert_eq!(deserialized.get::<String>(1, Format::Text).unwrap(), "42");
        assert_eq!(deserialized.get::<String>(2, Format::Text).unwrap(), "t");
        assert_eq!(deserialized.column(3), Some(Bytes::new()));
        assert_eq!(
            deserialized.get::<String>(4, Format::Text).unwrap(),
            "world"
        );

        assert_eq!(dr, deserialized);
    }

    #[test]
    fn test_drop_columns() {
        let mut dr = DataRow::new();
        dr.add("a");
        dr.add("b");
        dr.add("c");

        dr.drop_columns(&[1, 5]);

        assert_eq!(dr.len(), 2);
        assert_eq!(dr.get::<String>(0, Format::Text).unwrap(), "a");
        assert_eq!(dr.get::<String>(1, Format::Text).unwrap(), "c");
    }
}
