use crate::net::DataRow;

impl From<DataRow> for i64 {
    fn from(value: DataRow) -> Self {
        value.get_int(0, true).unwrap_or(0) as i64
    }
}
