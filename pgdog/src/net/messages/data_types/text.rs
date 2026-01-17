use crate::net::DataRow;

impl From<DataRow> for String {
    fn from(value: DataRow) -> Self {
        value.get_text(0).unwrap_or_default()
    }
}
