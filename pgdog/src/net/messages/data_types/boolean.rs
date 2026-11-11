use crate::net::{DataRow, Format};

impl From<DataRow> for bool {
    fn from(value: DataRow) -> Self {
        value.get::<bool>(0, Format::Text).unwrap_or_default()
    }
}
