use crate::net::messages::DataRow;

/// Get all tables in the database.
pub static TABLES: &str = include_str!("tables.sql");

#[derive(Debug, Clone)]
pub struct Table {
    pub schema: String,
    pub name: String,
    pub type_: String,
    pub owner: String,
    pub persistence: String,
    pub access_method: String,
    pub size: usize,
    pub description: String,
}

impl From<DataRow> for Table {
    fn from(value: DataRow) -> Self {
        Self {
            schema: value.get_text(0).unwrap_or_default(),
            name: value.get_text(1).unwrap_or_default(),
            type_: value.get_text(2).unwrap_or_default(),
            owner: value.get_text(3).unwrap_or_default(),
            persistence: value.get_text(4).unwrap_or_default(),
            access_method: value.get_text(5).unwrap_or_default(),
            size: value.get_int(6, true).unwrap_or_default() as usize,
            description: value.get_text(7).unwrap_or_default(),
        }
    }
}
