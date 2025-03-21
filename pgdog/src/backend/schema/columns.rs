//! Get all table definitions.
use super::Error;
use crate::{backend::Server, net::messages::DataRow};
use std::collections::HashMap;

static COLUMNS: &str = include_str!("columns.sql");

#[derive(Debug, Clone)]
pub struct Column {
    pub table_catalog: String,
    pub table_schema: String,
    pub table_name: String,
    pub column_name: String,
    pub column_default: String,
    pub is_nullable: bool,
    pub data_type: String,
}

impl Column {
    /// Load all columns from server.
    pub async fn load(
        server: &mut Server,
    ) -> Result<HashMap<(String, String), Vec<Column>>, Error> {
        let mut result = HashMap::new();
        let rows: Vec<Self> = server.fetch_all(COLUMNS).await?;

        for row in rows {
            let entry = result
                .entry((row.table_schema.clone(), row.table_name.clone()))
                .or_insert_with(Vec::new);
            entry.push(row);
        }

        Ok(result)
    }
}

impl From<DataRow> for Column {
    fn from(value: DataRow) -> Self {
        Self {
            table_catalog: value.get_text(0).unwrap_or_default(),
            table_schema: value.get_text(1).unwrap_or_default(),
            table_name: value.get_text(2).unwrap_or_default(),
            column_name: value.get_text(3).unwrap_or_default(),
            column_default: value.get_text(4).unwrap_or_default(),
            is_nullable: value.get_text(5).unwrap_or_default() == "true",
            data_type: value.get_text(6).unwrap_or_default(),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::backend::pool::test::pool;
    use crate::backend::pool::Request;
    use crate::backend::schema::columns::Column;

    #[tokio::test]
    async fn test_load_columns() {
        let pool = pool();
        let mut conn = pool.get(&Request::default()).await.unwrap();
        let columns = Column::load(&mut conn).await.unwrap();
        println!("{:#?}", columns);
    }
}
