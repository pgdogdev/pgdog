//! Get all table definitions.
pub use pgdog_stats::Column as StatsColumn;
use serde::{Deserialize, Serialize};

use super::Error;
use crate::{backend::Server, net::messages::DataRow};
use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
};

static COLUMNS: &str = include_str!("columns.sql");

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Column {
    inner: StatsColumn,
}

impl From<StatsColumn> for Column {
    fn from(value: StatsColumn) -> Self {
        Self { inner: value }
    }
}

impl From<Column> for StatsColumn {
    fn from(value: Column) -> Self {
        value.inner
    }
}

impl Deref for Column {
    type Target = StatsColumn;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for Column {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
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
        use crate::net::messages::Format;
        Self {
            inner: StatsColumn {
                table_catalog: value.get_text(0).unwrap_or_default(),
                table_schema: value.get_text(1).unwrap_or_default(),
                table_name: value.get_text(2).unwrap_or_default(),
                column_name: value.get_text(3).unwrap_or_default(),
                column_default: value.get_text(4).unwrap_or_default(),
                is_nullable: value.get_text(5).unwrap_or_default() == "true",
                data_type: value.get_text(6).unwrap_or_default(),
                ordinal_position: value.get::<i32>(7, Format::Text).unwrap_or(0),
                is_primary_key: value.get_text(8).unwrap_or_default() == "true",
            },
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
