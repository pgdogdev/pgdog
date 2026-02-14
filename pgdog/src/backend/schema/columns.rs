//! Get all table definitions.
pub use pgdog_stats::Column as StatsColumn;
pub use pgdog_stats::ForeignKey;
pub use pgdog_stats::ForeignKeyAction;
use serde::{Deserialize, Serialize};

use super::Error;
use crate::{backend::Server, net::messages::DataRow};
use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
};

static COLUMNS: &str = include_str!("columns.sql");
static FOREIGN_KEYS: &str = include_str!("foreign_keys.sql");

/// Represents a row from the foreign_keys.sql query.
struct ForeignKeyRow {
    source_schema: String,
    source_table: String,
    source_column: String,
    ref_schema: String,
    ref_table: String,
    ref_column: String,
    on_delete: ForeignKeyAction,
    on_update: ForeignKeyAction,
}

impl From<DataRow> for ForeignKeyRow {
    fn from(value: DataRow) -> Self {
        Self {
            source_schema: value.get_text(0).unwrap_or_default(),
            source_table: value.get_text(1).unwrap_or_default(),
            source_column: value.get_text(2).unwrap_or_default(),
            ref_schema: value.get_text(3).unwrap_or_default(),
            ref_table: value.get_text(4).unwrap_or_default(),
            ref_column: value.get_text(5).unwrap_or_default(),
            on_delete: ForeignKeyAction::from_pg_string(&value.get_text(6).unwrap_or_default()),
            on_update: ForeignKeyAction::from_pg_string(&value.get_text(7).unwrap_or_default()),
        }
    }
}

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
    /// Load all columns from server, including foreign key information.
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

        // Load foreign key constraints and merge into columns
        let fk_rows: Vec<ForeignKeyRow> = server.fetch_all(FOREIGN_KEYS).await?;
        for fk_row in fk_rows {
            let key = (fk_row.source_schema.clone(), fk_row.source_table.clone());
            if let Some(columns) = result.get_mut(&key) {
                if let Some(column) = columns
                    .iter_mut()
                    .find(|c| c.column_name == fk_row.source_column)
                {
                    column.foreign_keys.push(ForeignKey {
                        schema: fk_row.ref_schema,
                        table: fk_row.ref_table,
                        column: fk_row.ref_column,
                        on_delete: fk_row.on_delete,
                        on_update: fk_row.on_update,
                    });
                }
            }
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
                foreign_keys: Vec::new(),
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

    #[tokio::test]
    async fn test_load_foreign_keys() {
        use crate::backend::schema::columns::ForeignKeyAction;

        let pool = pool();
        let mut conn = pool.get(&Request::default()).await.unwrap();

        // Create test tables with FK relationships using different action types
        conn.execute("DROP TABLE IF EXISTS fk_test_orders CASCADE")
            .await
            .unwrap();
        conn.execute("DROP TABLE IF EXISTS fk_test_customers CASCADE")
            .await
            .unwrap();

        conn.execute(
            "CREATE TABLE fk_test_customers (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL
            )",
        )
        .await
        .unwrap();

        conn.execute(
            "CREATE TABLE fk_test_orders (
                id SERIAL PRIMARY KEY,
                customer_id INTEGER NOT NULL REFERENCES fk_test_customers(id)
                    ON DELETE CASCADE ON UPDATE RESTRICT,
                amount NUMERIC
            )",
        )
        .await
        .unwrap();

        let columns = Column::load(&mut conn).await.unwrap();

        // Find the customer_id column in fk_test_orders (uses "pgdog" schema in test db)
        let orders_columns = columns
            .get(&("pgdog".to_string(), "fk_test_orders".to_string()))
            .expect("fk_test_orders table should exist");

        let customer_id_col = orders_columns
            .iter()
            .find(|c| c.column_name == "customer_id")
            .expect("customer_id column should exist");

        // Verify FK info is captured
        assert!(
            !customer_id_col.foreign_keys.is_empty(),
            "customer_id should have foreign key info"
        );

        let fk = &customer_id_col.foreign_keys[0];
        assert_eq!(fk.schema, "pgdog");
        assert_eq!(fk.table, "fk_test_customers");
        assert_eq!(fk.column, "id");
        assert_eq!(fk.on_delete, ForeignKeyAction::Cascade);
        assert_eq!(fk.on_update, ForeignKeyAction::Restrict);

        // Verify the id column (PK) does NOT have FK info
        let id_col = orders_columns
            .iter()
            .find(|c| c.column_name == "id")
            .expect("id column should exist");
        assert!(id_col.is_primary_key);
        assert!(id_col.foreign_keys.is_empty());

        // Clean up
        conn.execute("DROP TABLE IF EXISTS fk_test_orders CASCADE")
            .await
            .unwrap();
        conn.execute("DROP TABLE IF EXISTS fk_test_customers CASCADE")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_load_composite_foreign_keys() {
        let pool = pool();
        let mut conn = pool.get(&Request::default()).await.unwrap();

        // Create test tables with composite FK
        conn.execute("DROP TABLE IF EXISTS fk_comp_child CASCADE")
            .await
            .unwrap();
        conn.execute("DROP TABLE IF EXISTS fk_comp_parent CASCADE")
            .await
            .unwrap();

        conn.execute(
            "CREATE TABLE fk_comp_parent (
                tenant_id INTEGER NOT NULL,
                id INTEGER NOT NULL,
                name TEXT,
                PRIMARY KEY (tenant_id, id)
            )",
        )
        .await
        .unwrap();

        conn.execute(
            "CREATE TABLE fk_comp_child (
                id SERIAL PRIMARY KEY,
                parent_tenant INTEGER NOT NULL,
                parent_id INTEGER NOT NULL,
                FOREIGN KEY (parent_tenant, parent_id) REFERENCES fk_comp_parent(tenant_id, id)
            )",
        )
        .await
        .unwrap();

        let columns = Column::load(&mut conn).await.unwrap();

        let child_columns = columns
            .get(&("pgdog".to_string(), "fk_comp_child".to_string()))
            .expect("fk_comp_child table should exist");

        // Check parent_tenant column FK
        let parent_tenant_col = child_columns
            .iter()
            .find(|c| c.column_name == "parent_tenant")
            .expect("parent_tenant column should exist");
        assert_eq!(parent_tenant_col.foreign_keys.len(), 1);
        assert_eq!(parent_tenant_col.foreign_keys[0].table, "fk_comp_parent");
        assert_eq!(parent_tenant_col.foreign_keys[0].column, "tenant_id");

        // Check parent_id column FK
        let parent_id_col = child_columns
            .iter()
            .find(|c| c.column_name == "parent_id")
            .expect("parent_id column should exist");
        assert_eq!(parent_id_col.foreign_keys.len(), 1);
        assert_eq!(parent_id_col.foreign_keys[0].table, "fk_comp_parent");
        assert_eq!(parent_id_col.foreign_keys[0].column, "id");

        // Clean up
        conn.execute("DROP TABLE IF EXISTS fk_comp_child CASCADE")
            .await
            .unwrap();
        conn.execute("DROP TABLE IF EXISTS fk_comp_parent CASCADE")
            .await
            .unwrap();
    }
}
