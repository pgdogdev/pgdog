//! Schema operations.
pub mod columns;
pub mod relation;
pub mod trigger_function;

use std::collections::HashMap;

pub use relation::{Relation, TABLES};
pub use trigger_function::source;

use crate::net::messages::{DataRow, FromBytes, Protocol, ToBytes};

use super::{Error, Server};

/// Load schema from database.
#[derive(Debug, Clone, Default)]
pub struct Schema {
    tables: HashMap<(String, String), Relation>,
}

impl Schema {
    /// Load schema from a server connection.
    pub async fn load(server: &mut Server) -> Result<Self, Error> {
        let result = server.execute(TABLES).await?;
        let mut tables = HashMap::new();

        for message in result {
            if message.code() == 'D' {
                let row = DataRow::from_bytes(message.to_bytes()?)?;
                let table = Relation::from(row);
                tables.insert((table.schema().to_string(), table.name.clone()), table);
            }
        }

        Ok(Self { tables })
    }

    /// Get table by name.
    pub fn table(&self, name: &str, schema: Option<&str>) -> Option<&Relation> {
        let schema = schema.unwrap_or("public");
        self.tables.get(&(name.to_string(), schema.to_string()))
    }

    /// Get all indices.
    pub fn indices(&self) -> Vec<Relation> {
        self.tables
            .values()
            .filter(|value| value.is_index())
            .map(|value| value.clone())
            .collect()
    }
}

#[cfg(test)]
mod test {
    use crate::backend::pool::Request;

    use super::super::pool::test::pool;
    use super::Schema;

    #[tokio::test]
    async fn test_schema() {
        let pool = pool();
        let mut conn = pool.get(&Request::default()).await.unwrap();
        let schema = Schema::load(&mut conn).await.unwrap();
        println!("{:#?}", schema.indices());
    }
}
