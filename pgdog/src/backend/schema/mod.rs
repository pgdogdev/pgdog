pub mod queries;
use std::collections::HashMap;

pub use queries::{Table, TABLES};

use crate::net::messages::{DataRow, FromBytes, Protocol, ToBytes};

use super::{Error, Server};

#[derive(Debug, Clone, Default)]
pub struct Schema {
    tables: HashMap<(String, String), Table>,
}

impl Schema {
    /// Load schema from a server connection.
    pub async fn load(server: &mut Server) -> Result<Self, Error> {
        let result = server.execute(TABLES).await?;
        let mut tables = HashMap::new();

        for message in result {
            if message.code() == 'D' {
                let row = DataRow::from_bytes(message.to_bytes()?)?;
                let table = Table::from(row);
                tables.insert((table.schema.clone(), table.name.clone()), table);
            }
        }

        Ok(Self { tables })
    }

    /// Get table by name.
    pub fn table(&self, name: &str, schema: Option<&str>) -> Option<&Table> {
        let schema = schema.unwrap_or("public");
        self.tables.get(&(name.to_string(), schema.to_string()))
    }
}

#[cfg(test)]
mod test {
    use crate::net::messages::BackendKeyData;

    use super::super::pool::test::pool;
    use super::Schema;

    #[tokio::test]
    async fn test_schema() {
        let pool = pool();
        let mut conn = pool.get(&BackendKeyData::new()).await.unwrap();
        let _schema = Schema::load(&mut conn).await.unwrap();
        // println!("{:#?}", schema);
    }
}
