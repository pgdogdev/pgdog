use std::collections::HashMap;

use super::{columns::Column, Error};
use crate::{
    backend::Server,
    net::messages::{DataRow, Format},
};

/// Get all relations in the database.
pub static TABLES: &str = include_str!("relations.sql");

#[derive(Debug, Clone)]
pub struct Relation {
    schema: String,
    pub name: String,
    pub type_: String,
    pub owner: String,
    pub persistence: String,
    pub access_method: String,
    pub size: usize,
    pub description: String,
    pub oid: i32,
    pub columns: HashMap<String, Column>,
}

impl From<DataRow> for Relation {
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
            oid: value.get::<i32>(8, Format::Text).unwrap_or_default(),
            columns: HashMap::new(),
        }
    }
}

impl Relation {
    /// Load relations and their columns.
    pub async fn load(server: &mut Server) -> Result<Vec<Relation>, Error> {
        let mut relations: HashMap<_, _> = server
            .fetch_all::<Relation>(TABLES)
            .await?
            .into_iter()
            .map(|relation| {
                (
                    (relation.schema().to_owned(), relation.name.clone()),
                    relation,
                )
            })
            .collect();
        let columns = Column::load(server).await?;
        for column in columns {
            if let Some(relation) = relations.get_mut(&column.0) {
                relation.columns = column
                    .1
                    .into_iter()
                    .map(|c| (c.column_name.clone(), c))
                    .collect();
            }
        }

        Ok(relations.into_values().collect())
    }

    /// Get schema where the table is located.
    pub fn schema(&self) -> &str {
        if self.schema.is_empty() {
            "public"
        } else {
            &self.schema
        }
    }

    /// This is an index.
    pub fn is_index(&self) -> bool {
        matches!(self.type_.as_str(), "index" | "partitioned index")
    }

    /// This is a table.
    pub fn is_table(&self) -> bool {
        matches!(self.type_.as_str(), "table" | "partitioned table")
    }

    /// This is a sequence.
    pub fn is_sequence(&self) -> bool {
        self.type_ == "sequence"
    }

    /// Columns by name.
    pub fn columns(&self) -> &HashMap<String, Column> {
        &self.columns
    }
}

#[cfg(test)]
mod test {
    use crate::backend::pool::{test::pool, Request};

    use super::*;

    #[tokio::test]
    async fn test_load_relations() {
        let pool = pool();
        let mut conn = pool.get(&Request::default()).await.unwrap();
        let relations = Relation::load(&mut conn).await.unwrap();
        println!("{:#?}", relations);
    }
}
