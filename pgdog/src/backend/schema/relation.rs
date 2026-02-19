use pgdog_stats::Relation as StatsRelation;
use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
};

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use super::{columns::Column, Error};
use crate::{
    backend::Server,
    net::messages::{DataRow, Format},
};

/// Get all relations in the database.
pub static TABLES: &str = include_str!("relations.sql");

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Relation {
    inner: StatsRelation,
}

impl Deref for Relation {
    type Target = StatsRelation;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for Relation {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl From<StatsRelation> for Relation {
    fn from(value: StatsRelation) -> Self {
        Self { inner: value }
    }
}

impl From<Relation> for StatsRelation {
    fn from(value: Relation) -> Self {
        value.inner
    }
}

impl From<DataRow> for Relation {
    fn from(value: DataRow) -> Self {
        Self {
            inner: StatsRelation {
                schema: value.get_text(0).unwrap_or_default(),
                name: value.get_text(1).unwrap_or_default(),
                type_: value.get_text(2).unwrap_or_default(),
                owner: value.get_text(3).unwrap_or_default(),
                persistence: value.get_text(4).unwrap_or_default(),
                access_method: value.get_text(5).unwrap_or_default(),
                description: value.get_text(6).unwrap_or_default(),
                oid: value.get::<i32>(7, Format::Text).unwrap_or_default(),
                columns: IndexMap::new(),
                is_sharded: false,
            },
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
                    .map(|c| (c.column_name.clone(), c.into()))
                    .collect();
            }
        }

        Ok(relations.into_values().collect())
    }
}

#[cfg(test)]
impl Relation {
    pub(crate) fn test_table(schema: &str, name: &str, columns: IndexMap<String, Column>) -> Self {
        StatsRelation {
            schema: schema.into(),
            name: name.into(),
            type_: "table".into(),
            owner: String::new(),
            persistence: String::new(),
            access_method: String::new(),
            description: String::new(),
            oid: 0,
            columns: columns.into_iter().map(|(k, v)| (k, v.into())).collect(),
            is_sharded: false,
        }
        .into()
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
