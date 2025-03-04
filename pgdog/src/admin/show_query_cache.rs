//! SHOW QUERY CACHE;

use crate::frontend::router::parser::Cache;

use super::prelude::*;

pub struct ShowQueryCache {
    filter: String,
}

#[async_trait]
impl Command for ShowQueryCache {
    fn name(&self) -> String {
        "SHOW QUERY CACHE".into()
    }

    fn parse(sql: &str) -> Result<Self, Error> {
        Ok(Self {
            filter: sql
                .split(" ")
                .skip(2)
                .filter(|s| !s.is_empty())
                .map(|s| s.to_lowercase())
                .collect::<Vec<String>>()
                .join(" "),
        })
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let queries = Cache::queries();
        let mut messages =
            vec![RowDescription::new(&[Field::text("query"), Field::numeric("hits")]).message()?];

        for query in queries {
            if !self.filter.is_empty() && !query.0.to_lowercase().contains(&self.filter) {
                continue;
            }
            let mut data_row = DataRow::new();
            data_row.add(query.0).add(query.1.hits);
            messages.push(data_row.message()?);
        }

        Ok(messages)
    }
}
