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
        let mut messages = vec![RowDescription::new(&[
            Field::text("query"),
            Field::numeric("hits"),
            Field::numeric("direct"),
            Field::numeric("multi"),
        ])
        .message()?];

        let mut queries: Vec<_> = queries.into_iter().collect();
        queries.sort_by_key(|v| v.1.hits);

        for query in queries.into_iter().rev() {
            if !self.filter.is_empty() && !query.0.to_lowercase().contains(&self.filter) {
                continue;
            }
            let mut data_row = DataRow::new();
            data_row
                .add(query.0)
                .add(query.1.hits)
                .add(query.1.direct)
                .add(query.1.multi);
            messages.push(data_row.message()?);
        }

        Ok(messages)
    }
}
