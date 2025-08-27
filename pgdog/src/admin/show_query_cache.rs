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
        let mut queries: Vec<_> = Cache::queries().into_iter().collect();
        let mut messages = vec![RowDescription::new(&[
            Field::text("query"),
            Field::numeric("hits"),
            Field::numeric("direct"),
            Field::numeric("multi"),
        ])
        .message()?];

        queries.sort_by_cached_key(|v| v.1.stats.lock().hits);

        for query in queries.into_iter().rev() {
            if !self.filter.is_empty() && !query.0.to_lowercase().contains(&self.filter) {
                continue;
            }
            let mut data_row = DataRow::new();
            let stats = *query.1.stats.lock();
            data_row
                .add(query.0)
                .add(stats.hits)
                .add(stats.direct)
                .add(stats.multi);
            messages.push(data_row.message()?);
        }

        Ok(messages)
    }
}

#[cfg(test)]
mod test {
    use crate::net::{FromBytes, ToBytes};

    use super::*;

    #[tokio::test]
    async fn test_show_query_cache() {
        let cache = Cache::get();

        for q in 0..5 {
            cache
                .parse(format!("SELECT $1::bigint, {}", q).as_str())
                .unwrap();
        }

        let show = ShowQueryCache {
            filter: String::new(),
        }
        .execute()
        .await
        .unwrap();

        let mut total = 0;
        for message in show {
            if message.code() == 'D' {
                total += 1;
                let data_row = DataRow::from_bytes(message.to_bytes().unwrap()).unwrap();
                let hits = data_row.get_int(1, true).unwrap();
                assert_eq!(hits, 1);
            }
        }

        assert_eq!(total, 5);
    }
}
