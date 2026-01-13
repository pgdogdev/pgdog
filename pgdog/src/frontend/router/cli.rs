//! Invoke the router on query.

use std::path::Path;

use tokio::fs::read_to_string;

use super::Error;
use crate::{
    backend::databases::databases,
    frontend::{client::Sticky, router::QueryParser, Command, RouterContext},
    net::{Parameters, ProtocolMessage, Query},
};

#[derive(Debug, Clone)]
pub struct RouterCli {
    database: String,
    user: String,
    queries: Vec<String>,
}

impl RouterCli {
    pub async fn new(
        database: impl ToString,
        user: impl ToString,
        file: impl AsRef<Path>,
    ) -> Result<Self, std::io::Error> {
        let queries = read_to_string(file).await?;
        let queries = queries
            .split(";")
            .filter(|q| !q.trim().is_empty())
            .map(|s| s.to_string())
            .collect();

        Ok(Self {
            database: database.to_string(),
            user: user.to_string(),
            queries,
        })
    }

    pub fn run(&self) -> Result<Vec<Command>, Error> {
        let mut result = vec![];
        let cluster = databases().cluster((self.user.as_str(), self.database.as_str()))?;

        for query in &self.queries {
            let mut qp = QueryParser::default();
            let req = vec![ProtocolMessage::from(Query::new(query))];
            let cmd = qp.parse(RouterContext::new(
                &req.into(),
                &cluster,
                &Parameters::default(),
                None,
                Sticky::new(),
            )?)?;
            result.push(cmd);
        }

        Ok(result)
    }
}
