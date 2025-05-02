use once_cell::sync::Lazy;
use regex::Regex;

use crate::{
    backend::{ProtocolMessage, Server},
    frontend::{buffer::BufferedQuery, Buffer},
    net::{Bind, DataRow, Execute, FromBytes, Parse, Protocol, Query, Sync, ToBytes},
};

use super::{plan::QueryPlan, Error};

static EXPLAIN_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r#"(?i)^\s*EXPLAIN\b"#).unwrap());

#[derive(Debug, Clone)]
pub enum PlanRequest {
    Query(Query),
    Prepared { parse: Parse, bind: Bind },
}

impl PlanRequest {
    pub(crate) fn from_buffer(buffer: &Buffer) -> Result<PlanRequest, Error> {
        let query = buffer.query()?;
        let bind = buffer.parameters()?;

        match query {
            Some(BufferedQuery::Query(query)) => Ok(PlanRequest::Query(query)),
            Some(BufferedQuery::Prepared(parse)) => {
                if let Some(bind) = bind {
                    Ok(PlanRequest::Prepared {
                        parse,
                        bind: bind.clone(),
                    })
                } else {
                    Err(Error::NothingToPlan)
                }
            }
            _ => Err(Error::NothingToPlan),
        }
    }

    pub(crate) fn skip(&self) -> bool {
        match self {
            Self::Query(query) => EXPLAIN_RE.find(query.query()).is_some(),
            Self::Prepared { parse, .. } => EXPLAIN_RE.find(parse.query()).is_some(),
        }
    }

    pub(crate) async fn load(&self, server: &mut Server) -> Result<QueryPlan, Error> {
        if !server.in_sync() {
            return Err(Error::NotInSync);
        }

        let prefix = "EXPLAIN (FORMAT JSON)";
        let query = match self {
            Self::Query(query) => format!("{} {}", prefix, query.query()),
            Self::Prepared { parse, .. } => format!("{} {}", prefix, parse.query()),
        };

        let reply = match self {
            Self::Query(_) => server.execute_checked(&query).await?,
            Self::Prepared { bind, .. } => {
                server
                    .send(vec![
                        ProtocolMessage::from(Parse::new_anonymous(&query)),
                        bind.clone().rename("").into(),
                        Execute::new().into(),
                        Sync.into(),
                    ])
                    .await?;
                let mut messages = vec![];
                while !server.done() {
                    messages.push(server.read().await?);
                }

                messages
            }
        };

        for message in reply {
            if message.code() == 'D' {
                let data_row = DataRow::from_bytes(message.to_bytes()?)?;
                let plan = data_row.get_text(0).map(|s| QueryPlan::from_json(&s));

                if let Some(plan) = plan {
                    return plan;
                }
            }
        }

        Err(Error::NotInSync)
    }
}

#[cfg(test)]
mod test {
    use crate::{
        backend::pool::{test::pool, Request},
        net::bind::Parameter,
    };

    use super::*;

    #[tokio::test]
    async fn test_plan() {
        let pool = pool();
        let mut conn = pool.get(&Request::default()).await.unwrap();

        for _ in 0..10 {
            // Simple
            let req = PlanRequest::Query(Query::new("SELECT 1"));
            let _plan = req.load(&mut conn).await.unwrap();

            // Prepared
            let req = PlanRequest::Prepared {
                parse: Parse::named("__pgdog_1", "SELECT $1"),
                bind: Bind::test_params(
                    "__pgdog_1",
                    &[Parameter {
                        len: 1,
                        data: "1".as_bytes().to_vec(),
                    }],
                ),
            };
            let _plan = req.load(&mut conn).await.unwrap();
        }
    }
}
