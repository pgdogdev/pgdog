use crate::{
    frontend::{ClientRequest, client::TransactionType},
    net::Query,
};

pub enum QueryEngineResult {
    Done(Option<TransactionType>),
    ReplaySplit {
        requests: Vec<ClientRequest>,
        extended: bool,
    },
}

impl QueryEngineResult {
    /// Rewrite simple queries into extended ones to make sure
    /// they are executed as part of one transaction.
    pub(super) fn new_replay(queries: &[String]) -> Self {
        let requests = queries
            .iter()
            .map(|q| ClientRequest::from(vec![Query::new(q).into()]))
            .collect::<Vec<_>>();

        Self::ReplaySplit {
            requests,
            extended: false,
        }
    }
}
