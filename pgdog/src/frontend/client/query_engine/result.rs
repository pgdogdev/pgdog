use crate::{
    frontend::{ClientRequest, client::TransactionType},
    net::{ProtocolMessage, Query},
};

pub enum QueryEngineResult {
    Done(Option<TransactionType>),
    ReplaySplitSimple(Vec<ClientRequest>),
    ReplaySplitExtended(Vec<ClientRequest>),
    /// One of the queries returned an error. If this was a single query, Postgres
    /// would abort the transaction.
    AbortSplitSimple(Option<TransactionType>),
}

impl QueryEngineResult {
    pub(super) fn replay(queries: &[Query]) -> Self {
        let reqs = queries
            .iter()
            .map(|q| ClientRequest::from(vec![ProtocolMessage::Query(q.clone())]))
            .collect::<Vec<_>>();

        Self::ReplaySplitSimple(reqs)
    }
}
