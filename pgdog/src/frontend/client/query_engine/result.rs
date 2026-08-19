use crate::{
    frontend::{ClientRequest, client::TransactionType},
    net::{ProtocolMessage, Query},
};

pub enum QueryEngineResult {
    Done(Option<TransactionType>),
    ReplaySplitSimple(Vec<ClientRequest>),
    ReplaySplitExtended(Vec<ClientRequest>),
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
