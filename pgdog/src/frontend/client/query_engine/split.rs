use itertools::Itertools;

use super::*;
use crate::{frontend::ClientRequest, net::Query};

/// Query engine pipeline state.
pub(crate) enum Pipeline {
    Extended { requests_left: usize },
    Simple { requests_left: usize },
    None,
}

impl Pipeline {
    /// Create new pipeline.
    pub(crate) fn new(requests_left: usize, extended: bool) -> Self {
        if extended {
            Self::Extended { requests_left }
        } else {
            Self::Simple { requests_left }
        }
    }

    /// How many requests left in the pipeline.
    pub(super) fn requests_left(&self) -> usize {
        match self {
            Self::Extended { requests_left } => *requests_left,
            Self::Simple { requests_left } => *requests_left,
            Self::None => 0,
        }
    }

    /// Is the pipeline finished executing?
    pub(super) fn is_done(&self) -> bool {
        self.requests_left() == 0
    }

    /// Is the pipeline consists of simple queries only?
    pub(super) fn is_simple(&self) -> bool {
        matches!(self, Self::Simple { .. })
    }
}

impl QueryEngine {
    /// Check if the request needs splitting and perform the split if necessary.
    ///
    /// Caller is expected to abort the request and return the result back to the caller
    /// for resubmission.
    pub(super) fn check_extended_pipeline_rewrite(
        request: &ClientRequest,
    ) -> Result<Option<QueryEngineResult>, Error> {
        if request.is_multi_exec() {
            Ok(Some(QueryEngineResult::Split {
                requests: request.split_extended()?,
                extended: true,
            }))
        } else {
            Ok(None)
        }
    }

    /// Discard messages until Sync when the backend is in an extended protocol error state.
    /// Return true if the entire request was discarded.
    pub(super) fn discard_until_sync(&self, client_request: &mut ClientRequest) -> bool {
        self.backend.out_of_sync() && client_request.discard_until_sync()
    }

    /// Return true if we should ignore this query because
    /// the simple query pipeline is in an error state, i.e., inside a failed
    /// transaction.
    pub(super) fn in_simple_pipeline_error(&self, context: &QueryEngineContext<'_>) -> bool {
        context.pipeline.is_simple() && context.in_error()
    }

    /// Build a multi-query split.
    pub(super) fn build_simple_split(queries: &[String]) -> QueryEngineResult {
        let requests = queries
            .iter()
            .map(|query| ClientRequest::from(vec![Query::new(query).into()]))
            .collect_vec();

        QueryEngineResult::Split {
            requests,
            extended: false,
        }
    }
}
