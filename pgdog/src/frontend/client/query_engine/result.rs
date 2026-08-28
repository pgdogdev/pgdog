use crate::frontend::{ClientRequest, client::TransactionType};

/// Query engine execution result.
pub(crate) enum QueryEngineResult {
    /// Query engine is done executing the request.
    Done(Option<TransactionType>),
    /// Query engine requests the request to be resubmitted
    /// as a series of separate requests.
    Split {
        requests: Vec<ClientRequest>,
        extended: bool,
    },
    // Query engine should re-try the request again.
    Retry,
}
