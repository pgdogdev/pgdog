use crate::frontend::ClientRequest;

#[derive(Debug, Clone)]
pub enum QueryEngineOutput {
    // The request has been executed as-is.
    Executed,
    // The request has been rewritten and needs to
    // be resent.
    Rewritten(Vec<ClientRequest>),
}
