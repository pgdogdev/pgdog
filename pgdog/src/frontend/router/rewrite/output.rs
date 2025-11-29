use crate::net::{Bind, Parse, ProtocolMessage, Query};

#[derive(Debug, Clone)]
pub struct RewrittenRequest {
    pub messages: Vec<ProtocolMessage>,
    pub action: ExecutionAction,
}

/// Output of a single rewrite step.
#[derive(Debug, Clone)]
pub enum StepOutput {
    NoOp,
    Extended { parse: Parse, bind: Bind },
    Simple { query: Query },
}

impl StepOutput {
    /// Get rewritten query, if any.
    pub fn query(&self) -> Result<&str, ()> {
        match self {
            Self::Extended { parse, .. } => Ok(parse.query()),
            Self::Simple { query } => Ok(query.query()),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Output {
    Passthrough,
    Simple(RewrittenRequest),
    Chain(Vec<RewrittenRequest>),
}

#[derive(Debug, Clone)]
pub enum ExecutionAction {
    /// Drop result completely.
    Drop,
    /// Return result to client.
    Return,
    /// Forward result to next step in the chain.
    Forward,
}
