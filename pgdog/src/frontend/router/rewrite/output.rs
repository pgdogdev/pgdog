use crate::{frontend::ClientRequest, net::ProtocolMessage};

use std::mem::discriminant;

#[derive(Debug, Clone)]
pub struct RewrittenRequest {
    pub messages: Vec<ProtocolMessage>,
    pub action: ExecutionAction,
    pub renamed: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RewriteAction {
    pub(super) message: ProtocolMessage,
    pub(super) action: RewriteActionKind,
}

impl RewriteAction {
    /// Execute rewrite action.
    pub fn execute(&self, request: &mut ClientRequest) {
        match self.action {
            RewriteActionKind::Append => request.push(self.message.clone()),
            RewriteActionKind::Replace => {
                if let Some(pos) = request
                    .iter()
                    .position(|p| discriminant(p) == discriminant(&self.message))
                {
                    request[pos] = self.message.clone();
                }
            }
            RewriteActionKind::Prepend => request.insert(0, self.message.clone()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum RewriteActionKind {
    Replace,
    Prepend,
    #[allow(dead_code)]
    Append,
}

/// Output of a single rewrite step.
#[derive(Debug, Clone)]
pub enum StepOutput {
    NoOp,
    Rewrite(Vec<RewriteAction>),
}

impl StepOutput {
    /// Get rewritten query, if any.
    pub fn query(&self) -> Result<&str, ()> {
        match self {
            Self::NoOp => Err(()),
            Self::Rewrite(actions) => {
                for action in actions {
                    if let Some(query) = action.message.query() {
                        return Ok(query);
                    }
                }

                Err(())
            }
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
