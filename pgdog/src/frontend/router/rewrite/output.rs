use crate::{
    frontend::ClientRequest,
    net::{Bind, Describe, Error, FromBytes, Parse, Protocol, ProtocolMessage, Query, ToBytes},
};

#[derive(Debug, Clone)]
pub struct RewrittenRequest {
    pub messages: Vec<ProtocolMessage>,
    pub action: ExecutionAction,
    pub renamed: Option<String>,
}

impl RewrittenRequest {
    /// Rewrite client request in-place,
    /// making sure all messages use new prepared statement names.
    pub fn rewrite_in_place(&self, request: &mut ClientRequest) -> Result<(), Error> {
        for message in &self.messages {
            let code = message.code();
            if let Some(pos) = request.messages.iter().position(|p| p.code() == code) {
                request.messages[pos] = message.clone();
            }
        }

        if let Some(ref renamed) = self.renamed {
            for message in request.messages.iter_mut() {
                // Rename describe to the new prepared statement.
                if message.code() == 'D' {
                    let mut describe = Describe::from_bytes(message.to_bytes()?)?;
                    if !describe.is_statement() {
                        describe.rename(renamed);
                    }

                    *message = ProtocolMessage::from(describe);
                }
            }
        }

        Ok(())
    }
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
