//! Handles client connections.

use std::collections::VecDeque;
use std::time::Duration;

use tokio::time::sleep;
use tracing::debug;

use crate::backend::ProtocolMessage;
use crate::frontend::Buffer;
use crate::net::messages::command_complete::CommandComplete;
use crate::net::messages::{ErrorResponse, FromBytes, Protocol, Query, ReadyForQuery};
use crate::net::ToBytes;

use super::parser::Parser;
use super::prelude::Message;
use super::Error;

/// Admin backend.
#[derive(Debug)]
pub struct Backend {
    messages: VecDeque<Message>,
}

impl Default for Backend {
    fn default() -> Self {
        Self::new()
    }
}

impl Backend {
    /// New admin backend handler.
    pub fn new() -> Self {
        Self {
            messages: VecDeque::new(),
        }
    }

    /// Handle command.
    pub async fn send(&mut self, messages: &Buffer) -> Result<(), Error> {
        let message = messages.first().ok_or(Error::Empty)?;
        let message: ProtocolMessage = message.clone();

        if message.code() != 'Q' {
            debug!("admin received unsupported message: {:?}", message);
            return Err(Error::SimpleOnly);
        }

        let query = Query::from_bytes(message.to_bytes()?)?;

        let messages = match Parser::parse(&query.query().to_lowercase()) {
            Ok(command) => {
                let mut messages = command.execute().await?;
                messages.push(CommandComplete::new(command.name()).message()?);

                messages
            }
            Err(err) => vec![ErrorResponse::syntax(err.to_string().as_str()).message()?],
        };

        self.messages.extend(messages);
        self.messages.push_back(ReadyForQuery::idle().message()?);

        Ok(())
    }

    /// Receive command result.
    pub async fn read(&mut self) -> Result<Message, Error> {
        if let Some(message) = self.messages.pop_front() {
            Ok(message)
        } else {
            loop {
                sleep(Duration::MAX).await;
            }
        }
    }

    pub fn done(&self) -> bool {
        self.messages.is_empty()
    }
}
