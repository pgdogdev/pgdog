//! Handles client connections.

use std::collections::VecDeque;
use std::time::Duration;

use tokio::time::sleep;

use crate::net::messages::command_complete::CommandComplete;
use crate::net::messages::{FromBytes, Protocol, Query, ReadyForQuery};

use super::parser::Parser;
use super::prelude::Message;
use super::Error;

/// Admin backend.
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
    pub async fn send(&mut self, messages: Vec<impl Protocol>) -> Result<(), Error> {
        let message = messages.first().ok_or(Error::Empty)?;

        if message.code() != 'Q' {
            return Err(Error::SimpleOnly);
        }

        let query = Query::from_bytes(message.to_bytes()?)?;

        let command = Parser::parse(&query.query.to_lowercase())?;

        self.messages.extend(command.execute().await?);

        self.messages.push_back(
            CommandComplete {
                command: command.name(),
            }
            .message()?,
        );
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
