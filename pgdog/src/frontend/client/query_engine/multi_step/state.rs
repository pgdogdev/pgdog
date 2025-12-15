use fnv::FnvHashMap as HashMap;

use super::Error;
use crate::net::{CommandComplete, FromBytes, Message, Protocol, ReadyForQuery, ToBytes};

#[derive(Debug, Clone)]
pub enum CommandType {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone)]
pub struct MultiServerState {
    servers: usize,
    rows: usize,
    counters: HashMap<char, usize>,
}

impl MultiServerState {
    /// New multi-server execution state.
    pub fn new(servers: usize) -> Self {
        Self {
            servers,
            rows: 0,
            counters: HashMap::default(),
        }
    }

    /// Should the message be forwarded to the client.
    pub fn forward(&mut self, message: &Message) -> Result<bool, Error> {
        let code = message.code();
        let count = self.counters.entry(code).or_default();
        *count += 1;

        Ok(match code {
            'T' | '1' | '2' | '3' | 't' => *count == 1,
            'C' => {
                let command_complete = CommandComplete::from_bytes(message.to_bytes()?)?;
                self.rows += command_complete.rows()?.unwrap_or(0);
                false
            }
            'Z' => false,
            'n' => *count == self.servers && !self.counters.contains_key(&'D'),
            'I' => *count == self.servers && !self.counters.contains_key(&'C'),
            _ => true,
        })
    }

    /// Number of rows returned.
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Error happened.
    pub fn error(&self) -> bool {
        self.counters.contains_key(&'E')
    }

    /// Create CommandComplete (C) message.
    pub fn command_complete(&self, command_type: CommandType) -> Option<CommandComplete> {
        if !self.counters.contains_key(&'C') || self.error() {
            return None;
        }

        let name = match command_type {
            CommandType::Delete => "DELETE",
            CommandType::Update => "UPDATE",
            CommandType::Insert => "INSERT 0",
        };

        Some(CommandComplete::new(format!("{} {}", name, self.rows())))
    }

    /// Create ReadyForQuery (C) message.
    pub fn ready_for_query(&self, in_transaction: bool) -> Option<ReadyForQuery> {
        if !self.counters.contains_key(&'Z') {
            return None;
        }

        if self.error() {
            Some(ReadyForQuery::error())
        } else {
            Some(ReadyForQuery::in_transaction(in_transaction))
        }
    }
}
