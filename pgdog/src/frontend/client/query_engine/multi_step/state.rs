use fnv::FnvHashMap as HashMap;

use super::super::Error;
use crate::net::{
    CommandComplete, FromBytes, Message, Protocol, ReadyForQuery, ToBytes, TransactionState,
};

#[derive(Debug, Clone)]
pub(crate) enum CommandType {
    Insert,
}

#[derive(Debug, Clone)]
pub(crate) struct MultiServerState {
    servers: usize,
    rows: usize,
    counters: HashMap<char, usize>,
    in_transaction: bool,
}

impl MultiServerState {
    /// New multi-server execution state.
    pub(crate) fn new(servers: usize) -> Self {
        Self {
            servers,
            rows: 0,
            counters: HashMap::default(),
            in_transaction: false,
        }
    }

    /// Should the message be forwarded to the client.
    pub(crate) fn forward(&mut self, message: &Message) -> Result<bool, Error> {
        let code = message.code();
        let count = self.counters.entry(code).or_default();
        *count += 1;

        Ok(match code {
            'T' | '1' | '2' | '3' | 't' => *count == 1,
            'C' => {
                let command_complete = CommandComplete::from_bytes(message.to_bytes())?;
                self.rows += command_complete.rows()?.unwrap_or(0);
                false
            }
            'Z' => {
                // track if we have opened the transaction on server
                // due to 2pc.auto. In that case we should process server
                // responses as inside the transaction.
                let state = ReadyForQuery::from_bytes(message.to_bytes())?.state()?;
                self.in_transaction |= state != TransactionState::Idle;
                false
            }
            'n' => *count == self.servers && !self.counters.contains_key(&'D'),
            'I' => *count == self.servers && !self.counters.contains_key(&'C'),
            _ => true,
        })
    }

    /// Number of rows returned.
    pub(crate) fn rows(&self) -> usize {
        self.rows
    }

    /// Error happened.
    pub(crate) fn error(&self) -> bool {
        self.counters.contains_key(&'E')
    }

    /// Create CommandComplete (C) message.
    pub(crate) fn command_complete(&self, command_type: CommandType) -> Option<CommandComplete> {
        if !self.counters.contains_key(&'C') || self.error() {
            return None;
        }

        let name = match command_type {
            CommandType::Insert => "INSERT 0",
        };

        Some(CommandComplete::new(format!("{} {}", name, self.rows())))
    }

    /// Create ReadyForQuery (Z) message from the server transaction state.
    pub(crate) fn ready_for_query(&self) -> Option<ReadyForQuery> {
        if !self.counters.contains_key(&'Z') {
            return None;
        }

        if self.error() {
            Some(ReadyForQuery::error())
        } else {
            Some(ReadyForQuery::in_transaction(self.in_transaction))
        }
    }
}
