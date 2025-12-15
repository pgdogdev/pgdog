use fnv::FnvHashMap as HashMap;

use crate::net::{CommandComplete, ReadyForQuery};

#[derive(Debug, Clone)]
pub enum CommandType {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone)]
pub struct MultiServerState {
    servers: usize,
    counters: HashMap<char, usize>,
}

impl MultiServerState {
    /// New multi-server execution state.
    pub fn new(servers: usize) -> Self {
        Self {
            servers,
            counters: HashMap::default(),
        }
    }

    /// Should the message be forwarded to the client.
    pub fn forward(&mut self, code: char) -> bool {
        let count = self.counters.entry(code).or_default();
        *count += 1;

        match code {
            'T' | '1' | '2' | '3' | 't' => *count == 1,
            'C' | 'Z' => false,
            'n' => *count == self.servers && !self.counters.contains_key(&'D'),
            'I' => *count == self.servers && !self.counters.contains_key(&'C'),
            _ => true,
        }
    }

    /// Number of rows returned.
    pub fn rows(&self) -> usize {
        self.counters.get(&'D').copied().unwrap_or_default()
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
            CommandType::Insert => "INSERT",
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
