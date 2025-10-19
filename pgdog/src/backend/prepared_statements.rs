use lru::LruCache;
use std::{collections::VecDeque, sync::Arc};

use parking_lot::RwLock;

use crate::{
    frontend::{self, prepared_statements::GlobalCache},
    net::{
        messages::{parse::Parse, RowDescription},
        Close, CloseComplete, FromBytes, Message, ParseComplete, Protocol, ProtocolMessage,
        ToBytes,
    },
    stats::memory::MemoryUsage,
};

use super::Error;
use super::{
    protocol::{state::Action, ProtocolState},
    state::ExecutionCode,
};

#[derive(Debug, Clone)]
pub enum HandleResult {
    Forward,
    Drop,
    Prepend(ProtocolMessage),
}

/// Server-specific prepared statements.
///
/// The global cache has names and Parse messages,
/// while the local cache has the names of the prepared statements
/// currently prepared on the server connection.
#[derive(Debug)]
pub struct PreparedStatements {
    global_cache: Arc<RwLock<GlobalCache>>,
    local_cache: LruCache<String, ()>,
    state: ProtocolState,
    // Prepared statements being prepared now on the connection.
    parses: VecDeque<String>,
    // Describes being executed now on the connection.
    describes: VecDeque<String>,
    capacity: usize,
    memory_used: usize,
}

impl MemoryUsage for PreparedStatements {
    #[inline]
    fn memory_usage(&self) -> usize {
        self.local_cache.memory_usage()
            + self.parses.memory_usage()
            + self.describes.memory_usage()
            + self.capacity.memory_usage()
            + std::mem::size_of::<Arc<RwLock<GlobalCache>>>()
            + self.state.memory_usage()
    }
}

impl Default for PreparedStatements {
    fn default() -> Self {
        Self::new()
    }
}

impl PreparedStatements {
    /// New server prepared statements.
    pub fn new() -> Self {
        Self {
            global_cache: frontend::PreparedStatements::global(),
            local_cache: LruCache::unbounded(),
            state: ProtocolState::default(),
            parses: VecDeque::new(),
            describes: VecDeque::new(),
            capacity: usize::MAX,
            memory_used: 0,
        }
    }

    /// Set maximum prepared statements capacity.
    #[inline]
    pub fn set_capacity(&mut self, capacity: usize) {
        self.capacity = capacity;
    }

    /// Get prepared statements capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Handle extended protocol message.
    pub fn handle(&mut self, request: &ProtocolMessage) -> Result<HandleResult, Error> {
        match request {
            ProtocolMessage::Bind(bind) => {
                if !bind.anonymous() {
                    let message = self.check_prepared(bind.statement())?;
                    match message {
                        Some(message) => {
                            self.state.add_ignore('1');
                            self.parses.push_back(bind.statement().to_string());
                            self.state.add('2');
                            return Ok(HandleResult::Prepend(message));
                        }

                        None => {
                            self.state.add('2');
                        }
                    }
                } else {
                    self.state.add('2');
                }
            }
            ProtocolMessage::Describe(describe) => {
                if !describe.anonymous() {
                    let message = self.check_prepared(describe.statement())?;

                    match message {
                        Some(message) => {
                            self.state.add_ignore('1');
                            self.parses.push_back(describe.statement().to_string());
                            self.state.add(ExecutionCode::DescriptionOrNothing); // t
                            self.state.add(ExecutionCode::DescriptionOrNothing); // T
                            return Ok(HandleResult::Prepend(message));
                        }

                        None => {
                            self.state.add(ExecutionCode::DescriptionOrNothing); // t
                            self.state.add(ExecutionCode::DescriptionOrNothing);
                            // T
                        }
                    }

                    self.describes.push_back(describe.statement().to_string());
                } else if describe.is_portal() {
                    self.state.add(ExecutionCode::DescriptionOrNothing);
                } else if describe.is_statement() {
                    self.state.add(ExecutionCode::DescriptionOrNothing); // t
                    self.state.add(ExecutionCode::DescriptionOrNothing); // T
                }
            }

            ProtocolMessage::Execute(_) => {
                self.state.add(ExecutionCode::ExecutionCompleted);
            }

            ProtocolMessage::Sync(_) => {
                self.state.add('Z');
            }

            ProtocolMessage::Query(_) => {
                self.state.add('Z');
            }

            ProtocolMessage::Parse(parse) => {
                if !parse.anonymous() {
                    if self.contains(parse.name()) {
                        self.state.add_simulated(ParseComplete.message()?);
                        return Ok(HandleResult::Drop);
                    } else {
                        self.state.add('1');
                        self.parses.push_back(parse.name().to_string());
                    }
                } else {
                    self.state.add('1');
                }
            }

            ProtocolMessage::Close(close) => {
                if !close.anonymous() {
                    // We don't allow clients to close prepared statements.
                    // We manage them ourselves.
                    self.state.add_simulated(CloseComplete.message()?);
                    return Ok(HandleResult::Drop);
                } else {
                    self.state.add('3');
                }
            }
            ProtocolMessage::Prepare { .. } => (),
            ProtocolMessage::CopyDone(_) => {
                self.state.action('c')?;
            }

            ProtocolMessage::CopyFail(_) => {
                self.state.action('f')?;
            }

            ProtocolMessage::CopyData(_) | ProtocolMessage::Other(_) => (),
        }

        Ok(HandleResult::Forward)
    }

    /// Should we forward the message to the client.
    pub fn forward(&mut self, message: &Message) -> Result<bool, Error> {
        let code = message.code();
        let action = self.state.action(code)?;

        // Cleanup prepared statements state.
        match code {
            'E' => {
                // Backend ignored any subsequent extended commands.
                // These prepared statements have not been prepared, even if they
                // are syntactically valid.
                self.describes.clear();
                self.parses.clear();
            }

            'T' => {
                if let Some(describe) = self.describes.pop_front() {
                    self.add_row_description(
                        &describe,
                        &RowDescription::from_bytes(message.to_bytes()?)?,
                    );
                };
            }

            // No data for DELETEs
            'n' => {
                self.describes.pop_front();
            }

            '1' => {
                if let Some(name) = self.parses.pop_front() {
                    self.prepared(&name);
                }
            }

            'G' => {
                self.state.prepend('G'); // Next thing we'll see is a CopyFail or CopyDone.
            }

            // Backend told us the copy is done.
            'c' => {
                self.state.action(code)?;
            }

            _ => (),
        }

        match action {
            Action::Ignore => Ok(false),
            Action::Forward => Ok(true),
        }
    }

    /// Extended protocol is in sync.
    pub(crate) fn done(&self) -> bool {
        self.state.done() && self.parses.is_empty() && self.describes.is_empty()
    }

    pub(crate) fn has_more_messages(&self) -> bool {
        self.state.has_more_messages()
    }

    pub(crate) fn copy_mode(&self) -> bool {
        self.state.copy_mode()
    }

    fn check_prepared(&mut self, name: &str) -> Result<Option<ProtocolMessage>, Error> {
        if !self.contains(name) && !self.parses.iter().any(|s| s == name) {
            let parse = self.parse(name);
            if let Some(parse) = parse {
                Ok(Some(ProtocolMessage::Parse(parse)))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// The server has prepared this statement already.
    pub fn contains(&mut self, name: &str) -> bool {
        self.local_cache.promote(name)
    }

    /// Indicate this statement is prepared on the connection.
    pub fn prepared(&mut self, name: &str) {
        self.local_cache.push(name.to_owned(), ());
        self.memory_used = self.memory_usage();
    }

    /// How much memory is used by this structure, approx.
    pub fn memory_used(&self) -> usize {
        self.memory_used
    }

    /// Get the Parse message stored in the global prepared statements
    /// cache for this statement.
    pub(crate) fn parse(&self, name: &str) -> Option<Parse> {
        self.global_cache.read().parse(name)
    }

    /// Get the globally stored RowDescription for this prepared statement,
    /// if any.
    pub fn row_description(&self, name: &str) -> Option<RowDescription> {
        self.global_cache.read().row_description(name)
    }

    /// Handle a Describe message, storing the RowDescription for the
    /// statement in the global cache.
    fn add_row_description(&self, name: &str, row_description: &RowDescription) {
        self.global_cache
            .write()
            .insert_row_description(name, row_description);
    }

    /// Remove statement from local cache.
    ///
    /// This should only be done when a statement has been closed,
    /// or failed to parse.
    pub(crate) fn remove(&mut self, name: &str) -> bool {
        let exists = self.local_cache.pop(name).is_some();
        self.memory_used = self.memory_usage();
        exists
    }

    /// Indicate all prepared statements have been removed
    /// from the server connection.
    pub fn clear(&mut self) {
        self.local_cache.clear();
        self.memory_used = self.memory_usage();
    }

    /// Get current extended protocol state.
    pub fn state(&self) -> &ProtocolState {
        &self.state
    }

    /// Get mutable reference to protocol state.
    pub fn state_mut(&mut self) -> &mut ProtocolState {
        &mut self.state
    }

    /// Number of prepared statements in local (connection) cache.
    pub fn len(&self) -> usize {
        self.local_cache.len()
    }

    /// True if the local (connection) prepared statement cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Ensure capacity of prepared statements is respected.
    ///
    /// WARNING: This removes prepared statements from the cache.
    /// Make sure to actually execute the close messages you receive
    /// from this method, or the statements will be out of sync with
    /// what's actually inside Postgres.
    pub fn ensure_capacity(&mut self) -> Vec<Close> {
        let mut close = vec![];
        while self.local_cache.len() > self.capacity {
            let candidate = self.local_cache.pop_lru();

            if let Some((name, _)) = candidate {
                close.push(Close::named(&name));
            }
        }

        if !close.is_empty() {
            self.memory_used = self.memory_usage();
        }

        close
    }
}
