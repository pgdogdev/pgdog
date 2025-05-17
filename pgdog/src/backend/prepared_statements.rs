use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
};

use parking_lot::Mutex;

use crate::{
    frontend::{self, prepared_statements::GlobalCache},
    net::{
        messages::{parse::Parse, RowDescription},
        CloseComplete, FromBytes, Message, ParseComplete, Protocol, ToBytes,
    },
};

use super::Error;
use super::{
    protocol::{state::Action, ProtocolMessage, ProtocolState},
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
    global_cache: Arc<Mutex<GlobalCache>>,
    local_cache: HashSet<String>,
    state: ProtocolState,
    // Prepared statements being prepared now on the connection.
    parses: VecDeque<String>,
    // Describes being executed now on the connection.
    describes: VecDeque<String>,
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
            local_cache: HashSet::new(),
            state: ProtocolState::default(),
            parses: VecDeque::new(),
            describes: VecDeque::new(),
        }
    }

    /// Handle extended protocol message.
    pub fn handle(&mut self, request: &ProtocolMessage) -> Result<HandleResult, Error> {
        match request {
            ProtocolMessage::Bind(bind) => {
                if !bind.anonymous() {
                    let message = self.check_prepared(bind.statement())?;
                    match message {
                        Some(message) => {
                            self.state.add_ignore('1', bind.statement());
                            self.prepared(bind.statement());
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
                            self.state.add_ignore('1', describe.statement());
                            self.prepared(describe.statement());
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
                } else {
                    self.state.add(ExecutionCode::DescriptionOrNothing);
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
                        self.prepared(parse.name());
                        self.state.add('1');
                        self.parses.push_back(parse.name().to_string());
                    }
                } else {
                    self.state.add('1');
                }
            }

            ProtocolMessage::CopyData(_) => (),
            ProtocolMessage::Other(_) => (),
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
                let parse = self.parses.pop_front();
                let describe = self.describes.pop_front();
                if let Some(parse) = parse {
                    self.remove(&parse);
                }
                if let Some(describe) = describe {
                    self.remove(&describe);
                }
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
                self.parses.pop_front();
            }

            _ => (),
        }

        match action {
            Action::Ignore => Ok(false),
            Action::ForwardAndRemove(names) => {
                for name in names {
                    self.remove(&name);
                }
                Ok(true)
            }
            Action::Forward => Ok(true),
        }
    }

    /// Extended protocol is in sync.
    pub(crate) fn done(&self) -> bool {
        self.state.done() && self.parses.is_empty() && self.describes.is_empty()
    }

    fn check_prepared(&mut self, name: &str) -> Result<Option<ProtocolMessage>, Error> {
        if !self.contains(name) {
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
    pub fn contains(&self, name: &str) -> bool {
        self.local_cache.contains(name)
    }

    /// Indicate this statement is prepared on the connection.
    pub fn prepared(&mut self, name: &str) {
        self.local_cache.insert(name.to_owned());
    }

    /// Get the Parse message stored in the global prepared statements
    /// cache for this statement.
    pub(crate) fn parse(&self, name: &str) -> Option<Parse> {
        self.global_cache.lock().parse(name)
    }

    /// Get the globally stored RowDescription for this prepared statement,
    /// if any.
    pub fn row_description(&self, name: &str) -> Option<RowDescription> {
        self.global_cache.lock().row_description(name)
    }

    /// Handle a Describe message, storing the RowDescription for the
    /// statement in the global cache.
    fn add_row_description(&self, name: &str, row_description: &RowDescription) {
        self.global_cache
            .lock()
            .insert_row_description(name, row_description);
    }

    /// Remove statement from local cache.
    ///
    /// This should only be done when a statement has been closed,
    /// or failed to parse.
    pub(crate) fn remove(&mut self, name: &str) -> bool {
        self.local_cache.remove(name)
    }

    /// Indicate all prepared statements have been removed
    /// from the server connection.
    pub fn clear(&mut self) {
        self.local_cache.clear();
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
}
