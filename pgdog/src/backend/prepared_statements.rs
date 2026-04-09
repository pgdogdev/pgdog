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
};

use super::Error;
use super::{
    protocol::{state::Action, ProtocolState},
    state::{ExecutionCode, ExecutionItem},
};

/// Approximate memory used by a String.
#[inline]
fn str_mem(s: &str) -> usize {
    s.len() + std::mem::size_of::<String>()
}

#[derive(Debug, Clone)]
pub enum HandleResult {
    Forward,
    Drop,
    Prepend(ProtocolMessage),
}

#[derive(Debug, Clone, PartialEq)]
struct PendingPrepare {
    name: String,
    ack: ExecutionItem,
}

impl PendingPrepare {
    fn parse(name: impl Into<String>, ignore: bool) -> Self {
        let ack = if ignore {
            ExecutionItem::Ignore(ExecutionCode::ParseComplete)
        } else {
            ExecutionItem::Code(ExecutionCode::ParseComplete)
        };

        Self {
            name: name.into(),
            ack,
        }
    }

    fn command_complete(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ack: ExecutionItem::Ignore(ExecutionCode::ExecutionCompleted),
        }
    }

    fn matches_front(&self, front: Option<&ExecutionItem>, code: char) -> bool {
        front == Some(&self.ack)
            && matches!(
                (&self.ack, code),
                (
                    ExecutionItem::Code(ExecutionCode::ParseComplete)
                        | ExecutionItem::Ignore(ExecutionCode::ParseComplete),
                    '1'
                ) | (
                    ExecutionItem::Code(ExecutionCode::ExecutionCompleted)
                        | ExecutionItem::Ignore(ExecutionCode::ExecutionCompleted),
                    'C'
                )
            )
    }
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
    // Prepared statements waiting for a specific backend acknowledgement.
    pending_prepares: VecDeque<PendingPrepare>,
    // Describes being executed now on the connection.
    describes: VecDeque<String>,
    capacity: usize,
    memory_used: usize,
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
            pending_prepares: VecDeque::new(),
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
                            self.pending_prepares
                                .push_back(PendingPrepare::parse(bind.statement(), true));
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
                            self.pending_prepares
                                .push_back(PendingPrepare::parse(describe.statement(), true));
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
                        self.pending_prepares
                            .push_back(PendingPrepare::parse(parse.name(), false));
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
            ProtocolMessage::Prepare { name, .. } => {
                if self.contains(name) || self.pending_prepare(name) {
                    return Ok(HandleResult::Drop);
                } else {
                    self.pending_prepares
                        .push_back(PendingPrepare::command_complete(name.clone()));
                    self.state.add_ignore('C');
                    self.state.add_ignore('Z');
                    return Ok(HandleResult::Forward);
                }
            }
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
        let prepare_completed = self
            .pending_prepares
            .front()
            .map(|pending| pending.matches_front(self.state.front(), code))
            .unwrap_or(false);
        let action = self.state.action(code)?;

        // Cleanup prepared statements state.
        match code {
            'E' => {
                // Backend ignored any subsequent extended commands.
                // These prepared statements have not been prepared, even if they
                // are syntactically valid.
                self.describes.clear();
                self.pending_prepares.clear();
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

            '1' | 'C' => {
                if prepare_completed {
                    if let Some(pending) = self.pending_prepares.pop_front() {
                        self.prepared(&pending.name);
                    }
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
        self.state.done() && self.pending_prepares.is_empty() && self.describes.is_empty()
    }

    /// The server connection has more messages to send
    /// to the client.
    pub(crate) fn has_more_messages(&self) -> bool {
        self.state.has_more_messages()
    }

    /// The server connection is in COPY mode.
    pub(crate) fn in_copy_mode(&self) -> bool {
        self.state.in_copy_mode()
    }

    /// The protocol is out of sync due to an error in extended protocol.
    pub(crate) fn out_of_sync(&self) -> bool {
        self.state.out_of_sync()
    }

    fn check_prepared(&mut self, name: &str) -> Result<Option<ProtocolMessage>, Error> {
        if !self.contains(name) && !self.pending_prepare(name) {
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

    fn pending_prepare(&self, name: &str) -> bool {
        self.pending_prepares
            .iter()
            .any(|pending| pending.name == name)
    }

    /// The server has prepared this statement already.
    pub fn contains(&mut self, name: &str) -> bool {
        self.local_cache.promote(name)
    }

    /// Indicate this statement is prepared on the connection.
    pub fn prepared(&mut self, name: &str) {
        self.memory_used += str_mem(name);
        self.local_cache.push(name.to_owned(), ());
    }

    /// How much memory is used by this structure, approx.
    pub fn memory_used(&self) -> usize {
        self.memory_used
    }

    /// Get the Parse message stored in the global prepared statements
    /// cache for this statement.
    pub(crate) fn parse(&self, name: &str) -> Option<Parse> {
        self.global_cache.read().rewritten_parse(name)
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
        if self.local_cache.pop(name).is_some() {
            self.memory_used = self.memory_used.saturating_sub(str_mem(name));
            true
        } else {
            false
        }
    }

    /// Indicate all prepared statements have been removed
    /// from the server connection.
    pub fn clear(&mut self) {
        self.local_cache.clear();
        self.memory_used = 0;
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
                self.memory_used = self.memory_used.saturating_sub(str_mem(&name));
            }
        }

        close
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::{CommandComplete, Execute, Query, ReadyForQuery};

    #[test]
    fn test_parse_waits_for_parse_complete() {
        let mut prepared = PreparedStatements::new();
        let name = "mixed_ack_stmt";

        prepared.handle(&Execute::new().into()).unwrap();
        prepared
            .handle(&Parse::named(name, "SELECT 1").into())
            .unwrap();

        prepared
            .forward(&CommandComplete::new("SELECT 1").message().unwrap())
            .unwrap();
        assert!(
            !prepared.contains(name),
            "execution CommandComplete must not complete a pending Parse"
        );

        prepared.forward(&ParseComplete.message().unwrap()).unwrap();
        assert!(
            prepared.contains(name),
            "ParseComplete should mark the statement as prepared"
        );
    }

    #[test]
    fn test_simple_prepare_waits_for_its_own_command_complete() {
        let mut prepared = PreparedStatements::new();
        let name = "__pgdog_simple_prepare";
        let prepare = ProtocolMessage::Prepare {
            name: name.into(),
            statement: "SELECT 1".into(),
        };

        prepared.handle(&Query::new("BEGIN").into()).unwrap();
        prepared.handle(&prepare).unwrap();

        prepared
            .forward(&CommandComplete::new_begin().message().unwrap())
            .unwrap();
        assert!(
            !prepared.contains(name),
            "unrelated CommandComplete must not complete an in-flight PREPARE"
        );

        prepared
            .forward(&ReadyForQuery::idle().message().unwrap())
            .unwrap();
        prepared
            .forward(&CommandComplete::new("PREPARE").message().unwrap())
            .unwrap();
        assert!(
            prepared.contains(name),
            "the PREPARE command's own CommandComplete should complete it"
        );
    }

    #[test]
    fn test_duplicate_simple_prepare_is_dropped_while_pending() {
        let mut prepared = PreparedStatements::new();
        let prepare = ProtocolMessage::Prepare {
            name: "__pgdog_dup".into(),
            statement: "SELECT 1".into(),
        };

        assert!(matches!(
            prepared.handle(&prepare).unwrap(),
            HandleResult::Forward
        ));
        assert!(matches!(
            prepared.handle(&prepare).unwrap(),
            HandleResult::Drop
        ));
    }
}
