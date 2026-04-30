use lru::LruCache;
use std::{collections::VecDeque, sync::Arc};

use crate::{
    frontend::{self, prepared_statements::GlobalCache},
    net::{
        messages::{parse::Parse, RowDescription},
        Close, CloseComplete, FromBytes, Message, ParseComplete, Protocol, ProtocolMessage,
        ToBytes,
    },
};
use parking_lot::RwLock;
use pgdog_config::PreparedStatements as PreparedStatementsLevel;

use super::Error;
use super::{
    protocol::{state::Action, ProtocolState},
    state::ExecutionCode,
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
    Rewrite(ProtocolMessage),
    PrependRewrite {
        prepend: ProtocolMessage,
        rewrite: ProtocolMessage,
    },
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
    level: PreparedStatementsLevel,
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
            level: PreparedStatementsLevel::default(),
        }
    }

    /// Set maximum prepared statements capacity.
    #[inline]
    pub fn set_capacity(&mut self, capacity: usize) {
        self.capacity = capacity;
    }

    #[inline]
    pub fn set_prepared_statements_level(&mut self, level: PreparedStatementsLevel) {
        self.level = level;
    }

    /// Get prepared statements capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Force the server to ignore the response to this message.
    ///
    /// This is done to inject messages into the extended request flow
    /// to set the connection into a particular state.
    ///
    pub(super) fn handle_ignore(&mut self, request: &ProtocolMessage) -> Result<(), Error> {
        match request {
            ProtocolMessage::Parse(_) => {
                self.state.add_ignore('1');
                Ok(())
            }
            _ => Err(Error::UnsupportedHandleIgnore(request.code())),
        }
    }

    /// Handle extended protocol message.
    pub fn handle(&mut self, request: &ProtocolMessage) -> Result<HandleResult, Error> {
        match request {
            ProtocolMessage::Bind(bind) => {
                if !bind.anonymous() {
                    let message = self.check_prepared(bind.statement())?;
                    match message {
                        Some(mut message) => {
                            self.state.add_ignore('1');
                            self.parses.push_back(bind.statement().to_string());
                            self.state.add('2');
                            if self.level.rewrite_anonymous() {
                                message.anonymize();
                                let mut bind = bind.clone();
                                bind.anonymize();
                                return Ok(HandleResult::PrependRewrite {
                                    prepend: message,
                                    rewrite: ProtocolMessage::Bind(bind),
                                });
                            } else {
                                return Ok(HandleResult::Prepend(message));
                            }
                        }

                        None => {
                            self.state.add('2');
                            if self.level.rewrite_anonymous() {
                                let mut bind = bind.clone();
                                bind.anonymize();
                                return Ok(HandleResult::Rewrite(ProtocolMessage::Bind(bind)));
                            }
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
                        Some(mut message) => {
                            self.state.add_ignore('1');
                            self.parses.push_back(describe.statement().to_string());
                            self.state.add(ExecutionCode::DescriptionOrNothing); // t
                            self.state.add(ExecutionCode::DescriptionOrNothing); // T

                            if self.level.rewrite_anonymous() {
                                // Save the RowDescription because
                                // we don't actually save prepared statements in the server
                                // anymore so they can be different every time.
                                self.describes.push_back(describe.statement().to_string());

                                message.anonymize();
                                let mut describe = describe.clone();
                                describe.anonymize();
                                return Ok(HandleResult::PrependRewrite {
                                    prepend: message,
                                    rewrite: ProtocolMessage::Describe(describe),
                                });
                            } else {
                                return Ok(HandleResult::Prepend(message));
                            }
                        }

                        None => {
                            self.state.add(ExecutionCode::DescriptionOrNothing); // t
                            self.state.add(ExecutionCode::DescriptionOrNothing);
                            // T
                            self.describes.push_back(describe.statement().to_string());
                            if self.level.rewrite_anonymous() {
                                let mut describe = describe.clone();
                                describe.anonymize();
                                return Ok(HandleResult::Rewrite(ProtocolMessage::Describe(
                                    describe,
                                )));
                            }
                        }
                    }
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
                    // The client is sending named prepared statements,
                    // but we're in ExtendedAnonymous mode so we rewrite
                    // them to anonymous to avoid storing them in Postgres.
                    if self.level.rewrite_anonymous() {
                        let mut parse = parse.clone();
                        parse.anonymize();
                        return Ok(HandleResult::Rewrite(ProtocolMessage::Parse(parse)));
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
                if self.contains(name) {
                    return Ok(HandleResult::Drop);
                } else {
                    self.parses.push_back(name.clone());
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

            '1' | 'C' => {
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

        // Reset cache, forcing all Bind/Execute, Describe, solo requests
        // to always re-prepare the statement next time it's sent.
        if !self.has_more_messages() && self.level.rewrite_anonymous() {
            self.clear();
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
mod test {
    use super::*;
    use crate::frontend::PreparedStatements as FrontendPreparedStatements;
    use crate::net::{
        bind::Parameter, messages::ReadyForQuery, Bind, Describe, Execute, Message, Parse,
        ProtocolMessage, Query, Sync,
    };
    use pgdog_config::PreparedStatements as PreparedStatementsLevel;

    /// Build a PreparedStatements instance configured for ExtendedAnonymous mode.
    fn new_extended_anonymous() -> PreparedStatements {
        let mut ps = PreparedStatements::new();
        ps.set_prepared_statements_level(PreparedStatementsLevel::ExtendedAnonymous);
        ps
    }

    /// Build a PreparedStatements instance configured for Extended (default) mode.
    fn new_extended() -> PreparedStatements {
        let mut ps = PreparedStatements::new();
        ps.set_prepared_statements_level(PreparedStatementsLevel::Extended);
        ps
    }

    /// Insert a prepared statement into the global cache so check_prepared can find it.
    fn insert_global(name: &str, query: &str) -> String {
        let parse = Parse::named(name, query);
        let (_, rewritten_name) = FrontendPreparedStatements::global().write().insert(&parse);
        rewritten_name
    }

    // -------------------------------------------------------
    // Parse message tests
    // -------------------------------------------------------

    #[test]
    fn parse_named_extended_mode_forwards() {
        let mut ps = new_extended();
        let parse = Parse::named("stmt1", "SELECT 1");
        let result = ps.handle(&ProtocolMessage::Parse(parse)).unwrap();
        assert!(matches!(result, HandleResult::Forward));
    }

    #[test]
    fn parse_named_extended_anonymous_mode_rewrites_to_anonymous() {
        let mut ps = new_extended_anonymous();
        let parse = Parse::named("stmt1", "SELECT 1");
        let result = ps.handle(&ProtocolMessage::Parse(parse)).unwrap();
        match result {
            HandleResult::Rewrite(ProtocolMessage::Parse(p)) => {
                assert!(p.anonymous(), "Parse should be anonymized");
                assert_eq!(p.query(), "SELECT 1");
            }
            other => panic!("expected Rewrite(Parse), got {:?}", other),
        }
    }

    #[test]
    fn parse_anonymous_unchanged_in_extended_anonymous_mode() {
        let mut ps = new_extended_anonymous();
        let parse = Parse::new_anonymous("SELECT 1");
        let result = ps.handle(&ProtocolMessage::Parse(parse)).unwrap();
        assert!(matches!(result, HandleResult::Forward));
    }

    #[test]
    fn parse_already_prepared_returns_drop_in_extended_mode() {
        let mut ps = new_extended();
        // Simulate the statement being already prepared on this connection.
        ps.prepared("stmt1");
        let parse = Parse::named("stmt1", "SELECT 1");
        let result = ps.handle(&ProtocolMessage::Parse(parse)).unwrap();
        assert!(matches!(result, HandleResult::Drop));
    }

    #[test]
    fn parse_already_prepared_returns_rewrite_in_extended_anonymous() {
        let mut ps = new_extended_anonymous();
        // Simulate the statement being already prepared on this connection.
        ps.prepared("stmt1");
        let parse = Parse::named("stmt1", "SELECT 1");
        let result = ps.handle(&ProtocolMessage::Parse(parse)).unwrap();
        // contains() returns true so Drop is returned before the rewrite_anonymous check.
        assert!(matches!(result, HandleResult::Drop));
    }

    // -------------------------------------------------------
    // Bind message tests
    // -------------------------------------------------------

    #[test]
    fn bind_named_not_in_cache_extended_mode_forwards() {
        let mut ps = new_extended();
        let bind = Bind::new_statement("stmt1");
        let result = ps.handle(&ProtocolMessage::Bind(bind)).unwrap();
        // Not in cache, no global parse -> Forward
        assert!(matches!(result, HandleResult::Forward));
    }

    #[test]
    fn bind_named_not_in_cache_extended_anonymous_rewrites() {
        let mut ps = new_extended_anonymous();
        let bind = Bind::new_statement("stmt1");
        let result = ps.handle(&ProtocolMessage::Bind(bind)).unwrap();
        match result {
            HandleResult::Rewrite(ProtocolMessage::Bind(b)) => {
                assert!(b.anonymous(), "Bind should be anonymized");
            }
            other => panic!("expected Rewrite(Bind), got {:?}", other),
        }
    }

    #[test]
    fn bind_anonymous_unchanged_in_extended_anonymous() {
        let mut ps = new_extended_anonymous();
        let bind = Bind::new_statement("");
        let result = ps.handle(&ProtocolMessage::Bind(bind)).unwrap();
        assert!(matches!(result, HandleResult::Forward));
    }

    #[test]
    fn bind_named_in_global_cache_extended_mode_prepends() {
        let mut ps = new_extended();
        let name = insert_global("my_stmt", "SELECT $1");
        let bind = Bind::new_statement(&name);
        let result = ps.handle(&ProtocolMessage::Bind(bind)).unwrap();
        match result {
            HandleResult::Prepend(ProtocolMessage::Parse(p)) => {
                assert_eq!(p.query(), "SELECT $1");
            }
            other => panic!("expected Prepend(Parse), got {:?}", other),
        }
    }

    #[test]
    fn bind_named_in_global_cache_extended_anonymous_prepend_rewrite() {
        let mut ps = new_extended_anonymous();
        let name = insert_global("bind_test", "SELECT $1");
        let bind = Bind::new_statement(&name);
        let result = ps.handle(&ProtocolMessage::Bind(bind)).unwrap();
        match result {
            HandleResult::PrependRewrite { prepend, rewrite } => {
                // The prepended Parse should be anonymized.
                if let ProtocolMessage::Parse(p) = &prepend {
                    assert!(p.anonymous(), "prepended Parse should be anonymous");
                    assert_eq!(p.query(), "SELECT $1");
                } else {
                    panic!("expected prepend to be Parse");
                }
                // The rewritten Bind should be anonymized.
                if let ProtocolMessage::Bind(b) = &rewrite {
                    assert!(b.anonymous(), "rewritten Bind should be anonymous");
                } else {
                    panic!("expected rewrite to be Bind");
                }
            }
            other => panic!("expected PrependRewrite, got {:?}", other),
        }
    }

    // -------------------------------------------------------
    // Describe message tests
    // -------------------------------------------------------

    #[test]
    fn describe_named_not_in_cache_extended_anonymous_rewrites() {
        let mut ps = new_extended_anonymous();
        let describe = Describe::new_statement("stmt1");
        let result = ps.handle(&ProtocolMessage::Describe(describe)).unwrap();
        match result {
            HandleResult::Rewrite(ProtocolMessage::Describe(d)) => {
                assert!(d.anonymous(), "Describe should be anonymized");
            }
            other => panic!("expected Rewrite(Describe), got {:?}", other),
        }
    }

    #[test]
    fn describe_named_in_global_cache_extended_anonymous_prepend_rewrite() {
        let mut ps = new_extended_anonymous();
        let name = insert_global("desc_test", "SELECT $1");
        let describe = Describe::new_statement(&name);
        let result = ps.handle(&ProtocolMessage::Describe(describe)).unwrap();
        match result {
            HandleResult::PrependRewrite { prepend, rewrite } => {
                if let ProtocolMessage::Parse(p) = &prepend {
                    assert!(p.anonymous(), "prepended Parse should be anonymous");
                } else {
                    panic!("expected prepend to be Parse");
                }
                if let ProtocolMessage::Describe(d) = &rewrite {
                    assert!(d.anonymous(), "rewritten Describe should be anonymous");
                } else {
                    panic!("expected rewrite to be Describe");
                }
            }
            other => panic!("expected PrependRewrite, got {:?}", other),
        }
    }

    #[test]
    fn describe_named_not_in_cache_extended_mode_forwards() {
        let mut ps = new_extended();
        let describe = Describe::new_statement("stmt1");
        let result = ps.handle(&ProtocolMessage::Describe(describe)).unwrap();
        assert!(matches!(result, HandleResult::Forward));
    }

    #[test]
    fn describe_portal_unchanged_in_extended_anonymous() {
        let mut ps = new_extended_anonymous();
        let describe = Describe::new_portal("myportal");
        let result = ps.handle(&ProtocolMessage::Describe(describe)).unwrap();
        // Portal describes are not rewritten.
        assert!(matches!(result, HandleResult::Forward));
    }

    // -------------------------------------------------------
    // Close message tests
    // -------------------------------------------------------

    #[test]
    fn close_named_is_dropped_in_both_modes() {
        for mut ps in [new_extended(), new_extended_anonymous()] {
            let result = ps
                .handle(&ProtocolMessage::Close(Close::named("stmt1")))
                .unwrap();
            assert!(
                matches!(result, HandleResult::Drop),
                "named Close should be dropped"
            );
        }
    }

    // -------------------------------------------------------
    // Forward response: cache clearing in extended_anonymous
    // -------------------------------------------------------

    #[test]
    fn forward_clears_local_cache_on_ready_for_query_in_extended_anonymous() {
        let mut ps = new_extended_anonymous();
        ps.prepared("stmt1");
        ps.prepared("stmt2");
        assert_eq!(ps.len(), 2);

        // Simulate a ReadyForQuery message.
        // First we need to add a 'Z' to the state so we can action it.
        ps.state.add('Z');
        let rfq = Message::new(ReadyForQuery::idle().to_bytes().unwrap());
        ps.forward(&rfq).unwrap();

        // In extended_anonymous mode, cache should be cleared after done.
        assert_eq!(ps.len(), 0, "local cache should be cleared after RFQ");
    }

    #[test]
    fn forward_keeps_cache_on_ready_for_query_in_extended_mode() {
        let mut ps = new_extended();
        ps.prepared("stmt1");
        ps.prepared("stmt2");
        assert_eq!(ps.len(), 2);

        ps.state.add('Z');
        let rfq = Message::new(ReadyForQuery::idle().to_bytes().unwrap());
        ps.forward(&rfq).unwrap();

        // In extended mode, cache should be preserved.
        assert_eq!(
            ps.len(),
            2,
            "local cache should be preserved in extended mode"
        );
    }

    // -------------------------------------------------------
    // Full Parse-Bind-Execute-Sync cycle tests
    // -------------------------------------------------------

    #[test]
    fn full_cycle_extended_anonymous_all_messages_anonymized() {
        let mut ps = new_extended_anonymous();

        // Parse
        let parse = Parse::named("stmt1", "SELECT $1");
        let result = ps.handle(&ProtocolMessage::Parse(parse)).unwrap();
        match &result {
            HandleResult::Rewrite(ProtocolMessage::Parse(p)) => {
                assert!(p.anonymous());
            }
            other => panic!("expected Rewrite(Parse), got {:?}", other),
        }

        // Bind
        let bind = Bind::new_params(
            "stmt1",
            &[Parameter {
                len: 1,
                data: "1".as_bytes().into(),
            }],
        );
        let result = ps.handle(&ProtocolMessage::Bind(bind)).unwrap();
        match &result {
            HandleResult::Rewrite(ProtocolMessage::Bind(b)) => {
                assert!(b.anonymous());
            }
            other => panic!("expected Rewrite(Bind), got {:?}", other),
        }

        // Execute
        let result = ps
            .handle(&ProtocolMessage::Execute(Execute::new()))
            .unwrap();
        assert!(matches!(result, HandleResult::Forward));

        // Sync
        let result = ps.handle(&ProtocolMessage::Sync(Sync)).unwrap();
        assert!(matches!(result, HandleResult::Forward));
    }

    #[test]
    fn full_cycle_extended_mode_no_rewriting() {
        let mut ps = new_extended();

        let parse = Parse::named("stmt1", "SELECT $1");
        let result = ps.handle(&ProtocolMessage::Parse(parse)).unwrap();
        assert!(matches!(result, HandleResult::Forward));

        let bind = Bind::new_params(
            "stmt1",
            &[Parameter {
                len: 1,
                data: "1".as_bytes().into(),
            }],
        );
        let result = ps.handle(&ProtocolMessage::Bind(bind)).unwrap();
        assert!(matches!(result, HandleResult::Forward));

        let result = ps
            .handle(&ProtocolMessage::Execute(Execute::new()))
            .unwrap();
        assert!(matches!(result, HandleResult::Forward));

        let result = ps.handle(&ProtocolMessage::Sync(Sync)).unwrap();
        assert!(matches!(result, HandleResult::Forward));
    }

    // -------------------------------------------------------
    // Simple query is unaffected by mode
    // -------------------------------------------------------

    #[test]
    fn simple_query_unaffected_by_extended_anonymous() {
        let mut ps = new_extended_anonymous();
        let result = ps
            .handle(&ProtocolMessage::Query(Query::new("SELECT 1")))
            .unwrap();
        assert!(matches!(result, HandleResult::Forward));
    }

    // -------------------------------------------------------
    // Execute/Sync are always forwarded regardless of mode
    // -------------------------------------------------------

    #[test]
    fn execute_and_sync_always_forward() {
        for mut ps in [new_extended(), new_extended_anonymous()] {
            let result = ps
                .handle(&ProtocolMessage::Execute(Execute::new()))
                .unwrap();
            assert!(matches!(result, HandleResult::Forward));

            let result = ps.handle(&ProtocolMessage::Sync(Sync)).unwrap();
            assert!(matches!(result, HandleResult::Forward));
        }
    }
}
