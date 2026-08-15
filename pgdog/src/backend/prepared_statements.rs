use lru::LruCache;
use std::{
    collections::VecDeque,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::util::time::deadline;
use crate::{
    frontend::{self, prepared_statements::GlobalCache},
    net::{
        Close, CloseComplete, FromBytes, Message, ParseComplete, Protocol, ProtocolMessage,
        ToBytes,
        messages::{ParameterDescription, RowDescription, parse::Parse},
    },
};
use parking_lot::RwLock;
use pgdog_stats::PreparedStatementsConfig;

use super::{Error, Oids};
use super::{
    protocol::{ProtocolState, state::Action},
    state::ExecutionCode,
};

/// Rough size of one local cache entry. Ignores the LRU node itself,
/// so it undercounts a little.
#[inline]
fn entry_mem(s: &str) -> usize {
    s.len() + std::mem::size_of::<String>() + std::mem::size_of::<LocalStatement>()
}

/// A statement info prepared on this connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LocalStatement {
    /// When this statement should be replanned
    deadline: Option<Instant>,
}

impl LocalStatement {
    fn new(ttl: Option<Duration>, jitter: Duration) -> Self {
        Self {
            deadline: ttl.map(|ttl| deadline(ttl, jitter)),
        }
    }

    /// Check for expired
    ///
    /// If the check is called and deadline is not set then it's marked as expired
    /// to cover the case when the TTL was set after the statement creation
    fn expired(&self, now: Instant) -> bool {
        self.deadline.is_none_or(|deadline| deadline <= now)
    }
}

/// A statement that has to be run before client messages
#[derive(Debug, Clone, PartialEq)]
pub(super) struct Prepare {
    /// Some if statement was prepared previously, but has expired since
    close: Option<ProtocolMessage>,
    parse: ProtocolMessage,
}

impl Prepare {
    /// The stale statement to close first, if the name is taken.
    pub(super) fn close(&self) -> Option<&ProtocolMessage> {
        self.close.as_ref()
    }

    pub(super) fn parse(&self) -> &ProtocolMessage {
        &self.parse
    }

    fn anonymize(&mut self) {
        self.parse.anonymize();
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum HandleResult {
    Drop,
    Forward,
    Rewrite(ProtocolMessage),
    Prepend(Prepare),
    PrependRewrite {
        prepend: Prepare,
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
    local_cache: LruCache<String, LocalStatement>,
    state: ProtocolState,
    // Prepared statements being prepared now on the connection.
    parses: VecDeque<String>,
    // Describes being executed now on the connection.
    describes: VecDeque<String>,
    config: PreparedStatementsConfig,
    memory_used: usize,
    oids: Arc<Oids>,
}

#[cfg(test)]
impl Default for PreparedStatements {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

impl PreparedStatements {
    /// New server prepared statements.
    pub(crate) fn new(oids: Arc<Oids>) -> Self {
        Self {
            global_cache: frontend::PreparedStatements::global(),
            local_cache: LruCache::unbounded(),
            state: ProtocolState::default(),
            parses: VecDeque::new(),
            describes: VecDeque::new(),
            config: PreparedStatementsConfig::default(),
            memory_used: 0,
            oids,
        }
    }

    /// Apply the pool's prepared statement settings.
    #[inline]
    pub fn configure(&mut self, config: PreparedStatementsConfig) {
        self.config = config;
    }

    /// Current prepared statement settings.
    pub fn config(&self) -> PreparedStatementsConfig {
        self.config
    }

    /// Get prepared statements capacity.
    pub fn capacity(&self) -> usize {
        self.config.limit
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
    pub(super) fn handle(&mut self, request: &ProtocolMessage) -> Result<HandleResult, Error> {
        match request {
            ProtocolMessage::Bind(bind) => {
                if !bind.anonymous() {
                    let message = self.check_prepared(bind.statement())?;
                    match message {
                        Some(mut message) => {
                            if message.close.is_some() {
                                self.state.add_ignore('3');
                            }
                            self.state.add_ignore('1');
                            self.parses.push_back(bind.statement().to_string());
                            self.state.add('2');
                            if self.config.level.rewrite_anonymous() {
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
                            if self.config.level.rewrite_anonymous() {
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
                            if message.close.is_some() {
                                self.state.add_ignore('3');
                            }
                            self.state.add_ignore('1');
                            self.parses.push_back(describe.statement().to_string());
                            self.state.add(ExecutionCode::DescriptionOrNothing); // t
                            self.state.add(ExecutionCode::DescriptionOrNothing); // T

                            if self.config.level.rewrite_anonymous() {
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
                            if self.config.level.rewrite_anonymous() {
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
                self.state.add(ExecutionCode::ReadyForQuerySync);
            }

            ProtocolMessage::Query(_) => {
                self.state.add(ExecutionCode::ReadyForQuery);
            }

            ProtocolMessage::Parse(parse) => {
                let mut parse = parse.clone();
                let mut rewritten = self.rewrite_parse_data_types(&mut parse);

                if !parse.anonymous() {
                    if self.contains(parse.name()) {
                        self.state.add_simulated(ParseComplete.message()?);
                        return Ok(HandleResult::Drop);
                    } else {
                        self.parses.push_back(parse.name().to_string());
                    }
                    // The client is sending named prepared statements,
                    // but we're in ExtendedAnonymous mode so we rewrite
                    // them to anonymous to avoid storing them in Postgres.
                    if self.config.level.rewrite_anonymous() {
                        parse.anonymize();
                        rewritten = true;
                    }
                }

                self.state.add('1');
                if rewritten {
                    return Ok(HandleResult::Rewrite(ProtocolMessage::Parse(parse)));
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
            ProtocolMessage::Prepare(prepare) => {
                if self.contains(prepare.name()) {
                    return Ok(HandleResult::Drop);
                } else {
                    self.parses.push_back(prepare.name().to_string());
                    self.state.add_ignore('C');

                    // Prepare turns into a Simple Query ('Q') so it expects a regular RFQ back.
                    self.state.add_ignore(ExecutionCode::ReadyForQuery);
                    return Ok(HandleResult::Forward);
                }
            }
            ProtocolMessage::CopyDone(_) => {
                self.state.action('c')?;
            }

            ProtocolMessage::CopyFail(_) => {
                self.state.action('f')?;
            }

            ProtocolMessage::CopyData(_) => (),

            // Fastpath (F): backend responds with FunctionCallResponse (V) + ReadyForQuery (Z).
            // V is Untracked and passes through; register Z so the response loop runs.
            ProtocolMessage::Fastpath(_) => {
                // If we have an extended error prior, this should be dropped.
                self.state.add(ExecutionCode::ReadyForQuery);
            }

            ProtocolMessage::Other(_) => (),
        }

        Ok(HandleResult::Forward)
    }

    /// Should we forward the message to the client.
    pub(crate) fn forward(&mut self, message: &mut Message) -> Result<bool, Error> {
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
                let maybe_row_description = self.parse_and_rewrite_row_description(message)?;
                if let Some(describe) = self.describes.pop_front() {
                    let row_description = maybe_row_description
                        .map(Ok)
                        .unwrap_or_else(|| RowDescription::from_bytes(message.payload()))?;
                    self.add_row_description(&describe, row_description);
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

            // The close statement that is ignored and we have the parse for
            // means we're repreparing the statement right now, so
            // drop from cache first and let it be readded later on ParseComplete
            '3' if matches!(action, Action::Ignore) => {
                // ok, pop_front -> push_front just to avoid borrowing issues
                // and not to copy the name just to remove by name
                if let Some(name) = self.parses.pop_front() {
                    self.remove(&name);
                    self.parses.push_front(name);
                }
            }

            'G' => {
                self.state.prepend('G'); // Next thing we'll see is a CopyFail or CopyDone.
            }

            // Backend told us the copy is done.
            'c' => {
                self.state.action(code)?;
            }

            't' => {
                self.rewrite_parameter_description_data_types(message)?;
            }

            _ => (),
        }

        // Reset cache, forcing all Bind/Execute, Describe, solo requests
        // to always re-prepare the statement next time it's sent.
        if !self.has_more_messages() && self.config.level.rewrite_anonymous() {
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

    /// Check the prepared state to identify if we need
    /// to run something before actual client's requests
    fn check_prepared(&mut self, name: &str) -> Result<Option<Prepare>, Error> {
        // Ignore if we already have a Parse in progress.
        if self.parses.iter().any(|s| s == name) {
            return Ok(None);
        }

        let entry = self.local_cache.get(name);
        let expired =
            self.config.ttl.is_some() && entry.is_some_and(|entry| entry.expired(Instant::now()));

        if entry.is_some() && !expired {
            return Ok(None);
        }

        // Nothing to prepare it from, so leave whatever is there alone.
        let Some(parse) = self.parse(name) else {
            return Ok(None);
        };

        Ok(Some(Prepare {
            // Postgres still has the expired statement under this name, so
            // we need to close it first to reprepare.
            //
            // The entry stays in the cache until CloseComplete confirms the
            // drop: an error earlier in the batch makes Postgres skip our
            // Close, and dropping it here would leave us re-preparing a name
            // it still holds.
            close: expired.then(|| ProtocolMessage::Close(Close::named(name))),
            parse: ProtocolMessage::Parse(parse),
        }))
    }

    /// The server has prepared this statement already.
    pub fn contains(&mut self, name: &str) -> bool {
        self.local_cache.promote(name)
    }

    #[cfg(test)]
    fn statement(&self, name: &str) -> Option<&LocalStatement> {
        self.local_cache.peek(name)
    }

    pub fn prepared(&mut self, name: &str) {
        let statement = LocalStatement::new(self.config.ttl, self.config.ttl_jitter);

        // Cache is unbounded, so anything handed back is the old entry
        // for this same name, never an eviction. Only new names cost us.
        if self.local_cache.push(name.to_owned(), statement).is_none() {
            self.memory_used += entry_mem(name);
        }
    }

    /// How much memory is used by this structure, approx.
    pub fn memory_used(&self) -> usize {
        self.memory_used
    }

    /// Get the Parse message stored in the global prepared statements
    /// cache for this statement.
    pub(crate) fn parse(&self, name: &str) -> Option<Parse> {
        self.global_cache
            .read()
            .rewritten_parse(name)
            .map(|mut parse| {
                self.rewrite_parse_data_types(&mut parse);
                parse
            })
    }

    /// Handle a Describe message, storing the RowDescription for the
    /// statement in the global cache.
    fn add_row_description(&self, name: &str, row_description: RowDescription) {
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
            self.memory_used = self.memory_used.saturating_sub(entry_mem(name));
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
    #[must_use]
    pub fn ensure_capacity(&mut self) -> Vec<Close> {
        let mut close = vec![];
        while self.local_cache.len() > self.config.limit {
            let candidate = self.local_cache.pop_lru();

            if let Some((name, _)) = candidate {
                close.push(Close::named(&name));
                self.memory_used = self.memory_used.saturating_sub(entry_mem(&name));
            }
        }

        close
    }

    pub(crate) fn replace_oids(&mut self, oids: &Arc<Oids>) {
        self.oids = Arc::clone(oids)
    }

    fn rewrite_parse_data_types(&self, parse: &mut Parse) -> bool {
        let Some(mappings) = self.oids.get() else {
            return false;
        };
        parse.rewrite_data_types(&mappings.canonical_to_shard)
    }

    /// Rewrite the given RowDescription Message to have the canonical set of
    /// OIDs. Returns the parsed RowDescription if parsing occurred
    fn parse_and_rewrite_row_description(
        &self,
        message: &mut Message,
    ) -> Result<Option<RowDescription>, Error> {
        // RowDescription is emitted during cluster startup, so we can't
        // require OIDs to be loaded.
        let empty_mapping = Default::default();
        let mappings = &self.oids.get().unwrap_or(&empty_mapping).shard_to_canonical;

        if !mappings.is_empty() {
            let mut row_description = RowDescription::from_bytes(message.payload())?;
            if row_description.rewrite_data_types(mappings) {
                message.replace_payload(row_description.to_bytes());
            }
            Ok(Some(row_description))
        } else {
            Ok(None)
        }
    }

    fn rewrite_parameter_description_data_types(&self, message: &mut Message) -> Result<(), Error> {
        let Some(mappings) = self.oids.get() else {
            return Ok(());
        };
        let mappings = &mappings.shard_to_canonical;
        if mappings.is_empty() {
            return Ok(());
        }

        let mut parameter_description = ParameterDescription::from_bytes(message.payload())?;
        parameter_description.rewrite_data_types(mappings);
        message.replace_payload(parameter_description.to_bytes());
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::frontend::PreparedStatements as FrontendPreparedStatements;
    use crate::net::{
        Bind, Describe, ErrorResponse, Execute, Message, Parse, ProtocolMessage, Query, Sync,
        bind::Parameter, messages::ReadyForQuery,
    };
    use pgdog_config::PreparedStatements as PreparedStatementsLevel;

    /// Build a PreparedStatements instance configured for ExtendedAnonymous mode.
    fn new_extended_anonymous() -> PreparedStatements {
        new_with_level(PreparedStatementsLevel::ExtendedAnonymous)
    }

    /// Build a PreparedStatements instance configured for Extended (default) mode.
    fn new_extended() -> PreparedStatements {
        new_with_level(PreparedStatementsLevel::Extended)
    }

    fn new_with_level(level: PreparedStatementsLevel) -> PreparedStatements {
        let mut ps = PreparedStatements::default();
        ps.configure(PreparedStatementsConfig {
            level,
            ..ps.config()
        });
        ps
    }

    const TTL: Duration = Duration::from_secs(300);

    fn new_with_ttl() -> PreparedStatements {
        let mut ps = new_extended();
        ps.configure(PreparedStatementsConfig {
            ttl: Some(TTL),
            ..ps.config()
        });
        ps
    }

    pub(crate) fn prepare_expired(ps: &mut PreparedStatements, name: &str) {
        let config = ps.config();
        ps.configure(PreparedStatementsConfig {
            ttl: Some(Duration::ZERO),
            ttl_jitter: Duration::ZERO,
            ..config
        });
        ps.prepared(name);
        ps.configure(config);
    }

    macro_rules! assert_close_and_parse {
        ($result:expr, $name:expr) => {
            match $result {
                HandleResult::Prepend(prepare) => {
                    assert_eq!(
                        prepare.close(),
                        Some(&ProtocolMessage::Close(Close::named($name)))
                    );
                    assert!(matches!(prepare.parse(), ProtocolMessage::Parse(_)));
                }
                other => panic!("expected Prepend carrying a Close, got {other:?}"),
            }
        };
    }

    macro_rules! assert_parse_without_close {
        ($result:expr) => {
            match $result {
                HandleResult::Prepend(prepare) => {
                    assert_eq!(prepare.close(), None);
                    assert!(matches!(prepare.parse(), ProtocolMessage::Parse(_)));
                }
                other => panic!("expected Prepend without a Close, got {other:?}"),
            }
        };
    }

    fn bind(name: &str) -> ProtocolMessage {
        ProtocolMessage::Bind(Bind::new_statement(name))
    }

    #[test]
    fn bind_prepares_an_unknown_statement_without_a_close() {
        let name = insert_global("ttl_unknown", "SELECT $1::bigint");
        let mut ps = new_with_ttl();

        assert_parse_without_close!(ps.handle(&bind(&name)).unwrap());
    }

    #[test]
    fn bind_leaves_a_statement_within_its_ttl_alone() {
        let name = insert_global("ttl_fresh", "SELECT $1::bigint");
        let mut ps = new_with_ttl();
        ps.prepared(&name);

        assert_eq!(ps.handle(&bind(&name)).unwrap(), HandleResult::Forward);
        assert!(ps.contains(&name));
    }

    #[test]
    fn bind_closes_a_statement_past_its_ttl() {
        let name = insert_global("ttl_expired", "SELECT $1::bigint");
        let mut ps = new_with_ttl();
        prepare_expired(&mut ps, &name);

        assert_close_and_parse!(ps.handle(&bind(&name)).unwrap(), &name);
    }

    #[test]
    fn bind_closes_a_statement_prepared_before_the_ttl_was_set() {
        let name = insert_global("ttl_enabled_later", "SELECT $1::bigint");
        let mut ps = new_extended();
        ps.prepared(&name);

        assert_eq!(ps.config().ttl, None);
        assert!(ps.statement(&name).unwrap().expired(Instant::now()));

        ps.configure(PreparedStatementsConfig {
            ttl: Some(TTL),
            ..ps.config()
        });

        assert_close_and_parse!(ps.handle(&bind(&name)).unwrap(), &name);
    }

    #[test]
    fn bind_leaves_an_expired_statement_alone_when_the_ttl_is_disabled() {
        let name = insert_global("ttl_disabled", "SELECT $1::bigint");
        let mut ps = new_extended();
        prepare_expired(&mut ps, &name);

        assert_eq!(ps.config().ttl, None);
        assert!(ps.statement(&name).unwrap().expired(Instant::now()));

        assert_eq!(ps.handle(&bind(&name)).unwrap(), HandleResult::Forward);
        assert!(ps.contains(&name));
    }

    #[test]
    fn bind_leaves_an_expired_statement_alone_when_it_cannot_be_re_prepared() {
        let mut ps = new_with_ttl();
        prepare_expired(&mut ps, "not_in_global_cache");

        assert_eq!(
            ps.handle(&bind("not_in_global_cache")).unwrap(),
            HandleResult::Forward
        );
        assert!(ps.contains("not_in_global_cache"));
    }

    #[test]
    fn bind_closes_a_statement_past_its_ttl_only_once() {
        let name = insert_global("ttl_in_flight", "SELECT $1::bigint");
        let mut ps = new_with_ttl();
        prepare_expired(&mut ps, &name);

        assert_close_and_parse!(ps.handle(&bind(&name)).unwrap(), &name);
        assert_eq!(ps.handle(&bind(&name)).unwrap(), HandleResult::Forward);
    }

    #[test]
    fn describe_closes_a_statement_past_its_ttl() {
        let name = insert_global("ttl_describe", "SELECT $1::bigint");
        let mut ps = new_with_ttl();
        prepare_expired(&mut ps, &name);

        let describe = ProtocolMessage::Describe(Describe::new_statement(&name));
        assert_close_and_parse!(ps.handle(&describe).unwrap(), &name);
    }

    #[test]
    fn close_confirmed_then_failed_parse_leaves_no_stale_entry() {
        let name = insert_global("ttl_close_then_error", "SELECT $1::bigint");
        let mut ps = new_with_ttl();
        prepare_expired(&mut ps, &name);

        // Bind prepends Close + Parse for the expired statement.
        assert_close_and_parse!(ps.handle(&bind(&name)).unwrap(), &name);

        // Postgres closes the statement, then rejects the Parse. The client
        // never asked for the Close, so its reply stays with us.
        let mut close_complete = Message::new(CloseComplete.to_bytes());
        assert!(!ps.forward(&mut close_complete).unwrap());

        let mut error = Message::new(ErrorResponse::syntax("boom").to_bytes());
        assert!(ps.forward(&mut error).unwrap());

        // The name is gone from the server, so the cache must not claim it.
        assert!(!ps.contains(&name));
    }

    #[test]
    fn error_before_the_close_keeps_the_entry() {
        let name = insert_global("ttl_error_then_close", "SELECT $1::bigint");
        let mut ps = new_with_ttl();
        prepare_expired(&mut ps, &name);

        assert_close_and_parse!(ps.handle(&bind(&name)).unwrap(), &name);

        // An earlier message failed, so Postgres skipped our Close and still
        // holds the statement.
        let mut error = Message::new(ErrorResponse::syntax("boom").to_bytes());
        assert!(ps.forward(&mut error).unwrap());

        assert!(ps.contains(&name));

        // The next Bind must close it again before re-preparing.
        assert_close_and_parse!(ps.handle(&bind(&name)).unwrap(), &name);
    }

    #[test]
    fn a_portal_close_does_not_drop_a_pending_statement() {
        let name = insert_global("ttl_portal_close", "SELECT $1::bigint");
        let mut ps = new_with_ttl();
        prepare_expired(&mut ps, &name);

        // The client closes a portal, then binds the expired statement.
        let portal = ProtocolMessage::Close(Close::portal("p"));
        assert_eq!(ps.handle(&portal).unwrap(), HandleResult::Forward);
        assert_close_and_parse!(ps.handle(&bind(&name)).unwrap(), &name);

        // Postgres answers the portal Close, then a message between it and our
        // Close fails, so our Close never runs. The client asked for this
        // Close, so its reply goes back.
        let mut close_complete = Message::new(CloseComplete.to_bytes());
        assert!(ps.forward(&mut close_complete).unwrap());

        let mut error = Message::new(ErrorResponse::syntax("boom").to_bytes());
        assert!(ps.forward(&mut error).unwrap());

        // Postgres still holds the statement, so the cache must too.
        assert!(ps.contains(&name));
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
            HandleResult::Prepend(prepare) => {
                assert_eq!(prepare.close(), None);
                let ProtocolMessage::Parse(p) = prepare.parse() else {
                    panic!("expected prepend to be Parse");
                };
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
                if let ProtocolMessage::Parse(p) = prepend.parse() {
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
                if let ProtocolMessage::Parse(p) = prepend.parse() {
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
        let mut rfq = Message::new(ReadyForQuery::idle().to_bytes());
        ps.forward(&mut rfq).unwrap();

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
        let mut rfq = Message::new(ReadyForQuery::idle().to_bytes());
        ps.forward(&mut rfq).unwrap();

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

    #[test]
    fn parse_rewrites_if_oids_change() {
        let mut ps = new_extended();
        ps.oids = Oids::from_canonical([(10000, 10001)].into_iter().collect());

        let parse = Parse::named("stmt1", "SELECT $1, $2");
        let client_parse = parse.with_data_types(&[10000, 10002]);
        let result = ps.handle(&ProtocolMessage::Parse(client_parse)).unwrap();
        let expected = parse.with_data_types(&[10001, 10002]);
        assert_eq!(
            result,
            HandleResult::Rewrite(ProtocolMessage::Parse(expected))
        );
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
