//! ClientRequest (messages buffer).
//!
//! Contains exactly one request.
//!
use std::ops::{Deref, DerefMut};

use lazy_static::lazy_static;
use regex::Regex;

use crate::{
    frontend::router::Ast,
    net::{
        messages::{Bind, CopyData, Protocol},
        Error, Flush, Parse, ProtocolMessage,
    },
    stats::memory::MemoryUsage,
};

use super::{router::Route, PreparedStatements};

pub use super::BufferedQuery;

/// Client request, containing exactly one query.
#[derive(Debug, Clone)]
pub struct ClientRequest {
    /// Messages, e.g. Query, or Parse, Bind, Execute, etc.
    pub messages: Vec<ProtocolMessage>,
    /// The route this request will take in our query engine.
    /// When the request is created, this is not known yet.
    /// The QueryEngine will set the route once it handles the request.
    pub route: Option<Route>,
    /// The statement AST, if we parsed the request with our query parser.
    pub ast: Option<Ast>,
    /// Last Parse we received.
    pub last_parse: Option<Parse>,
}

impl MemoryUsage for ClientRequest {
    #[inline]
    fn memory_usage(&self) -> usize {
        // ProtocolMessage uses memory allocated by BytesMut (mostly).
        self.messages.capacity() * std::mem::size_of::<ProtocolMessage>()
            + std::mem::size_of::<Option<Ast>>()
    }
}

impl Default for ClientRequest {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientRequest {
    /// Create new buffer.
    fn new() -> Self {
        Self {
            messages: Vec::with_capacity(5),
            route: None,
            ast: None,
            last_parse: None,
        }
    }

    /// Add message to request.
    ///
    /// If message is a Parse, we save it in case
    /// we receive a Bind/Descrive, Sync later.
    ///
    pub(crate) fn push(&mut self, message: ProtocolMessage) {
        if let ProtocolMessage::Parse(ref parse) = message {
            if parse.anonymous() {
                self.last_parse = Some(parse.clone());
            } else {
                self.last_parse = None;
            }
        }
        self.messages.push(message);
    }

    /// Remove any saved state from the request.
    pub fn clear(&mut self) {
        // We drop `last_parse` once the client has executed it. The gate is
        // `is_executable` (Bind/Execute/Query present), not the presence of
        // Sync: lib/pq sends Parse, Describe, Sync to learn parameter/row
        // types without executing, and we need the saved Parse to survive
        // that round so it can be injected before the following
        // Bind/Execute/Sync.
        if self.is_executable() {
            self.last_parse = None;
        }

        self.messages.clear();
        self.route = None;
        self.ast = None;
    }

    /// We received a complete request and we are ready to
    /// send it to the query engine.
    pub fn is_complete(&self) -> bool {
        if let Some(message) = self.messages.last() {
            // Flush (F) | Sync (F) | Query (F) | CopyDone (F) | CopyFail (F)
            if matches!(message.code(), 'H' | 'S' | 'Q' | 'c' | 'f') {
                return true;
            }

            // CopyData (F)
            // Flush data to backend if we've buffered 4K.
            if message.code() == 'd' && self.total_message_len() >= 4096 {
                return true;
            }

            // Don't buffer streams.
            if message.streaming() {
                return true;
            }
        }

        false
    }

    /// Number of bytes in the buffer.
    pub fn total_message_len(&self) -> usize {
        self.messages.iter().map(|b| b.len()).sum()
    }

    /// If this buffer contains a query, retrieve it.
    pub fn query(&self) -> Result<Option<BufferedQuery>, Error> {
        for message in &self.messages {
            match message {
                ProtocolMessage::Query(query) => {
                    return Ok(Some(BufferedQuery::Query(query.clone())))
                }
                ProtocolMessage::Parse(parse) => {
                    return Ok(Some(BufferedQuery::Prepared(parse.clone())))
                }
                ProtocolMessage::Bind(bind) => {
                    if !bind.anonymous() {
                        return Ok(PreparedStatements::global()
                            .read()
                            .parse(bind.statement())
                            .map(BufferedQuery::Prepared));
                    } else if let Some(ref parse) = self.last_parse {
                        return Ok(Some(BufferedQuery::Prepared(parse.clone())));
                    }
                }
                ProtocolMessage::Describe(describe) => {
                    if !describe.anonymous() {
                        return Ok(PreparedStatements::global()
                            .read()
                            .parse(describe.statement())
                            .map(BufferedQuery::Prepared));
                    } else if let Some(ref parse) = self.last_parse {
                        return Ok(Some(BufferedQuery::Prepared(parse.clone())));
                    }
                }
                _ => (),
            }
        }

        Ok(None)
    }

    /// Quick and cheap way to find out if this
    /// request is starting a transaction.
    pub(crate) fn is_begin(&self) -> bool {
        lazy_static! {
            static ref BEGIN: Regex = Regex::new("(?i)^BEGIN").unwrap();
        }

        for message in &self.messages {
            let query = match message {
                ProtocolMessage::Parse(parse) => parse.query(),
                ProtocolMessage::Query(query) => query.query(),
                _ => continue,
            };

            if BEGIN.is_match(query) {
                return true;
            }
        }

        false
    }

    /// If this buffer contains bound parameters, retrieve them.
    pub fn parameters(&self) -> Result<Option<&Bind>, Error> {
        for message in &self.messages {
            if let ProtocolMessage::Bind(bind) = message {
                return Ok(Some(bind));
            }
        }

        Ok(None)
    }

    /// Get all CopyData messages.
    pub fn copy_data(&self) -> Result<Vec<CopyData>, Error> {
        let mut rows = vec![];
        for message in &self.messages {
            if let ProtocolMessage::CopyData(copy_data) = message {
                rows.push(copy_data.clone())
            }
        }

        Ok(rows)
    }

    /// Remove all CopyData messages and return the rest.
    pub fn without_copy_data(&self) -> Self {
        let mut messages = self.messages.clone();
        messages.retain(|m| m.code() != 'd');

        Self {
            messages,
            route: self.route.clone(),
            ast: self.ast.clone(),
            last_parse: None,
        }
    }

    /// The buffer has COPY messages.
    pub fn is_copy(&self) -> bool {
        self.messages
            .last()
            .map(|m| m.code() == 'd' || m.code() == 'c')
            .unwrap_or(false)
    }

    /// The buffer contains only Sync (and possibly Flush) messages.
    /// Used to avoid resetting multi-shard state when Sync is sent
    /// as a separate request (via splice).
    pub fn is_sync_only(&self) -> bool {
        !self.messages.is_empty()
            && self
                .messages
                .iter()
                .all(|m| m.code() == 'S' || m.code() == 'H')
    }

    /// The client is setting state on the connection
    /// which we can no longer ignore.
    pub(crate) fn is_executable(&self) -> bool {
        self.messages
            .iter()
            .any(|m| ['E', 'Q', 'B'].contains(&m.code()))
    }

    /// We split up the extended protocol exhange as soon as we see
    /// a Flush message. This means we can handle this:
    ///
    /// 1. Parse, Describe, Flush
    /// 2. Bind, Execute, Sync
    ///
    /// without breaking the state by injecting the last Parse we saw into
    /// the second request and ignoring ParseComplete from the server.
    ///
    /// Returns true only when every Bind/Describe in the batch is anonymous
    /// and the batch has no Parse of its own. A single named Bind or Describe
    /// disqualifies the whole batch: we can only inject one Parse, and it
    /// would target the wrong shard for the named statement (which routes
    /// independently). Pipelined batches that mix anonymous and named
    /// statements are not supported — drivers we care about don't generate
    /// them.
    pub(crate) fn needs_parse_injection(&self) -> bool {
        let mut references_anonymous = false;
        for message in &self.messages {
            match message {
                ProtocolMessage::Parse(_) => return false,
                ProtocolMessage::Bind(bind) => {
                    if !bind.anonymous() {
                        return false;
                    }
                    references_anonymous = true;
                }
                ProtocolMessage::Describe(describe) => {
                    if !describe.anonymous() {
                        return false;
                    }
                    references_anonymous = true;
                }
                _ => {}
            }
        }
        references_anonymous
    }

    /// Rewrite query in buffer.
    pub fn rewrite(&mut self, request: &[ProtocolMessage]) -> Result<(), Error> {
        if self.messages.iter().any(|c| c.code() != 'Q') {
            return Err(Error::OnlySimpleForRewrites);
        }
        self.messages.clear();
        self.messages.extend(request.to_vec());
        Ok(())
    }

    /// Get the route for this client request.
    pub fn route(&self) -> &Route {
        lazy_static! {
            static ref DEFAULT_ROUTE: Route = Route::default();
        }
        self.route.as_ref().unwrap_or(&DEFAULT_ROUTE)
    }

    /// Split request into multiple serviceable requests by the query engine.
    pub fn spliced(&self) -> Result<Vec<Self>, Error> {
        // Splice iff using extended protocol and it executes
        // more than one statement.
        let req_count = self.messages.iter().filter(|m| m.code() == 'E').count();
        if req_count <= 1 {
            return Ok(vec![]);
        }

        let mut requests: Vec<Self> = vec![];
        let mut current_request = Self::new();

        for message in &self.messages {
            let code = message.code();
            match code {
                // Parse, Bind, Describe, Close typically refer
                // to the same statement.
                //
                // TODO: they don't actually have to.
                'P' | 'B' | 'D' | 'C' => {
                    current_request.push(message.clone());
                }

                // Flush typically indicates the end of the request.
                // We use it for request separation so we're only adding
                // it if we haven't already. We also don't want to send requests
                // that contain Flush only since they will get stuck.
                'H' => {
                    if let Some(last_message) = current_request.last() {
                        if last_message.code() != 'H' {
                            current_request.push(message.clone());
                        }
                    }
                }

                // Execute is the boundary between requests. Each request
                // can go to different shard, hence the splice.
                'E' => {
                    current_request.messages.push(message.clone());
                    current_request.messages.push(Flush.into());
                    requests.push(std::mem::take(&mut current_request));
                }

                // Sync is always in its own request. This ensures
                // we can handle ReadyForQuery separately from query results.
                'S' => {
                    // Push any accumulated messages first
                    if !current_request.is_empty() {
                        requests.push(std::mem::take(&mut current_request));
                    }
                    // Sync goes in its own request
                    current_request.messages.push(message.clone());
                    requests.push(std::mem::take(&mut current_request));
                }

                c => return Err(Error::UnexpectedMessage('S', c)),
            }
        }

        // Collect any remaining messages that aren't followed
        // by Flush or Sync.
        if !current_request.is_empty() {
            requests.push(current_request);
        }

        Ok(requests)
    }
}

impl From<ClientRequest> for Vec<ProtocolMessage> {
    fn from(val: ClientRequest) -> Self {
        val.messages
    }
}

impl From<Vec<ProtocolMessage>> for ClientRequest {
    fn from(messages: Vec<ProtocolMessage>) -> Self {
        ClientRequest {
            messages,
            route: None,
            ast: None,
            last_parse: None,
        }
    }
}

impl Deref for ClientRequest {
    type Target = Vec<ProtocolMessage>;

    fn deref(&self) -> &Self::Target {
        &self.messages
    }
}

impl DerefMut for ClientRequest {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.messages
    }
}

#[cfg(test)]
mod test {
    use crate::net::{Describe, Execute, Parse, Query, Sync};

    use super::*;

    #[test]
    fn test_request_splice() {
        let messages = vec![
            ProtocolMessage::from(Parse::named("start", "BEGIN")),
            Bind::new_statement("start").into(),
            Execute::new().into(),
            Parse::named("test", "SELECT $1").into(),
            Bind::new_statement("test").into(),
            Execute::new().into(),
            Describe::new_statement("test").into(),
            Sync::new().into(),
        ];
        let req = ClientRequest::from(messages);
        let splice = req.spliced().unwrap();
        assert_eq!(splice.len(), 4);

        // First slice should contain: Parse("start"), Bind("start"), Execute, Flush
        let first_slice = &splice[0];
        assert_eq!(first_slice.len(), 4);
        assert_eq!(first_slice[0].code(), 'P'); // Parse
        assert_eq!(first_slice[1].code(), 'B'); // Bind
        assert_eq!(first_slice[2].code(), 'E'); // Execute
        assert_eq!(first_slice[3].code(), 'H'); // Flush
        if let ProtocolMessage::Parse(parse) = &first_slice[0] {
            assert_eq!(parse.name(), "start");
            assert_eq!(parse.query(), "BEGIN");
        } else {
            panic!("Expected Parse message");
        }
        if let ProtocolMessage::Bind(bind) = &first_slice[1] {
            assert_eq!(bind.statement(), "start");
        } else {
            panic!("Expected Bind message");
        }

        // Second slice should contain: Parse("test"), Bind("test"), Execute, Flush
        let second_slice = &splice[1];
        assert_eq!(second_slice.len(), 4);
        assert_eq!(second_slice[0].code(), 'P'); // Parse
        assert_eq!(second_slice[1].code(), 'B'); // Bind
        assert_eq!(second_slice[2].code(), 'E'); // Execute
        assert_eq!(second_slice[3].code(), 'H'); // Flush
        if let ProtocolMessage::Parse(parse) = &second_slice[0] {
            assert_eq!(parse.name(), "test");
            assert_eq!(parse.query(), "SELECT $1");
        } else {
            panic!("Expected Parse message");
        }
        if let ProtocolMessage::Bind(bind) = &second_slice[1] {
            assert_eq!(bind.statement(), "test");
        } else {
            panic!("Expected Bind message");
        }

        // Third slice should contain: Describe("test")
        let third_slice = &splice[2];
        assert_eq!(third_slice.len(), 1);
        assert_eq!(third_slice[0].code(), 'D'); // Describe

        // Fourth slice should contain: Sync (always separate)
        let fourth_slice = &splice[3];
        assert_eq!(fourth_slice.len(), 1);
        assert_eq!(fourth_slice[0].code(), 'S'); // Sync

        let messages = vec![
            ProtocolMessage::from(Parse::named("test", "SELECT $1")),
            Bind::new_statement("test").into(),
            Execute::new().into(),
            Sync.into(),
        ];
        let req = ClientRequest::from(messages);
        let splice = req.spliced().unwrap();
        assert!(splice.is_empty());

        let messages = vec![
            ProtocolMessage::from(Parse::named("test", "SELECT 1")),
            Bind::new_statement("test").into(),
            Execute::new().into(),
            ProtocolMessage::from(Parse::named("test_1", "SELECT 2")),
            Bind::new_statement("test_1").into(),
            Execute::new().into(),
            Flush.into(),
        ];
        let req = ClientRequest::from(messages);
        let splice = req.spliced().unwrap();
        assert_eq!(splice.len(), 2);

        // First slice: Parse("test"), Bind("test"), Execute, Flush
        let first_slice = &splice[0];
        assert_eq!(first_slice.len(), 4);
        assert_eq!(first_slice[0].code(), 'P'); // Parse
        assert_eq!(first_slice[1].code(), 'B'); // Bind
        assert_eq!(first_slice[2].code(), 'E'); // Execute
        assert_eq!(first_slice[3].code(), 'H'); // Flush
        if let ProtocolMessage::Parse(parse) = &first_slice[0] {
            assert_eq!(parse.name(), "test");
            assert_eq!(parse.query(), "SELECT 1");
        } else {
            panic!("Expected Parse message");
        }
        if let ProtocolMessage::Bind(bind) = &first_slice[1] {
            assert_eq!(bind.statement(), "test");
        } else {
            panic!("Expected Bind message");
        }

        // Second slice: Parse("test_1"), Bind("test_1"), Execute, Flush
        let second_slice = &splice[1];
        assert_eq!(second_slice.len(), 4);
        assert_eq!(second_slice[0].code(), 'P'); // Parse
        assert_eq!(second_slice[1].code(), 'B'); // Bind
        assert_eq!(second_slice[2].code(), 'E'); // Execute
        assert_eq!(second_slice[3].code(), 'H'); // Flush
        if let ProtocolMessage::Parse(parse) = &second_slice[0] {
            assert_eq!(parse.name(), "test_1");
            assert_eq!(parse.query(), "SELECT 2");
        } else {
            panic!("Expected Parse message");
        }
        if let ProtocolMessage::Bind(bind) = &second_slice[1] {
            assert_eq!(bind.statement(), "test_1");
        } else {
            panic!("Expected Bind message");
        }

        assert_eq!(splice.first().unwrap().messages.last().unwrap().code(), 'H');
        assert_eq!(
            splice
                .iter()
                .map(|s| s.messages.iter().filter(|p| p.code() == 'H').count())
                .sum::<usize>(),
            2
        );

        // Test Parse, Describe, Flush, Bind, Execute, Bind, Execute, Sync sequence
        let messages = vec![
            Parse::named("stmt", "SELECT $1").into(),
            Describe::new_statement("stmt").into(),
            Flush.into(),
            Bind::new_statement("stmt").into(),
            Execute::new().into(),
            Bind::new_statement("stmt").into(),
            Execute::new().into(),
            Sync::new().into(),
        ];
        let req = ClientRequest::from(messages);
        let splice = req.spliced().unwrap();
        assert_eq!(splice.len(), 3);

        // First slice should contain: Parse("stmt"), Describe("stmt"), Flush, Bind("stmt"), Execute, Flush
        let first_slice = &splice[0];
        assert_eq!(first_slice.len(), 6);
        assert_eq!(first_slice[0].code(), 'P'); // Parse
        assert_eq!(first_slice[1].code(), 'D'); // Describe
        assert_eq!(first_slice[2].code(), 'H'); // Flush (should be the original Flush)
        assert_eq!(first_slice[3].code(), 'B'); // Bind
        assert_eq!(first_slice[4].code(), 'E'); // Execute
        assert_eq!(first_slice[5].code(), 'H'); // Flush (added by splice logic)

        // Second slice should contain: Bind("stmt"), Execute, Flush
        let second_slice = &splice[1];
        assert_eq!(second_slice.len(), 3);
        assert_eq!(second_slice[0].code(), 'B'); // Bind
        assert_eq!(second_slice[1].code(), 'E'); // Execute
        assert_eq!(second_slice[2].code(), 'H'); // Flush

        // Third slice should contain: Sync (always separate)
        let third_slice = &splice[2];
        assert_eq!(third_slice.len(), 1);
        assert_eq!(third_slice[0].code(), 'S'); // Sync
    }

    #[test]
    fn test_detect_begin() {
        for query in [
            ProtocolMessage::Query(Query::new("begin")),
            ProtocolMessage::Query(Query::new("BEGIN WORK REPEATABLE READ")),
            ProtocolMessage::Parse(Parse::new_anonymous("BEGIN")),
        ] {
            let req = ClientRequest::from(vec![query]);
            assert!(req.is_begin());
        }
    }

    #[test]
    fn test_is_complete() {
        // Sync marks request complete
        let req = ClientRequest::from(vec![
            Parse::named("test", "SELECT 1").into(),
            Bind::new_statement("test").into(),
            Execute::new().into(),
            Sync::new().into(),
        ]);
        assert!(req.is_complete());

        // Flush marks request complete
        let req = ClientRequest::from(vec![
            Parse::named("test", "SELECT 1").into(),
            Bind::new_statement("test").into(),
            Execute::new().into(),
            Flush.into(),
        ]);
        assert!(req.is_complete());

        // Simple Query marks request complete
        let req = ClientRequest::from(vec![Query::new("SELECT 1").into()]);
        assert!(req.is_complete());

        // Parse only - NOT complete
        let req = ClientRequest::from(vec![Parse::named("test", "SELECT 1").into()]);
        assert!(!req.is_complete());

        // Parse, Bind, Execute without Sync/Flush - NOT complete
        let req = ClientRequest::from(vec![
            Parse::named("test", "SELECT 1").into(),
            Bind::new_statement("test").into(),
            Execute::new().into(),
        ]);
        assert!(!req.is_complete());

        // Parse, Describe without Flush/Sync - NOT complete
        let req = ClientRequest::from(vec![
            Parse::named("test", "SELECT 1").into(),
            Describe::new_statement("test").into(),
        ]);
        assert!(!req.is_complete());
    }

    #[test]
    fn push_anonymous_parse_saves_last_parse() {
        let mut req = ClientRequest::new();
        req.push(Parse::new_anonymous("SELECT $1").into());
        let saved = req.last_parse.as_ref().expect("last_parse should be set");
        assert_eq!(saved.query(), "SELECT $1");
        assert!(saved.anonymous());
    }

    #[test]
    fn push_named_parse_clears_last_parse() {
        // A prior anonymous Parse must be dropped when a named Parse
        // arrives — otherwise we'd inject a stale anonymous Parse on
        // a subsequent Bind referring to the named statement.
        let mut req = ClientRequest::new();
        req.push(Parse::new_anonymous("SELECT $1").into());
        assert!(req.last_parse.is_some());
        req.push(Parse::named("foo", "SELECT 2").into());
        assert!(req.last_parse.is_none());
    }

    #[test]
    fn push_non_parse_message_does_not_touch_last_parse() {
        // last_parse is only set/cleared by Parse messages.
        let mut req = ClientRequest::new();
        req.push(Parse::new_anonymous("SELECT $1").into());
        req.push(Describe::new_statement("").into());
        req.push(Flush.into());
        assert!(req.last_parse.is_some());
    }

    #[test]
    fn clear_after_flush_preserves_last_parse() {
        // The split-extended-protocol case: Parse, Describe, Flush.
        // After clearing the buffer for the next round, we still
        // need last_parse so the saved Parse can be injected before
        // the upcoming Bind/Execute/Sync.
        let mut req = ClientRequest::new();
        req.push(Parse::new_anonymous("SELECT $1").into());
        req.push(Describe::new_statement("").into());
        req.push(Flush.into());
        req.clear();
        assert!(req.last_parse.is_some());
        assert!(req.messages.is_empty());
    }

    #[test]
    fn clear_after_sync_drops_last_parse() {
        // Sync ends the extended-protocol exchange. The anonymous
        // statement is gone from the server's perspective, so we
        // must drop last_parse too.
        let mut req = ClientRequest::new();
        req.push(Parse::new_anonymous("SELECT $1").into());
        req.push(Bind::new_statement("").into());
        req.push(Execute::new().into());
        req.push(Sync::new().into());
        req.clear();
        assert!(req.last_parse.is_none());
        assert!(req.messages.is_empty());
    }

    #[test]
    fn clear_empty_request_does_not_drop_last_parse() {
        // contains_sync() is false on an empty buffer, so a stray
        // clear() between rounds must not wipe last_parse.
        let mut req = ClientRequest::new();
        req.push(Parse::new_anonymous("SELECT $1").into());
        req.messages.clear(); // simulate consumption without going through clear()
                              // Re-establish: only the saved last_parse remains.
        assert!(req.last_parse.is_some());
        req.clear();
        assert!(req.last_parse.is_some());
    }

    #[test]
    fn needs_parse_injection_anonymous_bind_triggers() {
        // The classic split-extended-protocol case: prior request was
        // Parse, Describe, Flush; this is the follow-up.
        let req = ClientRequest::from(vec![
            Bind::new_statement("").into(),
            Execute::new().into(),
            Sync::new().into(),
        ]);
        assert!(req.needs_parse_injection());
    }

    #[test]
    fn needs_parse_injection_anonymous_describe_triggers() {
        // Standalone Describe of the anonymous statement also needs
        // the saved Parse injected.
        let req = ClientRequest::from(vec![Describe::new_statement("").into(), Flush.into()]);
        assert!(req.needs_parse_injection());
    }

    #[test]
    fn needs_parse_injection_named_bind_disqualifies() {
        // Named Bind refers to a server-side prepared statement —
        // injecting an anonymous Parse would be wrong.
        let req = ClientRequest::from(vec![
            Bind::new_statement("named_stmt").into(),
            Execute::new().into(),
            Sync::new().into(),
        ]);
        assert!(!req.needs_parse_injection());
    }

    #[test]
    fn needs_parse_injection_named_describe_disqualifies() {
        let req = ClientRequest::from(vec![
            Describe::new_statement("named_stmt").into(),
            Flush.into(),
        ]);
        assert!(!req.needs_parse_injection());
    }

    #[test]
    fn needs_parse_injection_parse_in_buffer_disqualifies() {
        // The buffer carries its own Parse — no injection needed.
        let req = ClientRequest::from(vec![
            Parse::new_anonymous("SELECT $1").into(),
            Bind::new_statement("").into(),
            Execute::new().into(),
            Sync::new().into(),
        ]);
        assert!(!req.needs_parse_injection());
    }

    #[test]
    fn needs_parse_injection_named_parse_in_buffer_disqualifies() {
        let req = ClientRequest::from(vec![
            Parse::named("foo", "SELECT $1").into(),
            Bind::new_statement("foo").into(),
            Execute::new().into(),
            Sync::new().into(),
        ]);
        assert!(!req.needs_parse_injection());
    }

    #[test]
    fn needs_parse_injection_execute_sync_only_does_not_trigger() {
        // Execute and Sync alone don't reference any statement, so
        // there's nothing for the saved Parse to apply to.
        let req = ClientRequest::from(vec![Execute::new().into(), Sync::new().into()]);
        assert!(!req.needs_parse_injection());
    }

    #[test]
    fn needs_parse_injection_sync_only_does_not_trigger() {
        let req = ClientRequest::from(vec![Sync::new().into()]);
        assert!(!req.needs_parse_injection());
    }

    #[test]
    fn needs_parse_injection_empty_request_does_not_trigger() {
        let req = ClientRequest::from(vec![]);
        assert!(!req.needs_parse_injection());
    }

    #[test]
    fn needs_parse_injection_mixed_anonymous_and_named_bind_disqualifies() {
        // One named Bind in the batch poisons the whole request:
        // injecting an anonymous Parse would clobber state for the
        // named one.
        let req = ClientRequest::from(vec![
            Bind::new_statement("").into(),
            Execute::new().into(),
            Bind::new_statement("named_stmt").into(),
            Execute::new().into(),
            Sync::new().into(),
        ]);
        assert!(!req.needs_parse_injection());
    }

    #[test]
    fn needs_parse_injection_anonymous_bind_and_describe_triggers() {
        // Both anonymous — still needs injection.
        let req = ClientRequest::from(vec![
            Bind::new_statement("").into(),
            Describe::new_statement("").into(),
            Execute::new().into(),
            Sync::new().into(),
        ]);
        assert!(req.needs_parse_injection());
    }

    #[test]
    fn needs_parse_injection_anonymous_bind_with_named_describe_disqualifies() {
        let req = ClientRequest::from(vec![
            Bind::new_statement("").into(),
            Describe::new_statement("named_stmt").into(),
            Execute::new().into(),
            Sync::new().into(),
        ]);
        assert!(!req.needs_parse_injection());
    }

    #[test]
    fn needs_parse_injection_flush_only_does_not_trigger() {
        // Flush alone is meaningless — no statement references.
        let req = ClientRequest::from(vec![Flush.into()]);
        assert!(!req.needs_parse_injection());
    }
}
