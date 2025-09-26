//! ClientRequest (messages buffer).
use std::ops::{Deref, DerefMut};

use lazy_static::lazy_static;

use crate::{
    frontend::router::parser::RewritePlan,
    net::{
        messages::{Bind, CopyData, Protocol, Query},
        Error, Flush, ProtocolMessage,
    },
    stats::memory::MemoryUsage,
};

use super::{router::Route, PreparedStatements};

pub use super::BufferedQuery;

/// Message buffer.
#[derive(Debug, Clone)]
pub struct ClientRequest {
    pub messages: Vec<ProtocolMessage>,
    pub route: Option<Route>,
}

impl MemoryUsage for ClientRequest {
    #[inline]
    fn memory_usage(&self) -> usize {
        // ProtocolMessage uses memory allocated by BytesMut (mostly).
        self.messages.capacity() * std::mem::size_of::<ProtocolMessage>()
    }
}

impl Default for ClientRequest {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientRequest {
    /// Create new buffer.
    pub fn new() -> Self {
        Self {
            messages: Vec::with_capacity(5),
            route: None,
        }
    }

    /// The buffer is full and the client won't send any more messages
    /// until it gets a reply, or we don't want to buffer the data in memory.
    pub fn full(&self) -> bool {
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
                    }
                }
                ProtocolMessage::Describe(describe) => {
                    if !describe.anonymous() {
                        return Ok(PreparedStatements::global()
                            .read()
                            .parse(describe.statement())
                            .map(BufferedQuery::Prepared));
                    }
                }
                _ => (),
            }
        }

        Ok(None)
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
        }
    }

    /// The buffer has COPY messages.
    pub fn copy(&self) -> bool {
        self.messages
            .last()
            .map(|m| m.code() == 'd' || m.code() == 'c')
            .unwrap_or(false)
    }

    /// The client is setting state on the connection
    /// which we can no longer ignore.
    pub(crate) fn executable(&self) -> bool {
        self.messages
            .iter()
            .any(|m| ['E', 'Q', 'B'].contains(&m.code()))
    }

    /// Rewrite query in buffer.
    pub fn rewrite(&mut self, query: &str) -> Result<(), Error> {
        if self.messages.iter().any(|c| c.code() != 'Q') {
            return Err(Error::OnlySimpleForRewrites);
        }
        self.messages.clear();
        self.messages.push(Query::new(query).into());
        Ok(())
    }

    /// Rewrite prepared statement SQL before sending it to the backend.
    pub fn rewrite_prepared(
        &mut self,
        query: &str,
        prepared: &mut PreparedStatements,
        plan: &RewritePlan,
    ) -> bool {
        let mut updated = false;

        for message in self.messages.iter_mut() {
            if let ProtocolMessage::Parse(parse) = message {
                parse.set_query(query);
                let name = parse.name().to_owned();
                let _ = prepared.update_query(&name, query);
                if !plan.is_noop() {
                    prepared.set_rewrite_plan(&name, plan.clone());
                }
                updated = true;
            }
        }

        updated
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

                // Sync typically is last. We place it with the last request
                // to save on round trips.
                'S' => {
                    current_request.messages.push(message.clone());
                    if current_request.len() == 1 {
                        if let Some(last_request) = requests.last_mut() {
                            last_request.extend(current_request.drain(..));
                        }
                    }
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
    use crate::net::{Describe, Execute, Parse, Sync};

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
        assert_eq!(splice.len(), 3);

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

        // Third slice should contain: Describe("test"), Sync
        let third_slice = &splice[2];
        assert_eq!(third_slice.len(), 2);
        assert_eq!(third_slice[0].code(), 'D'); // Describe
        assert_eq!(third_slice[1].code(), 'S'); // Sync

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

        assert_eq!(splice.get(0).unwrap().messages.last().unwrap().code(), 'H');
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
        assert_eq!(splice.len(), 2);

        // First slice should contain: Parse("stmt"), Describe("stmt"), Flush, Bind("stmt"), Execute, Flush
        let first_slice = &splice[0];
        assert_eq!(first_slice.len(), 6);
        assert_eq!(first_slice[0].code(), 'P'); // Parse
        assert_eq!(first_slice[1].code(), 'D'); // Describe
        assert_eq!(first_slice[2].code(), 'H'); // Flush (should be the original Flush)
        assert_eq!(first_slice[3].code(), 'B'); // Bind
        assert_eq!(first_slice[4].code(), 'E'); // Execute
        assert_eq!(first_slice[5].code(), 'H'); // Flush (added by splice logic)

        // Second slice should contain: Bind("stmt"), Execute, Flush, Sync
        let second_slice = &splice[1];
        assert_eq!(second_slice.len(), 4);
        assert_eq!(second_slice[0].code(), 'B'); // Bind
        assert_eq!(second_slice[1].code(), 'E'); // Execute
        assert_eq!(second_slice[2].code(), 'H'); // Flush
        assert_eq!(second_slice[3].code(), 'S'); // Sync
    }
}
