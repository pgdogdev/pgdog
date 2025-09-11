//! ClientRequest (messages buffer).
use lazy_static::lazy_static;

use crate::{
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
                            .lock()
                            .parse(bind.statement())
                            .map(BufferedQuery::Prepared));
                    }
                }
                ProtocolMessage::Describe(describe) => {
                    if !describe.anonymous() {
                        return Ok(PreparedStatements::global()
                            .lock()
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

    /// Get the route for this client request.
    pub fn route(&self) -> &Route {
        lazy_static! {
            static ref DEFAULT_ROUTE: Route = Route::default();
        }
        self.route.as_ref().unwrap_or(&DEFAULT_ROUTE)
    }

    /// Split request into multiple serviceable requests by the query engine.
    pub fn splice(&self) -> Result<Vec<Self>, Error> {
        let execs = self.messages.iter().filter(|m| m.code() == 'E').count();
        if execs <= 1 {
            return Ok(vec![]);
        }
        let mut requests: Vec<Self> = vec![];
        let mut req = Self::new();

        for message in &self.messages {
            let code = message.code();
            match code {
                'P' | 'B' | 'D' | 'C' | 'H' => {
                    req.messages.push(message.clone());
                }

                'E' | 'S' => {
                    if code == 'S' {
                        if req.messages.is_empty() {
                            if let Some(last) = requests.last_mut() {
                                last.messages.push(message.clone());
                            } else {
                                req.messages.push(message.clone());
                            }
                        } else {
                            req.messages.push(message.clone());
                        }
                    } else {
                        req.messages.push(message.clone());
                        req.messages.push(Flush.into());
                    }

                    if !req.messages.is_empty() {
                        requests.push(req);
                    }

                    req = Self::new();
                }

                c => return Err(Error::UnexpectedMessage(c, 'S')),
            }
        }

        if !req.messages.is_empty() {
            requests.push(req);
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
        let splice = req.splice().unwrap();
        assert_eq!(splice.len(), 3);

        let messages = vec![
            ProtocolMessage::from(Parse::named("test", "SELECT $1")),
            Bind::new_statement("test").into(),
            Execute::new().into(),
            Sync.into(),
        ];
        let req = ClientRequest::from(messages);
        let splice = req.splice().unwrap();
        assert!(splice.is_empty());

        let messages = vec![
            ProtocolMessage::from(Parse::named("test", "SELECT 1")),
            Flush.into(),
        ];
        let req = ClientRequest::from(messages);
        let splice = req.splice().unwrap();
        assert!(splice.is_empty());
    }
}
