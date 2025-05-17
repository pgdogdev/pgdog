//! Message buffer.

use std::ops::{Deref, DerefMut};

use crate::{
    backend::ProtocolMessage,
    net::{
        messages::{parse::Parse, Bind, CopyData, Protocol, Query},
        Error,
    },
};

use super::PreparedStatements;

/// Message buffer.
#[derive(Debug, Clone)]
pub struct Buffer {
    buffer: Vec<ProtocolMessage>,
}

impl Default for Buffer {
    fn default() -> Self {
        Self::new()
    }
}

impl Buffer {
    /// Create new buffer.
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(5),
        }
    }

    /// Client likely wants to communicate asynchronously.
    pub fn is_async(&self) -> bool {
        self.buffer.last().map(|m| m.code() == 'H').unwrap_or(false)
    }

    /// The buffer is full and the client won't send any more messages
    /// until it gets a reply, or we don't want to buffer the data in memory.
    pub fn full(&self) -> bool {
        if let Some(message) = self.buffer.last() {
            // Flush (F) | Sync (F) | Query (F) | CopyDone (F) | CopyFail (F)
            if matches!(message.code(), 'H' | 'S' | 'Q' | 'c' | 'f') {
                return true;
            }

            // CopyData (F)
            // Flush data to backend if we've buffered 4K.
            if message.code() == 'd' && self.len() >= 4096 {
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
    pub fn len(&self) -> usize {
        self.buffer.iter().map(|b| b.len()).sum()
    }

    /// Check if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// If this buffer contains a query, retrieve it.
    pub fn query(&self) -> Result<Option<BufferedQuery>, Error> {
        for message in &self.buffer {
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
        for message in &self.buffer {
            if let ProtocolMessage::Bind(bind) = message {
                return Ok(Some(bind));
            }
        }

        Ok(None)
    }

    /// Get all CopyData messages.
    pub fn copy_data(&self) -> Result<Vec<CopyData>, Error> {
        let mut rows = vec![];
        for message in &self.buffer {
            if let ProtocolMessage::CopyData(copy_data) = message {
                rows.push(copy_data.clone())
            }
        }

        Ok(rows)
    }

    /// Remove all CopyData messages and return the rest.
    pub fn without_copy_data(&self) -> Self {
        let mut buffer = self.buffer.clone();
        buffer.retain(|m| m.code() != 'd');
        Self { buffer }
    }

    /// The buffer has COPY messages.
    pub fn copy(&self) -> bool {
        self.buffer
            .last()
            .map(|m| m.code() == 'd' || m.code() == 'c')
            .unwrap_or(false)
    }

    /// The client is expecting a reply now.
    pub fn flush(&self) -> bool {
        self.buffer.last().map(|m| m.code() == 'H').unwrap_or(false)
    }

    /// The client is setting state on the connection
    /// which we can no longer ignore.
    pub(crate) fn executable(&self) -> bool {
        self.buffer
            .iter()
            .any(|m| ['E', 'Q', 'B'].contains(&m.code()))
    }

    /// Client told us the copy failed.
    pub fn copy_fail(&self) -> bool {
        self.buffer.last().map(|m| m.code() == 'f').unwrap_or(false)
    }

    /// Rewrite query in buffer.
    pub fn rewrite(&mut self, query: &str) -> Result<(), Error> {
        if self.buffer.iter().any(|c| c.code() != 'Q') {
            return Err(Error::OnlySimpleForRewrites);
        }
        self.buffer.clear();
        self.buffer.push(Query::new(query).into());
        Ok(())
    }
}

impl From<Buffer> for Vec<ProtocolMessage> {
    fn from(val: Buffer) -> Self {
        val.buffer
    }
}

impl From<Vec<ProtocolMessage>> for Buffer {
    fn from(value: Vec<ProtocolMessage>) -> Self {
        Buffer { buffer: value }
    }
}

impl Deref for Buffer {
    type Target = Vec<ProtocolMessage>;

    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}

impl DerefMut for Buffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buffer
    }
}

#[derive(Debug, Clone)]
pub enum BufferedQuery {
    Query(Query),
    Prepared(Parse),
}

impl BufferedQuery {
    pub fn query(&self) -> &str {
        match self {
            Self::Query(query) => query.query(),
            Self::Prepared(parse) => parse.query(),
        }
    }

    pub fn extended(&self) -> bool {
        matches!(self, Self::Prepared(_))
    }

    pub fn simple(&self) -> bool {
        matches!(self, Self::Query(_))
    }
}

impl Deref for BufferedQuery {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.query()
    }
}
