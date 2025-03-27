//! Message buffer.

use std::ops::{Deref, DerefMut};

use crate::net::{
    messages::{
        parse::Parse, Bind, CopyData, Describe, FromBytes, Message, Protocol, Query, ToBytes,
    },
    Error,
};

use super::PreparedStatements;

/// Message buffer.
#[derive(Debug, Clone)]
pub struct Buffer {
    buffer: Vec<Message>,
}

impl Default for Buffer {
    fn default() -> Self {
        Self::new()
    }
}

impl Buffer {
    /// Create new buffer.
    pub fn new() -> Self {
        Self { buffer: vec![] }
    }

    /// Client likely wants to communicate asynchronously.
    pub fn async_(&self) -> bool {
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
            match message.code() {
                'Q' => {
                    let query = Query::from_bytes(message.to_bytes()?)?;
                    return Ok(Some(BufferedQuery::Query(query)));
                }

                'P' => {
                    let parse = Parse::from_bytes(message.to_bytes()?)?;
                    return Ok(Some(BufferedQuery::Prepared(parse)));
                }

                'B' => {
                    let bind = Bind::from_bytes(message.to_bytes()?)?;
                    if !bind.anonymous() {
                        return Ok(PreparedStatements::global()
                            .lock()
                            .parse(&bind.statement)
                            .map(BufferedQuery::Prepared));
                    }
                }

                'D' => {
                    let describe = Describe::from_bytes(message.to_bytes()?)?;
                    if !describe.anonymous() {
                        return Ok(PreparedStatements::global()
                            .lock()
                            .parse(&describe.statement)
                            .map(BufferedQuery::Prepared));
                    }
                }

                _ => (),
            };
        }

        Ok(None)
    }

    /// If this buffer contains bound parameters, retrieve them.
    pub fn parameters(&self) -> Result<Option<Bind>, Error> {
        for message in &self.buffer {
            if message.code() == 'B' {
                let bind = Bind::from_bytes(message.to_bytes()?)?;
                return Ok(Some(bind));
            }
        }

        Ok(None)
    }

    /// Get all CopyData (F & B) messages.
    pub fn copy_data(&self) -> Result<Vec<CopyData>, Error> {
        let mut rows = vec![];
        for message in &self.buffer {
            if message.code() == 'd' {
                let copy_data = CopyData::from_bytes(message.to_bytes()?)?;
                rows.push(copy_data);
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

    pub fn remove(&mut self, code: char) {
        self.buffer.retain(|m| m.code() != code);
    }

    /// The buffer has CopyData messages.
    pub fn copy(&self) -> bool {
        self.buffer
            .last()
            .map(|m| m.code() == 'd' || m.code() == 'c')
            .unwrap_or(false)
    }

    pub fn flush(&self) -> bool {
        self.buffer.last().map(|m| m.code() == 'H').unwrap_or(false)
    }

    pub fn only(&self, code: char) -> bool {
        self.buffer.len() == 1
            && self
                .buffer
                .last()
                .map(|m| m.code() == code)
                .unwrap_or(false)
    }

    /// Client told us the copy failed.
    pub fn copy_fail(&self) -> bool {
        self.buffer.last().map(|m| m.code() == 'f').unwrap_or(false)
    }
}

impl From<Buffer> for Vec<Message> {
    fn from(val: Buffer) -> Self {
        val.buffer
    }
}

impl From<Vec<Message>> for Buffer {
    fn from(value: Vec<Message>) -> Self {
        Buffer { buffer: value }
    }
}

impl Deref for Buffer {
    type Target = Vec<Message>;

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
            Self::Query(query) => &query.query,
            Self::Prepared(query) => &query.query,
        }
    }
}

impl Deref for BufferedQuery {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.query()
    }
}
