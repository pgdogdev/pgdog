//! Message buffer.

use std::ops::{Deref, DerefMut};

use crate::net::{
    messages::{parse::Parse, Bind, FromBytes, Message, Protocol, Query, ToBytes},
    Error,
};

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
            // Flush (F) | Sync (F) | Query (F) | CopyDone (F)
            if matches!(message.code(), 'H' | 'S' | 'Q' | 'c') {
                return true;
            }

            // CopyData (F)
            // Flush data to backend if we've buffered 4K.
            if message.code() == 'd' && self.len() > 4096 {
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
    pub fn query(&self) -> Result<Option<String>, Error> {
        for message in &self.buffer {
            if message.code() == 'Q' {
                let query = Query::from_bytes(message.to_bytes()?)?;
                return Ok(Some(query.query));
            } else if message.code() == 'P' {
                let parse = Parse::from_bytes(message.to_bytes()?)?;
                return Ok(Some(parse.query));
            }
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
