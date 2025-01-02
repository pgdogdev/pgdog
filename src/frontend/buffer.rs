//! Message buffer.

use std::ops::{Deref, DerefMut};

use crate::net::messages::{Message, Protocol};

/// Message buffer.
pub struct Buffer {
    buffer: Vec<Message>,
}

impl Buffer {
    /// Create new buffer.
    pub fn new() -> Self {
        Self { buffer: vec![] }
    }

    /// The client expects a response immediately
    /// to a specific message which isn't a query.
    pub fn flush(&self) -> bool {
        for message in &self.buffer {
            // Describe (F) | Flush (F)
            if matches!(message.code(), 'D' | 'H') {
                return true;
            }
        }

        false
    }

    /// The buffer is full and the client won't send any more messages
    /// until it gets a reply.
    pub fn full(&self) -> bool {
        if let Some(ref message) = self.buffer.last() {
            // Flush (F) | Sync (F) | Query (F)
            if matches!(message.code(), 'H' | 'S' | 'Q') {
                return true;
            }
        }

        false
    }
}

impl Into<Vec<Message>> for Buffer {
    fn into(self) -> Vec<Message> {
        self.buffer
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
