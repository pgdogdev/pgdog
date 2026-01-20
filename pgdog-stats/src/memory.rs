use std::ops::Add;

use serde::{Deserialize, Serialize};

/// Statistics calculated for the network buffer used
/// by clients and servers.
#[derive(Debug, Clone, Default, Copy, Serialize, Deserialize)]
pub struct MemoryStats {
    /// Message buffer stats.
    pub buffer: MessageBufferStats,
    /// Memory used by prepared statements.
    pub prepared_statements: usize,
    /// Memory used by the network stream buffer.
    pub stream: usize,
}

impl Add for MemoryStats {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            buffer: self.buffer + rhs.buffer,
            prepared_statements: self.prepared_statements + rhs.prepared_statements,
            stream: self.stream + rhs.stream,
        }
    }
}

impl MemoryStats {
    /// Calculate total memory usage.
    pub fn total(&self) -> usize {
        self.buffer.bytes_alloc + self.prepared_statements + self.stream
    }
}

#[derive(Debug, Clone, Default, Copy, Serialize, Deserialize)]
pub struct MessageBufferStats {
    pub reallocs: usize,
    pub reclaims: usize,
    pub bytes_used: usize,
    pub bytes_alloc: usize,
}

impl Add for MessageBufferStats {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            reallocs: rhs.reallocs + self.reallocs,
            reclaims: rhs.reclaims + self.reclaims,
            bytes_used: rhs.bytes_used + self.bytes_used,
            bytes_alloc: rhs.bytes_alloc + self.bytes_alloc,
        }
    }
}
