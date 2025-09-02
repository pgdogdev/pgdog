//! Mirror client's handler.
//!
//! Buffers requests and simulates delay between queries.
//!

use super::*;
use crate::stats::mirror::{MirrorOutcome, MirrorStats};

/// Mirror handle state.
#[derive(Debug, Clone, PartialEq, Copy)]
enum MirrorHandlerState {
    Dropping,
    Sending,
    Idle,
}

#[derive(Debug)]
pub struct MirrorHandler {
    tx: Sender<MirrorRequest>,
    /// Percentage of requests being mirrored. 0 = 0%, 1.0 = 100%.
    exposure: f32,
    state: MirrorHandlerState,
    buffer: Vec<BufferWithDelay>,
    /// Request timer, to simulate delays between queries.
    timer: Instant,
    database: String,
    user: String,
    /// Count of requests dropped in current transaction due to exposure sampling
    dropped_in_transaction: usize,
}

impl MirrorHandler {
    #[cfg(test)]
    pub(super) fn buffer(&self) -> &[BufferWithDelay] {
        &self.buffer
    }

    pub fn new(tx: Sender<MirrorRequest>, exposure: f32, database: String, user: String) -> Self {
        Self {
            tx,
            exposure,
            state: MirrorHandlerState::Idle,
            buffer: vec![],
            timer: Instant::now(),
            database,
            user,
            dropped_in_transaction: 0,
        }
    }

    /// Request the buffer to be sent to the mirror.
    ///
    /// Returns true if request will be sent, false otherwise.
    ///
    pub fn send(&mut self, buffer: &ClientRequest) -> bool {
        let stats = MirrorStats::instance();
        stats.increment_total();

        match self.state {
            MirrorHandlerState::Dropping => {
                debug!("mirror dropping request");
                self.dropped_in_transaction += 1;
                false
            }
            MirrorHandlerState::Idle => {
                let roll = if self.exposure < 1.0 {
                    thread_rng().gen_range(0.0..1.0)
                } else {
                    0.99
                };

                if roll < self.exposure {
                    self.state = MirrorHandlerState::Sending;
                    self.buffer.push(BufferWithDelay {
                        buffer: buffer.clone(),
                        delay: Duration::ZERO,
                    });
                    self.timer = Instant::now();
                    true
                } else {
                    self.state = MirrorHandlerState::Dropping;
                    self.dropped_in_transaction = 1; // Start counting this first dropped request
                    debug!("mirror dropping transaction [exposure: {}]", self.exposure);
                    false
                }
            }
            MirrorHandlerState::Sending => {
                let now = Instant::now();
                self.buffer.push(BufferWithDelay {
                    delay: now.duration_since(self.timer),
                    buffer: buffer.clone(),
                });
                self.timer = now;
                true
            }
        }
    }

    pub fn flush(&mut self) -> bool {
        if self.state == MirrorHandlerState::Dropping {
            debug!(
                "mirror transaction dropped, recording {} dropped requests",
                self.dropped_in_transaction
            );
            self.state = MirrorHandlerState::Idle;

            // Record all dropped requests due to exposure sampling
            let stats = MirrorStats::instance();
            for _ in 0..self.dropped_in_transaction {
                stats.record_outcome(&self.database, &self.user, MirrorOutcome::Dropped);
            }
            self.dropped_in_transaction = 0; // Reset counter for next transaction

            false
        } else {
            debug!("mirror transaction flushed");
            self.state = MirrorHandlerState::Idle;
            self.dropped_in_transaction = 0; // Reset counter for next transaction

            let buffer = std::mem::take(&mut self.buffer);
            let buffer_len = buffer.len();
            match self.tx.try_send(MirrorRequest { buffer }) {
                Ok(_) => true,
                Err(e) => {
                    debug!("MirrorHandler::flush failed to send, channel full. Buffer had {} queries. Error: {:?}", buffer_len, e);
                    let stats = MirrorStats::instance();
                    // Record each request in the buffer as dropped (queue overflow)
                    for _ in 0..buffer_len {
                        stats.record_outcome(&self.database, &self.user, MirrorOutcome::Dropped);
                    }
                    false
                }
            }
        }
    }
}
