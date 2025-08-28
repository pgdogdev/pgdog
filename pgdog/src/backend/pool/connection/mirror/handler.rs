//! Mirror client's handler.
//!
//! Buffers requests and simulates delay between queries.
//!

use super::*;
use crate::stats::mirror::{MirrorErrorType, MirrorStats};

/// Mirror handle state.
#[derive(Debug, Clone, PartialEq, Copy)]
enum MirrorHandlerState {
    /// Subsequent requests will be dropped until
    /// mirror handle is flushed.
    Dropping,
    /// Requests are being buffered and will be forwarded
    /// to the mirror when flushed.
    Sending,
    /// Mirror handle is idle.
    Idle,
}

/// Mirror handle.
#[derive(Debug)]
pub struct MirrorHandler {
    /// Sender.
    tx: Sender<MirrorRequest>,
    /// Percentage of requests being mirrored. 0 = 0%, 1.0 = 100%.
    exposure: f32,
    /// Mirror handle state.
    state: MirrorHandlerState,
    /// Request buffer.
    buffer: Vec<BufferWithDelay>,
    /// Request timer, to simulate delays between queries.
    timer: Instant,
}

impl MirrorHandler {
    #[cfg(test)]
    pub(super) fn buffer(&self) -> &[BufferWithDelay] {
        &self.buffer
    }

    /// Create new mirror handle with exposure.
    pub fn new(tx: Sender<MirrorRequest>, exposure: f32) -> Self {
        Self {
            tx,
            exposure,
            state: MirrorHandlerState::Idle,
            buffer: vec![],
            timer: Instant::now(),
        }
    }

    /// Request the buffer to be sent to the mirror.
    ///
    /// Returns true if request will be sent, false otherwise.
    ///
    pub fn send(&mut self, buffer: &ClientRequest) -> bool {
        // Track total requests
        let stats = MirrorStats::instance();
        stats.increment_total();

        match self.state {
            MirrorHandlerState::Dropping => {
                debug!("mirror dropping request");
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

    /// Flush buffered requests to mirror.
    pub fn flush(&mut self) -> bool {
        if self.state == MirrorHandlerState::Dropping {
            debug!("mirror transaction dropped");
            self.state = MirrorHandlerState::Idle;
            false
        } else {
            debug!("mirror transaction flushed");
            self.state = MirrorHandlerState::Idle;

            match self.tx.try_send(MirrorRequest {
                buffer: std::mem::take(&mut self.buffer),
            }) {
                Ok(_) => true,
                Err(_) => {
                    // Buffer is full, count as dropped
                    let stats = MirrorStats::instance();
                    stats.increment_dropped();
                    stats.record_error("unknown", MirrorErrorType::BufferFull);
                    false
                }
            }
        }
    }
}
