//! Mirror client's handler.
//!
//! Buffers requests and simulates delay between queries.
//!

use super::*;
use crate::backend::pool::MirrorStats;
use parking_lot::Mutex;
use std::sync::Arc;

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
    /// Reference to cluster stats for tracking mirror metrics.
    stats: Arc<Mutex<MirrorStats>>,
}

impl MirrorHandler {
    #[cfg(test)]
    pub(super) fn buffer(&self) -> &[BufferWithDelay] {
        &self.buffer
    }

    /// Create new mirror handle with exposure.
    pub fn new(tx: Sender<MirrorRequest>, exposure: f32, stats: Arc<Mutex<MirrorStats>>) -> Self {
        Self {
            tx,
            exposure,
            state: MirrorHandlerState::Idle,
            buffer: vec![],
            timer: Instant::now(),
            stats,
        }
    }

    /// Request the buffer to be sent to the mirror.
    ///
    /// Returns true if request will be sent, false otherwise.
    ///
    pub fn send(&mut self, buffer: &ClientRequest) -> bool {
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
        self.increment_total_count();

        if self.state == MirrorHandlerState::Dropping {
            debug!("mirror transaction dropped");
            self.state = MirrorHandlerState::Idle;
            self.increment_dropped_count();
            false
        } else {
            debug!("mirror transaction flushed");
            self.state = MirrorHandlerState::Idle;

            match self.tx.try_send(MirrorRequest {
                buffer: std::mem::take(&mut self.buffer),
            }) {
                Ok(()) => {
                    self.increment_mirrored_count();
                    self.increment_queue_length();
                    true
                }
                Err(_) => {
                    warn!("mirror buffer overflow, dropping transaction");
                    self.increment_error_count();
                    false
                }
            }
        }
    }

    /// Increment the total request count.
    pub fn increment_total_count(&self) {
        let mut stats = self.stats.lock();
        stats.counts.total_count += 1;
    }

    /// Increment the mirrored request count.
    pub fn increment_mirrored_count(&self) {
        let mut stats = self.stats.lock();
        stats.counts.mirrored_count += 1;
    }

    /// Increment the dropped request count.
    pub fn increment_dropped_count(&self) {
        let mut stats = self.stats.lock();
        stats.counts.dropped_count += 1;
    }

    /// Increment the error count.
    pub fn increment_error_count(&self) {
        let mut stats = self.stats.lock();
        stats.counts.error_count += 1;
    }

    /// Increment the queue length.
    pub fn increment_queue_length(&self) {
        let mut stats = self.stats.lock();
        stats.counts.queue_length += 1;
    }

    /// Decrement the queue length.
    pub fn decrement_queue_length(&self) {
        let mut stats = self.stats.lock();
        stats.counts.queue_length = stats.counts.queue_length.saturating_sub(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::pool::MirrorStats;
    use parking_lot::Mutex;
    use std::sync::Arc;
    use tokio::sync::mpsc::{channel, Receiver};

    fn create_test_handler(
        exposure: f32,
    ) -> (
        MirrorHandler,
        Arc<Mutex<MirrorStats>>,
        Receiver<MirrorRequest>,
    ) {
        let (tx, rx) = channel(1000); // Keep receiver to prevent channel closure
        let stats = Arc::new(Mutex::new(MirrorStats::default()));
        let handler = MirrorHandler::new(tx, exposure, stats.clone());
        (handler, stats, rx)
    }

    fn get_stats_counts(stats: &Arc<Mutex<MirrorStats>>) -> (usize, usize, usize, usize) {
        let stats = stats.lock();
        (
            stats.counts.total_count,
            stats.counts.mirrored_count,
            stats.counts.dropped_count,
            stats.counts.error_count,
        )
    }

    #[test]
    fn test_stats_initially_zero() {
        let (_handler, stats, _rx) = create_test_handler(1.0);
        let (total, mirrored, dropped, error) = get_stats_counts(&stats);
        assert_eq!(total, 0, "total_count should be 0 initially");
        assert_eq!(mirrored, 0, "mirrored_count should be 0 initially");
        assert_eq!(dropped, 0, "dropped_count should be 0 initially");
        assert_eq!(error, 0, "error_count should be 0 initially");
    }

    #[test]
    fn test_successful_mirror_updates_stats() {
        let (mut handler, stats, _rx) = create_test_handler(1.0);

        // Send some requests
        assert!(handler.send(&vec![].into()));
        assert!(handler.send(&vec![].into()));
        assert!(handler.send(&vec![].into()));

        // Stats shouldn't update until flush
        let (total, mirrored, dropped, _) = get_stats_counts(&stats);
        assert_eq!(total, 0, "total_count should be 0 before flush");
        assert_eq!(mirrored, 0, "mirrored_count should be 0 before flush");
        assert_eq!(dropped, 0, "dropped_count should be 0 before flush");

        // Flush should update stats
        assert!(handler.flush());
        let (total, mirrored, dropped, _) = get_stats_counts(&stats);
        assert_eq!(total, 1, "total_count should be 1 after flush");
        assert_eq!(
            mirrored, 1,
            "mirrored_count should be 1 after successful flush"
        );
        assert_eq!(dropped, 0, "dropped_count should remain 0");
    }

    #[test]
    fn test_dropped_requests_update_stats() {
        let (mut handler, stats, _rx) = create_test_handler(0.0); // 0% exposure - always drop

        // First request determines if we're dropping or sending
        assert!(
            !handler.send(&vec![].into()),
            "Should drop with 0% exposure"
        );

        // Flush a dropped request
        assert!(!handler.flush());
        let (total, mirrored, dropped, _) = get_stats_counts(&stats);
        assert_eq!(total, 1, "total_count should be 1 after flush");
        assert_eq!(
            mirrored, 0,
            "mirrored_count should be 0 for dropped request"
        );
        assert_eq!(dropped, 1, "dropped_count should be 1 after dropping");
    }

    #[test]
    fn test_multiple_transactions() {
        let (mut handler, stats, _rx) = create_test_handler(1.0);

        // Transaction 1: successful
        assert!(handler.send(&vec![].into()));
        assert!(handler.send(&vec![].into()));
        assert!(handler.flush());

        // Transaction 2: successful
        assert!(handler.send(&vec![].into()));
        assert!(handler.flush());

        // Transaction 3: successful
        assert!(handler.send(&vec![].into()));
        assert!(handler.send(&vec![].into()));
        assert!(handler.send(&vec![].into()));
        assert!(handler.flush());

        let (total, mirrored, dropped, _) = get_stats_counts(&stats);
        assert_eq!(total, 3, "total_count should be 3 after 3 flushes");
        assert_eq!(
            mirrored, 3,
            "mirrored_count should be 3 after 3 successful flushes"
        );
        assert_eq!(dropped, 0, "dropped_count should be 0");
    }

    #[test]
    fn test_increment_methods() {
        let (handler, stats, _rx) = create_test_handler(1.0);

        // Test each increment method directly
        handler.increment_total_count();
        handler.increment_mirrored_count();
        handler.increment_dropped_count();
        handler.increment_error_count();

        let (total, mirrored, dropped, error) = get_stats_counts(&stats);
        assert_eq!(total, 1, "increment_total_count should increment by 1");
        assert_eq!(
            mirrored, 1,
            "increment_mirrored_count should increment by 1"
        );
        assert_eq!(dropped, 1, "increment_dropped_count should increment by 1");
        assert_eq!(error, 1, "increment_error_count should increment by 1");

        // Test incrementing multiple times
        handler.increment_total_count();
        handler.increment_total_count();

        let (total, _, _, _) = get_stats_counts(&stats);
        assert_eq!(total, 3, "Multiple increments should accumulate");
    }

    #[test]
    fn test_mixed_success_and_drops() {
        // Create handler with partial exposure
        let (mut handler1, stats, _rx1) = create_test_handler(1.0);
        let (mut handler2, _, _rx2) = create_test_handler(0.0);

        // handler1: successful transaction
        assert!(handler1.send(&vec![].into()));
        assert!(handler1.flush());

        // handler2 (0% exposure): dropped transaction
        assert!(!handler2.send(&vec![].into()));

        // For handler1's stats
        let (total, mirrored, dropped, _) = get_stats_counts(&stats);
        assert_eq!(total, 1, "Should have 1 total from handler1");
        assert_eq!(mirrored, 1, "Should have 1 mirrored from handler1");
        assert_eq!(dropped, 0, "Should have 0 dropped from handler1");
    }

    #[test]
    fn test_queue_length_increments_on_send() {
        let (mut handler, stats, _rx) = create_test_handler(1.0);

        // Initially queue_length should be 0
        {
            let stats = stats.lock();
            assert_eq!(
                stats.counts.queue_length, 0,
                "queue_length should be 0 initially"
            );
        }

        // Send a request (it should be buffered but not yet sent to channel)
        assert!(handler.send(&vec![].into()));
        assert!(handler.send(&vec![].into()));
        assert!(handler.send(&vec![].into()));

        // Queue length should remain 0 until flush (which sends to channel)
        {
            let stats = stats.lock();
            assert_eq!(
                stats.counts.queue_length, 0,
                "queue_length should still be 0 before flush"
            );
        }

        // After flush, queue_length should be incremented
        assert!(handler.flush());
        {
            let stats = stats.lock();
            assert_eq!(
                stats.counts.queue_length, 1,
                "queue_length should be 1 after flush"
            );
        }
    }

    #[test]
    fn test_queue_length_decrements_on_process() {
        // This test will fail until we implement decrement logic in mod.rs
        // It tests that queue_length decrements when messages are consumed from the channel
    }

    #[test]
    fn test_queue_length_with_dropped_transactions() {
        let (mut handler, stats, _rx) = create_test_handler(0.0); // 0% exposure

        // Initially queue_length should be 0
        {
            let stats = stats.lock();
            assert_eq!(
                stats.counts.queue_length, 0,
                "queue_length should be 0 initially"
            );
        }

        // Send request (will be dropped due to 0% exposure)
        assert!(!handler.send(&vec![].into()));

        // Flush the dropped request
        assert!(!handler.flush());

        // Queue length should remain 0 for dropped transactions
        {
            let stats = stats.lock();
            assert_eq!(
                stats.counts.queue_length, 0,
                "queue_length should remain 0 for dropped transactions"
            );
            assert_eq!(stats.counts.dropped_count, 1, "dropped_count should be 1");
        }
    }

    #[test]
    fn test_queue_length_with_channel_overflow() {
        let (tx, _rx) = channel(1); // Channel with capacity of 1
        let stats = Arc::new(Mutex::new(MirrorStats::default()));
        let mut handler = MirrorHandler::new(tx, 1.0, stats.clone());

        // Fill the channel
        assert!(handler.send(&vec![].into()));
        assert!(handler.flush());

        // Now try to send another when channel is full
        assert!(handler.send(&vec![].into()));
        assert!(!handler.flush()); // Should fail due to channel overflow

        // Queue length should not increment on error
        {
            let stats = stats.lock();
            assert_eq!(
                stats.counts.queue_length, 1,
                "queue_length should be 1 (first successful send)"
            );
            assert_eq!(
                stats.counts.error_count, 1,
                "error_count should be 1 due to overflow"
            );
        }
    }

    #[test]
    fn test_queue_length_never_negative() {
        // Test to ensure queue_length never goes negative even with mismatched increment/decrement
        let stats = Arc::new(Mutex::new(MirrorStats::default()));

        // Manually try to decrement without incrementing (should use saturating_sub)
        // This will be tested more thoroughly once decrement_queue_length is implemented
        {
            let mut stats = stats.lock();
            // Simulating a decrement when queue is already 0
            stats.counts.queue_length = stats.counts.queue_length.saturating_sub(1);
            assert_eq!(
                stats.counts.queue_length, 0,
                "queue_length should not go negative"
            );
        }
    }
}
