//! Cancel-safe and memory-efficient
//! read buffer for Postgres messages.

use std::io::Cursor;

use bytes::{Buf, BytesMut};
use pgdog_stats::MessageBufferStats;
use tokio::io::AsyncReadExt;

use crate::net::stream::eof;

use super::{Error, Message};

const HEADER_SIZE: usize = 5;

#[derive(Default, Debug, Clone)]
pub struct MessageBuffer {
    buffer: BytesMut,
    capacity: usize,
    stats: MessageBufferStats,
}

impl MessageBuffer {
    /// Create new cancel-safe
    /// message buffer.
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: BytesMut::with_capacity(capacity),
            capacity,
            stats: MessageBufferStats {
                bytes_alloc: capacity,
                ..Default::default()
            },
        }
    }

    /// Buffer capacity.
    pub fn capacity(&self) -> usize {
        self.buffer.capacity()
    }

    async fn read_internal(
        &mut self,
        stream: &mut (impl Unpin + AsyncReadExt),
    ) -> Result<Message, Error> {
        loop {
            if let Some(size) = self.message_size() {
                if self.have_message() {
                    return Ok(Message::new(self.buffer.split_to(size).freeze()));
                }

                self.ensure_capacity(size); // Reserve at least enough space for the whole message.
            }

            // Ensure there is 1/4 of the buffer
            // available at all times. This clears memory usage
            // frequently.
            self.ensure_capacity(self.capacity / 4);

            let read = eof(stream.read_buf(&mut self.buffer).await)?;
            self.stats.bytes_used += read;

            if read == 0 {
                return Err(Error::UnexpectedEof);
            }
        }
    }

    // This may or may not allocate memory, depending on how big of
    // a message we are receiving.
    fn ensure_capacity(&mut self, amount: usize) {
        if self.buffer.try_reclaim(amount) {
            // I know this isn't exactly right, we could be reclaiming more.
            // But undercounting is better than overcounting.
            self.stats.bytes_used = self.stats.bytes_used.saturating_sub(amount);
            self.stats.reclaims += 1;
        } else {
            self.buffer.reserve(amount);
            // Possibly undercounting.
            self.stats.bytes_alloc += amount;
        }
    }

    fn have_message(&self) -> bool {
        self.message_size()
            .map(|len| self.buffer.len() >= len)
            .unwrap_or(false)
    }

    fn message_size(&self) -> Option<usize> {
        if self.buffer.len() >= HEADER_SIZE {
            let mut cur = Cursor::new(&self.buffer);
            let _code = cur.get_u8();
            let len = cur.get_i32() as usize + 1;
            Some(len)
        } else {
            None
        }
    }

    /// Re-allcoate buffer if it exceeds capacity.
    pub fn shrink_to_fit(&mut self) -> bool {
        // Re-allocate the buffer to save on memory.
        if self.stats.bytes_alloc > self.capacity * 2 {
            // Create new buffer and copy contents.
            let mut buffer = BytesMut::with_capacity(self.capacity);
            buffer.extend_from_slice(&self.buffer);

            // Update stats.
            self.stats.bytes_used = self.buffer.len();
            self.buffer = buffer;
            self.stats.reallocs += 1;
            self.stats.bytes_alloc = self.capacity; // Possibly undercounting.
            true
        } else {
            false
        }
    }

    /// Get buffer stats.
    pub fn stats(&self) -> &MessageBufferStats {
        &self.stats
    }

    /// Read a Postgres message off of a stream.
    ///
    /// # Cancellation safety
    ///
    /// This method is cancel-safe.
    ///
    pub async fn read(
        &mut self,
        stream: &mut (impl Unpin + AsyncReadExt),
    ) -> Result<Message, Error> {
        self.read_internal(stream).await
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::net::{FromBytes, Parse, Protocol, Sync, ToBytes};
    use bytes::BufMut;
    use std::time::Duration;
    use tokio::{
        io::AsyncWriteExt,
        net::{TcpListener, TcpStream},
        spawn,
        sync::mpsc,
        time::interval,
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, mut rx) = mpsc::channel(1);

        spawn(async move {
            let mut conn = TcpStream::connect(addr).await.unwrap();
            use rand::{rngs::StdRng, Rng, SeedableRng};
            let mut rng = StdRng::from_os_rng();

            for i in 0..5000 {
                let msg = Sync.to_bytes().unwrap();
                conn.write_all(&msg).await.unwrap();

                let query_len = rng.random_range(10..=1000);
                let query: String = (0..query_len)
                    .map(|_| rng.sample(rand::distr::Alphanumeric) as char)
                    .collect();

                let msg = Parse::named(format!("test_{}", i), &query)
                    .to_bytes()
                    .unwrap();
                conn.write_all(&msg).await.unwrap();
                conn.flush().await.unwrap();
            }
            rx.recv().await;
        });

        let (mut conn, _) = listener.accept().await.unwrap();
        let mut buf = MessageBuffer::default();

        let mut counter = 0;
        let mut interrupted = 0;
        let mut interval = interval(Duration::from_millis(1));

        while counter < 10000 {
            let msg = tokio::select! {
                msg = buf.read(&mut conn) => {
                    msg.unwrap()
                }

                _ = interval.tick() => {
                    interrupted += 1;
                    continue;
                }
            };

            if counter % 2 == 0 {
                assert_eq!(msg.code(), 'S');
            } else {
                assert_eq!(msg.code(), 'P');
                let parse = Parse::from_bytes(msg.to_bytes().unwrap()).unwrap();
                assert_eq!(parse.name(), format!("test_{}", counter / 2));
            }

            counter += 1;
        }

        tx.send(0).await.unwrap();

        assert!(interrupted > 0, "no cancellations");
        assert_eq!(counter, 10000, "didnt receive all messages");
        assert!(matches!(
            buf.read(&mut conn).await.err(),
            Some(Error::UnexpectedEof)
        ));
        assert!(buf.capacity() > 0);
    }

    #[test]
    fn test_bytes_mut() {
        let region = stats_alloc::Region::new(crate::GLOBAL);

        let mut original = BytesMut::with_capacity(5 * 1000);
        assert_eq!(original.capacity(), 5 * 1000);
        assert_eq!(original.len(), 0);

        for _ in 0..(5 * 25 * 1000) {
            original.put_u8('S' as u8);
            original.put_i32(4);

            let sync = original.split_to(5);
            assert_eq!(sync.capacity(), 5);
            assert_eq!(sync.len(), 5);

            // Removes it from the buffer, giving that space back.
            drop(sync);
        }

        assert_eq!(region.change().allocations, 2);
        assert!(region.change().bytes_allocated < 6000); // Depends on the allocator, but it will never be more.
    }

    #[tokio::test]
    async fn test_shrink_to_fit() {
        use std::io::Cursor;

        let mut stream_data = Vec::new();

        // Create a large message (10KB query)
        let large_query = "SELECT * FROM ".to_string() + &"x".repeat(10_000);
        let large_msg = Parse::named("large", &large_query).to_bytes().unwrap();
        stream_data.extend_from_slice(&large_msg);

        // Create a small message
        let small_msg = Sync.to_bytes().unwrap();
        stream_data.extend_from_slice(&small_msg);

        let mut cursor = Cursor::new(stream_data);
        let mut buf = MessageBuffer::new(4096);

        // Read the large message
        let msg = buf.read(&mut cursor).await.unwrap();
        assert_eq!(msg.code(), 'P');

        // At this point, bytes_used should be > BUFFER_SIZE
        let bytes_used_before = buf.stats.bytes_used;
        assert!(bytes_used_before > 4096);

        // Shrink the buffer
        assert!(buf.shrink_to_fit());

        // After shrinking, we should have reset to BUFFER_SIZE capacity
        assert_eq!(buf.buffer.capacity(), 4096);

        // Should still be able to read the next message
        let msg = buf.read(&mut cursor).await.unwrap();
        assert_eq!(msg.code(), 'S');
    }

    #[tokio::test]
    async fn test_shrink_to_fit_preserves_partial_data() {
        use bytes::BufMut;

        let mut buf = MessageBuffer::new(4096);

        // Simulate having allocated memory for a large message
        buf.stats.bytes_alloc = 4096 * 3;
        buf.stats.bytes_used = 4096 * 2;

        // Put some partial message data in the buffer (incomplete header)
        buf.buffer.put_u8(b'P');
        buf.buffer.put_u8(0);
        buf.buffer.put_u8(0);

        let data_before = buf.buffer.clone();

        // Shrink should preserve the partial data
        assert!(buf.shrink_to_fit());

        assert_eq!(buf.stats().bytes_alloc, 4096);
        assert_eq!(buf.buffer.len(), data_before.len());
        assert_eq!(buf.buffer[..], data_before[..]);
        assert_eq!(buf.buffer.capacity(), 4096);
    }

    #[tokio::test]
    async fn test_shrink_to_fit_no_realloc_when_under_capacity() {
        use std::io::Cursor;

        let mut stream_data = Vec::new();

        // Create several small messages that won't exceed BUFFER_SIZE
        for i in 0..10 {
            let query = format!("SELECT {}", i);
            let msg = Parse::named(format!("stmt_{}", i), &query)
                .to_bytes()
                .unwrap();
            stream_data.extend_from_slice(&msg);
        }

        let mut cursor = Cursor::new(stream_data);
        let mut buf = MessageBuffer::new(4096);

        // Read all small messages
        for _ in 0..10 {
            let msg = buf.read(&mut cursor).await.unwrap();
            assert_eq!(msg.code(), 'P');
        }

        // At this point, bytes_used should be below BUFFER_SIZE
        let bytes_used = buf.stats.bytes_used;
        assert!(bytes_used <= 4096);

        let capacity_before = buf.buffer.capacity();
        let reallocs_before = buf.stats.reallocs;
        let bytes_alloc_before = buf.stats.bytes_alloc;
        let frees_before = buf.stats.reclaims;

        // Should not reallocate since we haven't exceeded BUFFER_SIZE
        assert!(!buf.shrink_to_fit());

        // Verify no reallocation occurred and stats remain unchanged
        assert_eq!(buf.buffer.capacity(), capacity_before);
        assert_eq!(buf.stats.reallocs, reallocs_before);
        assert_eq!(buf.stats.bytes_alloc, bytes_alloc_before);
        assert_eq!(buf.stats.reclaims, frees_before);
    }
}
