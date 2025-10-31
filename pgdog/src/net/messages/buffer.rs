//! Cancel-safe and memory-efficient
//! read buffer for Postgres messages.

use std::io::Cursor;

use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::net::stream::eof;

use super::{Error, Message};

const HEADER_SIZE: usize = 5;
const BUFFER_SIZE: usize = 4096;

#[derive(Default, Debug, Clone)]
pub struct MessageBuffer {
    buffer: BytesMut,
    bytes_used: usize,
}

impl MessageBuffer {
    /// Create new cancel-safe
    /// message buffer.
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::with_capacity(BUFFER_SIZE),
            bytes_used: 0,
        }
    }

    /// Buffer capacity.
    pub fn capacity(&self) -> usize {
        self.buffer.capacity()
    }

    async fn read_internal(
        &mut self,
        stream: &mut (impl AsyncRead + Unpin + AsyncReadExt),
    ) -> Result<Message, Error> {
        loop {
            if let Some(size) = self.message_size() {
                if self.have_message() {
                    return Ok(Message::new(self.buffer.split_to(size).freeze()));
                }

                self.ensure_capacity(size); // Reserve at least enough space for the whole message.
            }

            if self.buffer.capacity() == 0 {
                self.ensure_capacity(BUFFER_SIZE);
            }

            let read = eof(stream.read_buf(&mut self.buffer).await)?;
            self.bytes_used += read;

            if read == 0 {
                return Err(Error::UnexpectedEof);
            }
        }
    }

    // This may or may not allocate memory, depending on how big of
    // a message we are receiving.
    fn ensure_capacity(&mut self, amount: usize) {
        if self.buffer.try_reclaim(amount) {
            self.bytes_used -= amount;
        } else {
            self.buffer.reserve(amount);
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
            Some(len as usize)
        } else {
            None
        }
    }

    /// Re-allcoate buffer if it exceeds capacity.
    pub fn shrink_to_fit(&mut self) {
        if self.bytes_used > BUFFER_SIZE {
            let mut buffer = BytesMut::with_capacity(BUFFER_SIZE);
            buffer.extend_from_slice(&self.buffer);
            self.bytes_used += self.buffer.len();
            self.buffer = buffer;
        }
    }

    /// Read a Postgres message off of a stream.
    ///
    /// # Cancellation safety
    ///
    /// This method is cancel-safe.
    ///
    pub async fn read(
        &mut self,
        stream: &mut (impl AsyncRead + Unpin + AsyncReadExt),
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
            let mut rng = StdRng::from_entropy();

            for i in 0..5000 {
                let msg = Sync.to_bytes().unwrap();
                conn.write_all(&msg).await.unwrap();

                let query_len = rng.gen_range(10..=1000);
                let query: String = (0..query_len)
                    .map(|_| rng.sample(rand::distributions::Alphanumeric) as char)
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
}
