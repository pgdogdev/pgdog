//! Cancel-safe and memory-efficient
//! read buffer for Postgres messages.

use std::io::Cursor;

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::net::stream::eof;

use super::{Error, Message};

const HEADER_SIZE: usize = 5;

#[derive(Default, Debug, Clone)]
pub struct MessageBuffer {
    buffer: BytesMut,
}

impl MessageBuffer {
    /// Create new cancel-safe
    /// message buffer.
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::with_capacity(1028),
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

                self.buffer.reserve(size); // Reserve at least enough space for the whole message.
            }

            if self.buffer.capacity() == 0 {
                self.buffer.reserve(1028);
            }

            let read = eof(stream.read_buf(&mut self.buffer).await)?;

            if read == 0 {
                return Err(Error::UnexpectedEof);
            }
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

#[tokio::test(flavor = "multi_thread")]
async fn test_read() {
    use crate::net::{FromBytes, Parse, Protocol, Sync, ToBytes};
    use std::time::Duration;
    use tokio::{
        io::AsyncWriteExt,
        net::{TcpListener, TcpStream},
        spawn,
        sync::mpsc,
        time::interval,
    };

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
