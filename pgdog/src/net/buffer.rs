//! Cancellation-safe read/write buffer
//! for Postgres messages.

use bytes::{BufMut, Bytes, BytesMut};
use rand::{thread_rng, Rng};
use std::{sync::Arc, time::Duration};
#[cfg(test)]
use tokio::sync::Notify;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    spawn,
    time::{interval, sleep, timeout},
};

use crate::{
    frontend::router::sharding::list,
    net::{Error, Message, Protocol, Query, Stream, ToBytes},
};

/// Cancellation-safe read/write buffer.
#[derive(Default, Debug, Clone)]
pub struct Buffer {
    /// Input buffer. This ensures we can partially read data
    /// from socket and we almost never allocate memory.
    input: BytesMut,
    /// Indicates we read the message code.
    code: Option<char>,
    /// Indicates we read the message length.
    len: Option<i32>,
    /// Indicates how many bytes of the message we read in total.
    bytes_read: usize,

    /// Ouput data we need to send to the stream.
    output: Option<Bytes>,
    /// How many bytes we are about to send.
    send_len: usize,

    /// Handling of multi-message requests.
    ///
    /// How many messages we already sent.
    req_sent: usize,
    /// Total request size (how many messages in total).
    req_size: usize,
    /// Where the caller thinks we are in the request.
    /// While this is less than req_sent, messages are skipped.
    req_pos: usize,

    #[cfg(test)]
    test_signal: Option<Arc<Notify>>,
}

impl Buffer {
    /// Create new cancellation-safe buffer for reading
    /// and writing Postgres messages.
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    pub fn new_cancel_test(notify: Arc<Notify>) -> Self {
        Self {
            test_signal: Some(notify),
            ..Default::default()
        }
    }

    /// Get current capacity of the buffer.
    pub fn capacity(&self) -> usize {
        self.input.len() + self.output.as_ref().map(|o| o.len()).unwrap_or(0)
    }

    /// Read a message from the stream and return it.
    ///
    /// # Cancellation safety
    ///
    /// This method is cancel-safe.
    ///
    pub async fn read(&mut self, stream: &mut Stream) -> Result<Message, Error> {
        // Read message code, if we haven't already.
        if self.code.is_none() {
            let code = stream.read_u8().await?;
            self.code = Some(code as char);
            self.input.put_u8(code);
            self.bytes_read = 1;
        } else {
            println!("cancelled");
        }

        // Read message length, if we haven't already.
        if self.len.is_none() {
            let len = stream.read_i32().await?;
            self.len = Some(len);
            self.input.put_i32(len);
            self.input.reserve(len as usize - 4);
            // SAFETY: We're about to write to this memory
            // and Postgres will send the
            unsafe {
                self.input.set_len(len as usize + 1);
            }
            self.bytes_read = 5;
        } else {
            println!("cancelled");
        }

        // Read whatever data is left for the message.
        while self.bytes_read < self.len.unwrap() as usize + 1 {
            self.bytes_read += stream.read(&mut self.input[self.bytes_read..]).await?;
        }

        // Reset state.
        self.code = None;
        self.len = None;
        self.bytes_read = 0;

        Ok(Message::new(self.input.split().freeze()))
    }

    /// Write a message to the stream.
    ///
    /// # Cancellation safety
    ///
    /// This method is cancel-safe as long as you
    /// call it again with the same message.
    ///
    pub async fn send(
        &mut self,
        message: &impl Protocol,
        stream: &mut Stream,
    ) -> Result<usize, Error> {
        if self.output.is_none() {
            self.output = Some(message.to_bytes()?);
            self.send_len = self.output.as_ref().unwrap().len();
        }

        #[cfg(test)]
        if let Some(signal) = self.test_signal.clone() {
            signal.notified().await;
        }
        stream
            .write_all_buf(&mut self.output.as_mut().unwrap())
            .await?;

        self.output = None;

        Ok(self.send_len)
    }

    /// Start request chain.
    pub fn start_chain(&mut self, len: usize) {
        self.req_size = len;
        self.req_pos = 0;
    }

    /// Send a message to the stream that's part of a chain of requests.
    ///
    /// # Cancellation safety.
    ///
    /// This method is cancel safe. If aborted, it will resume sending
    /// from the last successfully sent message.
    ///
    pub async fn send_chain(
        &mut self,
        message: &impl Protocol,
        stream: &mut Stream,
    ) -> Result<usize, Error> {
        if self.req_pos < self.req_sent {
            self.req_pos += 1;
            return Ok(0); // Don't mess up stats.
        }

        let bytes_sent = self.send(message, stream).await?;
        self.req_sent += 1;
        self.req_pos += 1;

        Ok(bytes_sent)
    }

    pub async fn flush(&mut self, stream: &mut Stream) -> Result<(), Error> {
        #[cfg(test)]
        if let Some(signal) = self.test_signal.clone() {
            signal.notified().await;
        }
        stream.flush().await?;
        self.req_pos = 0;
        self.req_sent = 0;
        self.req_size = 0;

        Ok(())
    }

    /// Request is sent in its entirety.
    pub fn send_done(&self) -> bool {
        if self.req_size > 0 {
            self.req_sent == self.req_size - 1
        } else {
            true
        }
    }

    /// There is a partial request in-flight.
    pub fn io_in_progress(&self) -> bool {
        let output = self.req_size > 0 || self.output.is_some();
        let input = self.code.is_some();
        output || input
    }
}

#[tokio::test]
async fn test_cancellation_send() {
    use crate::net::messages::Sync;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::select;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let mut stream = Stream::plain(
        TcpStream::connect(&format!("127.0.0.1:{}", port))
            .await
            .unwrap(),
    );
    let (mut recv, _) = listener.accept().await.unwrap();

    //
    // Test complete message.
    //
    let mut buffer = Buffer::new();
    for _ in 0..25 {
        let msg = Sync;
        buffer.send(&msg, &mut stream).await.unwrap();
    }
    buffer.flush(&mut stream).await.unwrap();

    for _ in 0..25 {
        let mut buf = vec![0u8; 5];
        recv.read_exact(&mut buf).await.unwrap();

        assert_eq!(buf[0] as char, 'S');
    }

    //
    // Test cancellation.
    //
    let notify = Arc::new(Notify::new());
    let mut buffer = Buffer::new_cancel_test(notify.clone());
    for _ in 0..25 {
        let cancel_notify = notify.clone();
        let cancel = async move {
            // Let the other select! branch run.
            sleep(Duration::from_millis(5)).await;

            let cancel = thread_rng().gen_bool(0.5);
            if cancel {
                return;
            } else {
                cancel_notify.notify_one();
                sleep(Duration::MAX).await;
            }
        };

        let future = buffer.send(&Sync, &mut stream);
        select! {
            res = future => { res.unwrap(); }
            _ = cancel => {
                // Try again.
                notify.notify_one();
                buffer.send(&Sync, &mut stream).await.unwrap();
            }
        }
    }

    notify.notify_one();
    buffer.flush(&mut stream).await.unwrap();

    for _ in 0..25 {
        let mut buf = vec![0u8; 5];
        recv.read_exact(&mut buf).await.unwrap();

        assert_eq!(buf[0] as char, 'S');
    }

    let err = timeout(Duration::from_millis(5), recv.read_u8())
        .await
        .is_err();
    assert!(err, "should be no more data in the socket");
}

#[tokio::test]
async fn test_cancellation_read() {
    use crate::net::messages::Sync;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::select;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let mut stream = Stream::plain(
        TcpStream::connect(&format!("127.0.0.1:{}", port))
            .await
            .unwrap(),
    );
    let (mut recv, _) = listener.accept().await.unwrap();

    //
    // Test read without cancellation.
    //
    let mut buffer = Buffer::new();

    for _ in 0..25 {
        let msg = Sync.to_bytes().unwrap();
        recv.write_all(&msg).await.unwrap();

        let msg = buffer.read(&mut stream).await.unwrap();
        assert_eq!(msg.code(), 'S');
        assert_eq!(msg.len(), 5);
    }

    //
    // Test cancellation.
    //
    let notify = Arc::new(Notify::new());
    let mut buffer = Buffer::new_cancel_test(notify.clone());
    for _ in 0..25 {
        let mut interval = interval(Duration::from_millis(1));
        let query = Query::new("1");
        recv.write_all(&query.to_bytes().unwrap()).await.unwrap();

        loop {
            let future = buffer.read(&mut stream);

            select! {
                message = future => {
                    let message = message.unwrap();
                    assert_eq!(message.code(), 'Q');
                    break;
                }

                // This will fire every ms, let the buffer continue reading.
                _ = interval.tick() => {
                    notify.notify_one();
                }
            }
        }
    }
}
