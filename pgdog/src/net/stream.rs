//! Network socket wrapper allowing us to treat secure, plain and UNIX
//! connections the same across the code.
use bytes::{BufMut, BytesMut};
use pin_project::pin_project;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufStream, ReadBuf};
use tokio::net::TcpStream;
use tracing::trace;

use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use std::ops::Deref;
use std::pin::Pin;
use std::task::Context;

use super::messages::{ErrorResponse, Message, Protocol, ReadyForQuery, Terminate};

/// Inner stream types.
#[pin_project(project = StreamInnerProjection)]
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum StreamInner {
    Plain(#[pin] BufStream<TcpStream>),
    Tls(#[pin] BufStream<tokio_rustls::TlsStream<TcpStream>>),
    DevNull,
}

/// A network socket.
#[pin_project]
#[derive(Debug)]
pub struct Stream {
    #[pin]
    inner: StreamInner,
    io_in_progress: bool,
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let project = self.project();
        match project.inner.project() {
            StreamInnerProjection::Plain(stream) => stream.poll_read(cx, buf),
            StreamInnerProjection::Tls(stream) => stream.poll_read(cx, buf),
            StreamInnerProjection::DevNull => std::task::Poll::Ready(Ok(())),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, Error>> {
        let project = self.project();
        match project.inner.project() {
            StreamInnerProjection::Plain(stream) => stream.poll_write(cx, buf),
            StreamInnerProjection::Tls(stream) => stream.poll_write(cx, buf),
            StreamInnerProjection::DevNull => std::task::Poll::Ready(Ok(buf.len())),
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> std::task::Poll<Result<(), Error>> {
        let project = self.project();
        match project.inner.project() {
            StreamInnerProjection::Plain(stream) => stream.poll_flush(cx),
            StreamInnerProjection::Tls(stream) => stream.poll_flush(cx),
            StreamInnerProjection::DevNull => std::task::Poll::Ready(Ok(())),
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> std::task::Poll<Result<(), Error>> {
        let project = self.project();
        match project.inner.project() {
            StreamInnerProjection::Plain(stream) => stream.poll_shutdown(cx),
            StreamInnerProjection::Tls(stream) => stream.poll_shutdown(cx),
            StreamInnerProjection::DevNull => std::task::Poll::Ready(Ok(())),
        }
    }
}

impl Stream {
    /// Wrap an unencrypted TCP stream.
    pub fn plain(stream: TcpStream) -> Self {
        Self {
            inner: StreamInner::Plain(BufStream::with_capacity(9126, 9126, stream)),
            io_in_progress: false,
        }
    }

    /// Wrap an encrypted TCP stream.
    pub fn tls(stream: tokio_rustls::TlsStream<TcpStream>) -> Self {
        Self {
            inner: StreamInner::Tls(BufStream::with_capacity(9126, 9126, stream)),
            io_in_progress: false,
        }
    }

    /// Create a dev null stream that discards all data.
    pub fn dev_null() -> Self {
        Self {
            inner: StreamInner::DevNull,
            io_in_progress: false,
        }
    }

    /// This is a TLS stream.
    pub fn is_tls(&self) -> bool {
        matches!(self.inner, StreamInner::Tls(_))
    }

    /// Get peer address if any. We're not using UNIX sockets (yet)
    /// so the peer address should always be available.
    pub fn peer_addr(&self) -> PeerAddr {
        match &self.inner {
            StreamInner::Plain(stream) => stream.get_ref().peer_addr().ok().into(),
            StreamInner::Tls(stream) => stream.get_ref().get_ref().0.peer_addr().ok().into(),
            StreamInner::DevNull => PeerAddr { addr: None },
        }
    }

    /// Check socket is okay while we wait for something else.
    pub async fn check(&mut self) -> Result<(), crate::net::Error> {
        let mut buf = [0u8; 1];
        match &mut self.inner {
            StreamInner::Plain(plain) => eof(plain.get_mut().peek(&mut buf).await)?,
            StreamInner::Tls(tls) => eof(tls.get_mut().get_mut().0.peek(&mut buf).await)?,
            StreamInner::DevNull => 0,
        };

        Ok(())
    }

    /// Get the current io_in_progress state.
    pub fn io_in_progress(&self) -> bool {
        self.io_in_progress
    }

    /// Send data via the stream.
    ///
    /// # Performance
    ///
    /// This is fast because the stream is buffered. Make sure to call [`Stream::send_flush`]
    /// for the last message in the exchange.
    pub async fn send(&mut self, message: &impl Protocol) -> Result<usize, crate::net::Error> {
        self.io_in_progress = true;
        let result = async {
            let bytes = message.to_bytes()?;

            match &mut self.inner {
                StreamInner::Plain(ref mut stream) => eof(stream.write_all(&bytes).await)?,
                StreamInner::Tls(ref mut stream) => eof(stream.write_all(&bytes).await)?,
                StreamInner::DevNull => (),
            }

            trace!("{:?} <-- {:#?}", self.peer_addr(), message);

            #[cfg(debug_assertions)]
            {
                use crate::net::messages::FromBytes;
                use tracing::error;

                if message.code() == 'E' {
                    let error = ErrorResponse::from_bytes(bytes.clone())?;
                    if !error.message.is_empty() {
                        error!("{:?} <-- {}", self.peer_addr(), error)
                    }
                }
            }

            Ok(bytes.len())
        }
        .await;
        self.io_in_progress = false;
        result
    }

    /// Send data via the stream and flush the buffer,
    /// ensuring the message is sent immediately.
    ///
    /// # Performance
    ///
    /// This will flush all buffers and ensure the data is actually sent via the socket.
    /// Use this only for the last message in the exchange to avoid bottlenecks.
    pub async fn send_flush(
        &mut self,
        message: &impl Protocol,
    ) -> Result<usize, crate::net::Error> {
        let sent = self.send(message).await?;
        eof(self.flush().await)?;
        trace!("😳");

        Ok(sent)
    }

    /// Send multiple messages and flush the buffer.
    pub async fn send_many(
        &mut self,
        messages: &[impl Protocol],
    ) -> Result<usize, crate::net::Error> {
        let mut sent = 0;
        for message in messages {
            sent += self.send(message).await?;
        }
        eof(self.flush().await)?;
        trace!("😳");
        Ok(sent)
    }

    /// Read a message from the stream.
    ///
    /// # Performance
    ///
    /// The stream is buffered, so this is quite fast. The pooler will perform exactly
    /// one memory allocation per protocol message. It can be optimized to re-use an existing
    /// buffer but it's not worth the complexity.
    pub async fn read(&mut self) -> Result<Message, crate::net::Error> {
        let mut buf = BytesMut::with_capacity(5);
        self.read_buf(&mut buf).await
    }

    /// Read data into a buffer, avoiding unnecessary allocations.
    pub async fn read_buf(&mut self, bytes: &mut BytesMut) -> Result<Message, crate::net::Error> {
        self.io_in_progress = true;
        let result = async {
            let code = eof(self.read_u8().await)?;
            bytes.put_u8(code);
            let len = eof(self.read_i32().await)?;
            bytes.put_i32(len);

            // Length must be at least 4 bytes.
            if len < 4 {
                return Err(crate::net::Error::UnexpectedEof);
            }

            let capacity = len as usize + 1;
            bytes.reserve(capacity); // self + 1 byte for the message code
            unsafe {
                // SAFETY: We reserved the memory above, so it's there.
                // It contains garbage but we're about to write to it.
                bytes.set_len(capacity);
            }

            eof(self.read_exact(&mut bytes[5..capacity]).await)?;

            let message = Message::new(bytes.split().freeze());

            Ok(message)
        }
        .await;
        self.io_in_progress = false;
        result
    }

    /// Send an error to the client and disconnect gracefully.
    pub async fn fatal(&mut self, error: ErrorResponse) -> Result<(), crate::net::Error> {
        self.send(&error).await?;
        self.send_flush(&Terminate).await?;

        Ok(())
    }

    /// Send an error to the client and let them know we are ready
    /// for more queries.
    pub async fn error(
        &mut self,
        error: ErrorResponse,
        in_transaction: bool,
    ) -> Result<usize, crate::net::Error> {
        let mut bytes_sent = self.send(&error).await?;
        bytes_sent += self
            .send_flush(&if in_transaction {
                ReadyForQuery::error()
            } else {
                ReadyForQuery::idle()
            })
            .await?;

        Ok(bytes_sent)
    }

    /// Get the wrapped TCP stream back.
    pub(crate) fn take(self) -> Result<TcpStream, crate::net::Error> {
        match self.inner {
            StreamInner::Plain(stream) => Ok(stream.into_inner()),
            _ => Err(crate::net::Error::UnexpectedTlsRequest),
        }
    }
}

fn eof<T>(result: std::io::Result<T>) -> Result<T, crate::net::Error> {
    match result {
        Ok(val) => Ok(val),
        Err(err) => {
            if err.kind() == ErrorKind::UnexpectedEof {
                Err(crate::net::Error::UnexpectedEof)
            } else {
                Err(crate::net::Error::Io(err))
            }
        }
    }
}

/// Wrapper around SocketAddr
/// to make it easier to debug.
pub struct PeerAddr {
    addr: Option<SocketAddr>,
}

impl Deref for PeerAddr {
    type Target = Option<SocketAddr>;

    fn deref(&self) -> &Self::Target {
        &self.addr
    }
}

impl From<Option<SocketAddr>> for PeerAddr {
    fn from(value: Option<SocketAddr>) -> Self {
        Self { addr: value }
    }
}

impl std::fmt::Debug for PeerAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(addr) = &self.addr {
            write!(f, "[{}]", addr)
        } else {
            write!(f, "")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn test_io_in_progress_initially_false() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });

        let (server_stream, _) = listener.accept().await.unwrap();
        let stream = Stream::plain(server_stream);

        assert!(
            !stream.io_in_progress(),
            "io_in_progress should be false initially"
        );

        client.await.unwrap();
    }
}
