//! Network socket wrapper allowing us to treat secure, plain and UNIX
//! connections the same across the code.
use bytes::{BufMut, BytesMut};
use pin_project::pin_project;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufStream, ReadBuf};
use tokio::net::TcpStream;
use tracing::trace;

use std::io::Error;
use std::net::SocketAddr;
use std::ops::Deref;
use std::pin::Pin;
use std::task::Context;

use super::messages::{ErrorResponse, Message, Protocol, ReadyForQuery, Terminate};

/// A network socket.
#[pin_project(project = StreamProjection)]
#[derive(Debug)]
pub enum Stream {
    Plain(#[pin] BufStream<TcpStream>),
    Tls(#[pin] BufStream<tokio_rustls::TlsStream<TcpStream>>),
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let project = self.project();
        match project {
            StreamProjection::Plain(stream) => stream.poll_read(cx, buf),
            StreamProjection::Tls(stream) => stream.poll_read(cx, buf),
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
        match project {
            StreamProjection::Plain(stream) => stream.poll_write(cx, buf),
            StreamProjection::Tls(stream) => stream.poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> std::task::Poll<Result<(), Error>> {
        let project = self.project();
        match project {
            StreamProjection::Plain(stream) => stream.poll_flush(cx),
            StreamProjection::Tls(stream) => stream.poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> std::task::Poll<Result<(), Error>> {
        let project = self.project();
        match project {
            StreamProjection::Plain(stream) => stream.poll_shutdown(cx),
            StreamProjection::Tls(stream) => stream.poll_shutdown(cx),
        }
    }
}

impl Stream {
    /// Wrap an unencrypted TCP stream.
    pub fn plain(stream: TcpStream) -> Self {
        Self::Plain(BufStream::with_capacity(9126, 9126, stream))
    }

    /// Wrap an encrypted TCP stream.
    pub fn tls(stream: tokio_rustls::TlsStream<TcpStream>) -> Self {
        Self::Tls(BufStream::with_capacity(9126, 9126, stream))
    }

    /// This is a TLS stream.
    pub fn is_tls(&self) -> bool {
        matches!(self, Self::Tls(_))
    }

    /// Get peer address if any. We're not using UNIX sockets (yet)
    /// so the peer address should always be available.
    pub fn peer_addr(&self) -> PeerAddr {
        match self {
            Self::Plain(stream) => stream.get_ref().peer_addr().ok().into(),
            Self::Tls(stream) => stream.get_ref().get_ref().0.peer_addr().ok().into(),
        }
    }

    /// Check socket is okay while we wait for something else.
    pub async fn check(&mut self) -> Result<(), crate::net::Error> {
        let mut buf = [0u8; 1];
        match self {
            Self::Plain(plain) => plain.get_mut().peek(&mut buf).await?,
            Self::Tls(tls) => tls.get_mut().get_mut().0.peek(&mut buf).await?,
        };

        Ok(())
    }

    /// Send data via the stream.
    ///
    /// # Performance
    ///
    /// This is fast because the stream is buffered. Make sure to call [`Stream::send_flush`]
    /// for the last message in the exchange.
    pub async fn send(&mut self, message: &impl Protocol) -> Result<usize, crate::net::Error> {
        let bytes = message.to_bytes()?;

        match self {
            Stream::Plain(ref mut stream) => stream.write_all(&bytes).await?,
            Stream::Tls(ref mut stream) => stream.write_all(&bytes).await?,
        }

        #[cfg(debug_assertions)]
        {
            trace!(">>> {:?} [{:?}]", message.message()?, self.peer_addr());
            use crate::net::messages::FromBytes;
            use tracing::error;

            if message.code() == 'E' {
                let error = ErrorResponse::from_bytes(bytes.clone())?;
                if !error.message.is_empty() {
                    error!("{:?} <= {}", self.peer_addr(), error)
                }
            }
        }

        Ok(bytes.len())
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
        self.flush().await?;
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
        self.flush().await?;
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
        let code = self.read_u8().await?;
        let len = self.read_i32().await?;

        bytes.put_u8(code);
        bytes.put_i32(len);

        // Length must be at least 4 bytes.
        if len < 4 {
            return Err(crate::net::Error::Eof);
        }

        let capacity = len as usize + 1;
        bytes.reserve(capacity); // self + 1 byte for the message code
        unsafe {
            // SAFETY: We reserved the memory above, so it's there.
            // It contains garbage but we're about to write to it.
            bytes.set_len(capacity);
        }

        self.read_exact(&mut bytes[5..capacity]).await?;

        let message = Message::new(bytes.split().freeze());

        Ok(message)
    }

    /// Send an error to the client and disconnect gracefully.
    pub async fn fatal(&mut self, error: ErrorResponse) -> Result<(), crate::net::Error> {
        self.send(&error).await?;
        self.send_flush(&Terminate).await?;

        Ok(())
    }

    /// Send an error to the client and let them know we are ready
    /// for more queries.
    pub async fn error(&mut self, error: ErrorResponse) -> Result<(), crate::net::Error> {
        self.send(&error).await?;
        self.send_flush(&ReadyForQuery::idle()).await?;

        Ok(())
    }

    /// Get the wrapped TCP stream back.
    pub(crate) fn take(self) -> Result<TcpStream, crate::net::Error> {
        match self {
            Self::Plain(stream) => Ok(stream.into_inner()),
            _ => Err(crate::net::Error::UnexpectedTlsRequest),
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
