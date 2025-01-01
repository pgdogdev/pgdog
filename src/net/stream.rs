//! Network socket wrapper allowing us to treat secure, plain and UNIX
//! connections the same across the code.
use bytes::{BufMut, BytesMut};
use pin_project::pin_project;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufStream, ReadBuf};
use tokio::net::TcpStream;
use tokio_native_tls::TlsStream;
use tracing::debug;

use std::io::Error;
use std::pin::Pin;
use std::task::Context;

use super::messages::{Message, Protocol, ToBytes};

/// A network socket.
#[pin_project(project = StreamProjection)]
pub enum Stream {
    Plain(#[pin] BufStream<TcpStream>),
    Tls(#[pin] BufStream<TlsStream<TcpStream>>),
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
        Self::Plain(BufStream::new(stream))
    }

    /// Wrap an encrypted TCP stream.
    pub fn tls(stream: TlsStream<TcpStream>) -> Self {
        Self::Tls(BufStream::new(stream))
    }

    /// Send data via the stream.
    ///
    /// # Performance
    ///
    /// This is fast because the stream is buffered. Make sure to call [`Stream::send_flush`]
    /// for the last message in the exchange.
    pub async fn send(
        &mut self,
        message: impl ToBytes + Protocol,
    ) -> Result<(), crate::net::Error> {
        debug!("📡 <= {}", message.code());

        let bytes = message.to_bytes()?;
        match self {
            Stream::Plain(ref mut stream) => stream.write_all(&bytes).await?,
            Stream::Tls(ref mut stream) => stream.write_all(&bytes).await?,
        }

        Ok(())
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
        message: impl ToBytes + Protocol,
    ) -> Result<(), crate::net::Error> {
        self.send(message).await?;
        self.flush().await?;

        Ok(())
    }

    pub async fn send_flush_many(
        &mut self,
        messages: Vec<impl ToBytes + Protocol>,
    ) -> Result<(), crate::net::Error> {
        let len = messages.len();
        for (i, message) in messages.into_iter().enumerate() {
            if i == len - 1 {
                self.send_flush(message).await?;
            } else {
                self.send(message).await?;
            }
        }

        Ok(())
    }

    /// Read a message from the stream.
    ///
    /// # Performance
    ///
    /// The stream is buffered, so this is quite fast. The pooler will perform exactly
    /// one memory allocation per protocol message. It can be optimized to re-use an existing
    /// buffer but it's not worth the complexity.
    pub async fn read(&mut self) -> Result<Message, crate::net::Error> {
        let code = self.read_u8().await?;
        let len = self.read_i32().await?;

        let mut bytes = BytesMut::new();

        bytes.put_u8(code);
        bytes.put_i32(len);

        bytes.resize(len as usize + 1, 0); // self + 1 byte for the message code

        self.read_exact(&mut bytes[5..]).await?;

        let message = Message::new(bytes.freeze());

        debug!("📡 => {}", message.code());

        Ok(message)
    }

    /// Get the wrapped TCP stream back.
    pub(crate) fn take(self) -> Result<TcpStream, crate::net::Error> {
        match self {
            Self::Plain(stream) => Ok(stream.into_inner()),
            _ => Err(crate::net::Error::UnexpectedTlsRequest),
        }
    }
}
