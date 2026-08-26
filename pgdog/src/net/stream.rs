//! Network socket wrapper allowing us to treat secure, plain and UNIX
//! connections the same across the code.
use bytes::{BufMut, BytesMut};
use futures::FutureExt;
use pin_project::pin_project;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufStream, ReadBuf};
use tokio::net::TcpStream;
use tracing::trace;

use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use std::ops::Deref;
use std::pin::Pin;
use std::task::Context;

use super::messages::{ErrorResponse, Message, Protocol, ReadyForQuery};

/// Inner stream types.
#[pin_project(project = StreamInnerProjection)]
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum StreamInner {
    Plain(#[pin] BufStream<TcpStream>),
    Tls(#[pin] BufStream<tokio_rustls::TlsStream<TcpStream>>),
    DevNull,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Liveness {
    Clean,
    DataPending,
    Closed,
}

/// A network socket.
#[pin_project]
#[derive(Debug)]
pub(crate) struct Stream {
    #[pin]
    inner: StreamInner,
    io_in_progress: bool,
    capacity: usize,
    tls_identity: Option<String>,
    tls_client_certificate: bool,
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
    /// Memory used by the stream buffers.
    pub(crate) fn memory_usage(&self) -> usize {
        self.capacity * 2
    }

    /// Wrap an unencrypted TCP stream.
    pub(crate) fn plain(stream: TcpStream, capacity: usize) -> Self {
        Self {
            inner: StreamInner::Plain(BufStream::with_capacity(capacity, capacity, stream)),
            io_in_progress: false,
            capacity,
            tls_identity: None,
            tls_client_certificate: false,
        }
    }

    /// Wrap an encrypted TCP stream.
    pub(crate) fn tls(
        stream: tokio_rustls::TlsStream<TcpStream>,
        capacity: usize,
        tls_identity: Option<String>,
        tls_client_certificate: bool,
    ) -> Self {
        Self {
            inner: StreamInner::Tls(BufStream::with_capacity(capacity, capacity, stream)),
            io_in_progress: false,
            capacity,
            tls_identity,
            tls_client_certificate,
        }
    }

    /// Create a dev null stream that discards all data.
    pub(crate) fn dev_null() -> Self {
        Self {
            inner: StreamInner::DevNull,
            io_in_progress: false,
            capacity: 0,
            tls_identity: None,
            tls_client_certificate: false,
        }
    }

    /// Get the hostname identity (SAN dNSName, falling back to Subject CN)
    /// from the client's TLS certificate, if any.
    pub(crate) fn tls_identity(&self) -> Option<&str> {
        self.tls_identity.as_deref()
    }

    /// The client presented a TLS certificate, even one we can't name.
    pub(crate) fn tls_client_certificate(&self) -> bool {
        self.tls_client_certificate
    }

    /// This is a TLS stream.
    pub(crate) fn is_tls(&self) -> bool {
        matches!(self.inner, StreamInner::Tls(_))
    }

    /// Get peer address if any. We're not using UNIX sockets (yet)
    /// so the peer address should always be available.
    pub(crate) fn peer_addr(&self) -> PeerAddr {
        match &self.inner {
            StreamInner::Plain(stream) => stream.get_ref().peer_addr().ok().into(),
            StreamInner::Tls(stream) => stream.get_ref().get_ref().0.peer_addr().ok().into(),
            StreamInner::DevNull => PeerAddr { addr: None },
        }
    }

    pub(crate) fn liveness(&mut self) -> Liveness {
        let mut buf = [0u8; 1];
        let peeked = match &mut self.inner {
            StreamInner::Plain(plain) => plain.get_mut().peek(&mut buf).now_or_never(),
            StreamInner::Tls(tls) => tls.get_mut().get_mut().0.peek(&mut buf).now_or_never(),
            StreamInner::DevNull => return Liveness::Clean,
        };

        match peeked {
            None => Liveness::Clean,
            Some(Ok(0)) => Liveness::Closed,
            Some(Ok(_)) => Liveness::DataPending,
            Some(Err(_)) => Liveness::Closed,
        }
    }

    /// Get the current io_in_progress state.
    pub(crate) fn io_in_progress(&self) -> bool {
        self.io_in_progress
    }

    /// Send data via the stream.
    ///
    /// # Performance
    ///
    /// This is fast because the stream is buffered. Make sure to call [`Stream::send_flush`]
    /// for the last message in the exchange.
    pub(crate) async fn send(
        &mut self,
        message: &impl Protocol,
    ) -> Result<usize, crate::net::Error> {
        self.io_in_progress = true;
        let result = async {
            let bytes = message.to_bytes();

            match &mut self.inner {
                StreamInner::Plain(stream) => eof(stream.write_all(&bytes).await)?,
                StreamInner::Tls(stream) => eof(stream.write_all(&bytes).await)?,
                StreamInner::DevNull => (),
            }

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
    pub(crate) async fn send_flush(
        &mut self,
        message: &impl Protocol,
    ) -> Result<usize, crate::net::Error> {
        let sent = self.send(message).await?;
        eof(self.flush().await)?;
        trace!("😳");

        Ok(sent)
    }

    /// Send multiple messages and flush the buffer.
    pub(crate) async fn send_many(
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
    pub(crate) async fn read(&mut self) -> Result<Message, crate::net::Error> {
        let mut buf = BytesMut::with_capacity(5);
        self.read_buf(&mut buf).await
    }

    /// Read data into a buffer, avoiding unnecessary allocations.
    pub(crate) async fn read_buf(
        &mut self,
        bytes: &mut BytesMut,
    ) -> Result<Message, crate::net::Error> {
        let result = async {
            let code = eof(self.read_u8().await)?;
            self.io_in_progress = true;
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
    pub(crate) async fn fatal(&mut self, error: ErrorResponse) -> Result<(), crate::net::Error> {
        self.send_flush(&error).await?;

        Ok(())
    }

    /// Send an error to the client and let them know we are ready
    /// for more queries.
    pub(crate) async fn error(
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

pub(crate) fn eof<T>(result: std::io::Result<T>) -> Result<T, crate::net::Error> {
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
pub(crate) struct PeerAddr {
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
    use std::time::Duration;

    use super::*;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn test_io_in_progress_initially_false() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });

        let (server_stream, _) = listener.accept().await.unwrap();
        let stream = Stream::plain(server_stream, 4096);

        assert!(
            !stream.io_in_progress(),
            "io_in_progress should be false initially"
        );

        client.await.unwrap();
    }

    #[tokio::test]
    async fn test_plain_stream_has_no_client_certificate() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });

        let (server_stream, _) = listener.accept().await.unwrap();
        let stream = Stream::plain(server_stream, 4096);

        // No TLS handshake happened, so there is nothing to have presented.
        assert!(!stream.is_tls());
        assert!(!stream.tls_client_certificate());
        assert_eq!(stream.tls_identity(), None);

        client.await.unwrap();
    }

    #[test]
    fn test_dev_null_has_no_client_certificate() {
        let stream = Stream::dev_null();

        assert!(!stream.tls_client_certificate());
        assert_eq!(stream.tls_identity(), None);
    }

    async fn connected_pair() -> (Stream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });
        let (ours, _) = listener.accept().await.unwrap();

        (Stream::plain(ours, 4096), peer.await.unwrap())
    }

    #[tokio::test]
    async fn test_liveness_clean_when_peer_idle() {
        let (mut stream, _peer) = connected_pair().await;

        assert_eq!(stream.liveness(), Liveness::Clean);
    }

    #[tokio::test(start_paused = true)]
    async fn test_liveness_does_not_block_on_idle_socket() {
        let (mut stream, _peer) = connected_pair().await;

        let checked =
            tokio::time::timeout(Duration::from_secs(5), async { stream.liveness() }).await;

        assert_eq!(
            checked.expect("liveness() blocked on an idle socket"),
            Liveness::Clean
        );
    }

    #[tokio::test]
    async fn test_liveness_reports_unsolicited_data() {
        let (mut stream, mut peer) = connected_pair().await;

        peer.write_all(b"E").await.unwrap();
        peer.flush().await.unwrap();
        tokio::task::yield_now().await;

        assert_eq!(stream.liveness(), Liveness::DataPending);
    }

    #[tokio::test]
    async fn test_liveness_reports_closed_peer() {
        let (mut stream, peer) = connected_pair().await;

        drop(peer);
        tokio::task::yield_now().await;

        assert_eq!(stream.liveness(), Liveness::Closed);
    }

    #[tokio::test]
    async fn test_liveness_does_not_consume_pending_bytes() {
        let (mut stream, mut peer) = connected_pair().await;

        peer.write_all(b"hello").await.unwrap();
        peer.flush().await.unwrap();
        tokio::task::yield_now().await;

        assert_eq!(stream.liveness(), Liveness::DataPending);
        assert_eq!(stream.liveness(), Liveness::DataPending);

        let mut buf = [0u8; 5];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn test_liveness_dev_null_is_clean() {
        let mut stream = Stream::dev_null();

        assert_eq!(stream.liveness(), Liveness::Clean);
    }
}
