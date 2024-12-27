//! Network socket wrapper allowing us to treat secure, plain and UNIX
//! connections the same across the code.
use pin_project::pin_project;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

use std::io::Error;
use std::pin::Pin;
use std::task::Context;

/// A network socket.
#[pin_project(project = StreamProjection)]
pub enum Stream {
    Plain(#[pin] TcpStream),
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
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> std::task::Poll<Result<(), Error>> {
        let project = self.project();
        match project {
            StreamProjection::Plain(stream) => stream.poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> std::task::Poll<Result<(), Error>> {
        let project = self.project();
        match project {
            StreamProjection::Plain(stream) => stream.poll_shutdown(cx),
        }
    }
}
