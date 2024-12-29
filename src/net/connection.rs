//! Handle TCP connection.
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};

use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::net::{TcpSocket, TcpStream, ToSocketAddrs};
use tokio::select;
use tokio::sync::broadcast::{channel, Receiver, Sender};
use tokio::task::spawn;

use super::Error;
use crate::net::Stream;

#[derive(Debug, Clone)]
pub enum Message {
    Bytes(Bytes),
    Flush,
    Shutdown { error: bool },
}

impl Message {
    pub async fn read(stream: &mut (impl AsyncRead + Unpin + Send)) -> Result<Self, Error> {
        let code = stream.read_u8().await? as char;

        todo!()
    }
}

/// Client connection.
#[allow(dead_code)]
pub struct Connection {
    tx: Sender<Message>,
    rx: Receiver<Message>,
}

impl Connection {
    /// Create new client connection from a network connection.
    ///
    /// # Arguments
    ///
    /// * `stream`: TCP connection socket.
    ///
    pub fn new(stream: Stream) -> Result<Self, Error> {
        let (tx, rx) = channel::<Message>(4096);
        spawn(async move {
            // select! {}
        });
        todo!()
    }

    pub async fn server(addr: impl ToSocketAddrs) -> Result<Self, Error> {
        let mut stream = TcpStream::connect(addr).await?;

        todo!()
    }
}
