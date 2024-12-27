//! Handle TCP connection.
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};

use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::spawn;

use super::Error;
use crate::net::Stream;

#[derive(Debug)]
pub enum Message {
    Bytes(Bytes),
    Flush,
}

/// Client connection.
#[allow(dead_code)]
pub struct Connection {
    stream: Stream,
    peer_addr: SocketAddr,
}

impl Connection {
    /// Create new client connection from a network connection.
    ///
    /// # Arguments
    ///
    /// * `stream`: TCP connection socket.
    ///
    pub fn new(stream: TcpStream) -> Result<Self, Error> {
        let peer_addr = stream.peer_addr()?;

        Ok(Self {
            stream: Stream::Plain(stream),
            peer_addr,
        })
    }
}

impl Deref for Connection {
    type Target = Stream;

    fn deref(&self) -> &Self::Target {
        &self.stream
    }
}

impl DerefMut for Connection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.stream
    }
}
