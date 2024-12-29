//! Frontend client.
//!
use tokio::net::TcpStream;

use super::Error;
use crate::net::Connection;

/// Frontend client.
#[allow(dead_code)]
pub struct Client {
    connection: Connection,
}

impl Client {
    pub fn new(stream: TcpStream) -> Result<Self, Error> {
        todo!()
    }
}
