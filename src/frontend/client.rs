//! Frontend client.
//!

use super::Error;
use crate::net::{Connection, Stream};

/// Frontend client.
#[allow(dead_code)]
pub struct Client {
    connection: Connection,
}

impl Client {
    pub fn new(stream: Stream) -> Result<Self, Error> {
        let connection = Connection::new(stream)?;

        Ok(Self { connection })
    }
}
