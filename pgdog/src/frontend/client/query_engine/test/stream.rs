use crate::net::{stream::PeerAddr, Error, ErrorResponse, Message, Protocol, ReadyForQuery};

/// Fake TCP stream that allows us to see what
/// messages the server sent.
#[derive(Default, Debug)]
pub struct Stream {
    /// Messages received from server.
    pub messages: Vec<Message>,
    /// Was the socket flushed?
    pub flushed: bool,
}

impl Stream {
    /// Send one message.
    pub async fn send(&mut self, message: &impl Protocol) -> Result<usize, Error> {
        let message = message.message()?.backend();
        let len = message.len();
        self.messages.push(message);
        self.flushed = false;
        Ok(len)
    }

    /// Send multiple messages.
    pub async fn send_many(&mut self, messages: &[impl Protocol]) -> Result<usize, Error> {
        let mut len = 0;
        for message in messages {
            len += self.send(message).await?;
        }
        self.flushed = true;

        Ok(len)
    }

    /// Send message and flush stream.
    pub async fn send_flush(
        &mut self,
        message: &impl Protocol,
    ) -> Result<usize, crate::net::Error> {
        let len = self.send(message).await?;
        self.flushed = true;

        Ok(len)
    }

    /// Send error.
    pub async fn error(&mut self, error: ErrorResponse, in_transaction: bool) -> Result<(), Error> {
        self.send(&error).await?;
        self.send_flush(&if in_transaction {
            ReadyForQuery::error()
        } else {
            ReadyForQuery::idle()
        })
        .await?;

        Ok(())
    }

    /// Get peer address.
    pub fn peer_addr(&self) -> PeerAddr {
        PeerAddr::default()
    }
}
