//! SCRAM-SHA-256 client.

use super::Error;

use scram::{
    client::{ClientFinal, ServerFinal, ServerFirst},
    ScramClient,
};

enum State<'a> {
    Initial(ScramClient<'a>),
    First(ServerFirst<'a>),
    Final(ClientFinal),
    ServerFinal(ServerFinal),
}

/// SASL SCRAM client.
pub struct Client<'a> {
    state: Option<State<'a>>,
}

impl<'a> Client<'a> {
    /// Create new SCRAM client.
    pub fn new(user: &'a str, password: &'a str) -> Self {
        Self {
            state: Some(State::Initial(ScramClient::new(user, password, None))),
        }
    }

    /// Client first message.
    pub fn first(&mut self) -> Result<String, Error> {
        let (scram, client_first) = match self.state.take() {
            Some(State::Initial(scram)) => scram.client_first(),
            _ => return Err(Error::OutOfOrder),
        };
        self.state = Some(State::First(scram));
        Ok(client_first)
    }

    /// Handle server first message.
    pub fn server_first(&mut self, message: &str) -> Result<(), Error> {
        let scram = match self.state.take() {
            Some(State::First(scram)) => scram.handle_server_first(message)?,
            _ => return Err(Error::OutOfOrder),
        };
        self.state = Some(State::Final(scram));
        Ok(())
    }

    /// Client last message.
    pub fn last(&mut self) -> Result<String, Error> {
        let (scram, client_final) = match self.state.take() {
            Some(State::Final(scram)) => scram.client_final(),
            _ => return Err(Error::OutOfOrder),
        };
        self.state = Some(State::ServerFinal(scram));
        Ok(client_final)
    }

    /// Verify server last message.
    pub fn server_last(&mut self, message: &str) -> Result<(), Error> {
        match self.state.take() {
            Some(State::ServerFinal(scram)) => scram.handle_server_final(message)?,
            _ => return Err(Error::OutOfOrder),
        };
        Ok(())
    }
}
