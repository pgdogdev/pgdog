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

#[cfg(test)]
mod tests {
    use super::*;
    use scram::{
        hash_password, AuthenticationProvider, AuthenticationStatus, PasswordInfo, ScramServer,
    };
    use std::num::NonZeroU32;

    struct TestProvider {
        password: String,
    }

    impl AuthenticationProvider for TestProvider {
        fn get_password_for(&self, _user: &str) -> Option<PasswordInfo> {
            let iterations = NonZeroU32::new(4096).unwrap();
            let salt = b"testsalt".to_vec();
            let hash = hash_password(&self.password, iterations, &salt);
            Some(PasswordInfo::new(
                hash.to_vec(),
                iterations.get() as u16,
                salt,
            ))
        }
    }

    #[test]
    fn scram_client_full_handshake_succeeds() {
        let mut client = Client::new("user", "secret");
        let provider = TestProvider {
            password: "secret".into(),
        };
        let server = ScramServer::new(provider);

        let client_first = client.first().expect("client first message");
        let server = server
            .handle_client_first(&client_first)
            .expect("server handle client first");
        let (server_client_final, server_first) = server.server_first();

        client
            .server_first(&server_first)
            .expect("client handles server first");

        let client_final = client.last().expect("client final message");
        let server_final = server_client_final
            .handle_client_final(&client_final)
            .expect("server handles client final");
        let (status, server_final_msg) = server_final.server_final();
        assert_eq!(status, AuthenticationStatus::Authenticated);

        client
            .server_last(&server_final_msg)
            .expect("client validates server final");
    }

    #[test]
    fn scram_client_enforces_call_order() {
        let mut client = Client::new("user", "secret");
        let err = client
            .last()
            .expect_err("last without handshake should fail");
        matches!(err, Error::OutOfOrder)
            .then_some(())
            .expect("expected out-of-order error");
    }
}
