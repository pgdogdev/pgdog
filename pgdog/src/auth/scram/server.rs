//! SCRAM-SHA-256 server.

use crate::frontend::Error;
use crate::net::messages::*;
use crate::net::Stream;

use tracing::error;

use rand::Rng;
use scram::{
    hash_password, AuthenticationProvider, AuthenticationStatus, PasswordInfo, ScramServer,
};
use std::num::NonZeroU32;

#[derive(Clone)]
struct UserPassword {
    password: String,
}

impl AuthenticationProvider for UserPassword {
    fn get_password_for(&self, _user: &str) -> Option<PasswordInfo> {
        let iterations = 4096;
        let salt = rand::thread_rng().gen::<[u8; 32]>().to_vec();
        let hash = hash_password(&self.password, NonZeroU32::new(iterations).unwrap(), &salt);
        Some(PasswordInfo::new(hash.to_vec(), iterations as u16, salt))
    }
}

/// SCRAM-SHA-256 server that handles
/// authenticating clients.
pub struct Server {
    provider: UserPassword,
    client_response: String,
}

impl Server {
    /// Create new SCRAM server.
    pub fn new(password: &str) -> Self {
        Self {
            provider: UserPassword {
                password: password.to_owned(),
            },
            client_response: String::new(),
        }
    }

    /// Handle authentication.
    pub async fn handle(mut self, stream: &mut Stream) -> Result<bool, Error> {
        let scram = ScramServer::new(self.provider);
        let mut scram_client = None;

        loop {
            let message = stream.read().await?;
            match message.code() {
                'p' => {
                    let password = Password::from_bytes(message.to_bytes()?)?;

                    match password {
                        Password::SASLInitialResponse { response, .. } => {
                            self.client_response = response;
                            let server = scram.handle_client_first(&self.client_response)?;
                            let (client, reply) = server.server_first();
                            let reply = Authentication::AuthenticationSASLContinue(reply);
                            stream.send_flush(reply).await?;
                            scram_client = Some(client);
                        }

                        Password::SASLResponse { response } => {
                            if let Some(scram_client) = scram_client.take() {
                                let server_final = scram_client.handle_client_final(&response)?;
                                let (status, reply) = server_final.server_final();

                                match status {
                                    AuthenticationStatus::Authenticated => {
                                        stream
                                            .send(Authentication::AuthenticationSASLFinal(reply))
                                            .await?;
                                        return Ok(true);
                                    }

                                    _ => return Ok(false),
                                }
                            }
                        }
                    }
                }

                'R' => {
                    let auth = Authentication::from_bytes(message.to_bytes()?)?;
                    println!("{:?}", auth);
                }

                'E' => {
                    let err = ErrorResponse::from_bytes(message.to_bytes()?)?;
                    error!("{}", err);
                    return Ok(false);
                }

                c => return Err(Error::UnexpectedMessage(c)),
            }
        }
    }
}
