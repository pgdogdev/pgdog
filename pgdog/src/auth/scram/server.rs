//! SCRAM-SHA-256 server.

use crate::frontend::Error;
use crate::net::messages::*;
use crate::net::Stream;

use scram::server::ClientFinal;
use tracing::error;

use rand::Rng;
use scram::{
    hash_password, AuthenticationProvider, AuthenticationStatus, PasswordInfo, ScramServer,
};
use std::num::NonZeroU32;

enum Provider {
    Plain(UserPassword),
    Hashed(HashedPassword),
}

/// Derive the SCRAM-SHA-256 auth
/// from a plain text password.
#[derive(Clone)]
pub struct UserPassword {
    password: String,
}

/// Used a prehashed password obtained from
/// pg_shadow. This allows operators not to store
/// passwords in plain text in the config.
///
/// TODO: Doesn't work yet. I'm not sure how to actually
/// implement this.
#[derive(Clone)]
pub struct HashedPassword {
    hash: String,
}

enum Scram {
    Plain(ScramServer<UserPassword>),
    Hashed(ScramServer<HashedPassword>),
}

enum ScramFinal<'a> {
    Plain(ClientFinal<'a, UserPassword>),
    Hashed(ClientFinal<'a, HashedPassword>),
}

use base64::prelude::*;

impl AuthenticationProvider for UserPassword {
    fn get_password_for(&self, _user: &str) -> Option<PasswordInfo> {
        // TODO: This is slow. We should move it to its own thread pool.
        let iterations = 4096;
        let salt = rand::thread_rng().gen::<[u8; 16]>().to_vec();
        let hash = hash_password(&self.password, NonZeroU32::new(iterations).unwrap(), &salt);
        Some(PasswordInfo::new(hash.to_vec(), iterations as u16, salt))
    }
}

impl AuthenticationProvider for HashedPassword {
    fn get_password_for(&self, _user: &str) -> Option<PasswordInfo> {
        let mut parts = self.hash.split("$");
        if let Some(algo) = parts.next() {
            if algo != "SCRAM-SHA-256" {
                return None;
            }
        } else {
            return None;
        }

        let (mut salt, mut iter) = (None, None);
        if let Some(iter_salt) = parts.next() {
            let mut split = iter_salt.split(":");
            let maybe_iter = split.next().map(|iter| iter.parse::<u16>());
            let maybe_salt = split.next().map(|salt| BASE64_STANDARD.decode(salt));

            if let Some(Ok(num)) = maybe_iter {
                iter = Some(num);
            }

            if let Some(Ok(s)) = maybe_salt {
                salt = Some(s);
            }
        };

        let hashes = parts.next().map(|hashes| hashes.split(":"));

        if let Some(hashes) = hashes {
            if let Some(last) = hashes.last() {
                if let Ok(hash) = BASE64_STANDARD.decode(last) {
                    if let Some(iter) = iter {
                        if let Some(salt) = salt {
                            return Some(PasswordInfo::new(hash, iter, salt));
                        }
                    }
                }
            }
        }

        None
    }
}

/// SCRAM-SHA-256 server that handles
/// authenticating clients.
pub struct Server {
    provider: Provider,
    client_response: String,
}

impl Server {
    /// Create new SCRAM server.
    pub fn new(password: &str) -> Self {
        Self {
            provider: Provider::Plain(UserPassword {
                password: password.to_owned(),
            }),
            client_response: String::new(),
        }
    }

    pub fn hashed(hash: &str) -> Self {
        Self {
            provider: Provider::Hashed(HashedPassword {
                hash: hash.to_owned(),
            }),
            client_response: String::new(),
        }
    }

    /// Handle authentication.
    pub async fn handle(mut self, stream: &mut Stream) -> Result<bool, Error> {
        let scram = match self.provider {
            Provider::Plain(plain) => Scram::Plain(ScramServer::new(plain)),
            Provider::Hashed(hashed) => Scram::Hashed(ScramServer::new(hashed)),
        };

        let mut scram_client = None;

        loop {
            let message = stream.read().await?;
            match message.code() {
                'p' => {
                    let password = Password::from_bytes(message.to_bytes()?)?;

                    match password {
                        Password::SASLInitialResponse { response, .. } => {
                            self.client_response = response;
                            let reply = match scram {
                                Scram::Plain(ref plain) => {
                                    let server =
                                        plain.handle_client_first(&self.client_response)?;
                                    let (client, reply) = server.server_first();
                                    scram_client = Some(ScramFinal::Plain(client));
                                    reply
                                }
                                Scram::Hashed(ref hashed) => {
                                    let server =
                                        hashed.handle_client_first(&self.client_response)?;
                                    let (client, reply) = server.server_first();
                                    scram_client = Some(ScramFinal::Hashed(client));
                                    reply
                                }
                            };
                            let reply = Authentication::SaslContinue(reply);
                            stream.send_flush(&reply).await?;
                        }

                        Password::PasswordMessage { response } => {
                            if let Some(scram_client) = scram_client {
                                let server_final = match scram_client {
                                    ScramFinal::Plain(plain) => {
                                        plain.handle_client_final(&response)?
                                    }
                                    ScramFinal::Hashed(hashed) => {
                                        hashed.handle_client_final(&response)?
                                    }
                                };
                                let (status, reply) = server_final.server_final();

                                match status {
                                    AuthenticationStatus::Authenticated => {
                                        stream.send(&Authentication::SaslFinal(reply)).await?;
                                        return Ok(true);
                                    }

                                    _ => return Ok(false),
                                }
                            }
                        }
                    }
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_hashed_password() {
        let hash = "SCRAM-SHA-256$4096:lApbvrTR0W7WOZLcVrbz0A==$O+AwRnblFCJwEezpaozQfC6iKmbJFHQ7+0WZBsR+hFU=:wWjPizZvFjc5jmIkdN/EsuLGz/9FMjOhJ7IHxZI8eqE="
            .to_string();
        let hashed = HashedPassword { hash };
        let info = hashed.get_password_for("user");
        assert!(info.is_some());
    }
}
