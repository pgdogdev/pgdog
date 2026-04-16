//! SCRAM-SHA-256 server.

use crate::frontend::Error;
use crate::net::messages::*;
use crate::net::Stream;

use pgdog_config::users::PasswordKind;
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

/// Derive the SCRAM-SHA-256 auth from one or more plain text passwords.
///
/// Multiple passwords share a single salt and iteration count so the SCRAM
/// `server-first-message` can be unambiguous. The server will accept a client
/// proof matching any of the configured passwords.
#[derive(Clone)]
pub struct UserPassword {
    passwords: Vec<String>,
    salt: Vec<u8>,
    iterations: u16,
}

/// Used a prehashed password obtained from
/// pg_shadow. This allows operators not to store
/// passwords in plain text in the config.
///
/// Note: prehashed passwords from `pg_shadow` come with their own salt and
/// iteration count baked in, so multi-password support is not possible here —
/// only the first hash is used.
#[derive(Clone)]
pub struct HashedPassword {
    pub(crate) hash: String,
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
        let iterations = NonZeroU32::new(self.iterations as u32).unwrap();
        let hashed_passwords = self
            .passwords
            .iter()
            .map(|password| hash_password(password, iterations, &self.salt).to_vec())
            .collect();
        Some(PasswordInfo::new_multi(
            hashed_passwords,
            self.iterations,
            self.salt.clone(),
        ))
    }
}

impl AuthenticationProvider for HashedPassword {
    fn get_password_for(&self, _user: &str) -> Option<PasswordInfo> {
        let mut parts = self.hash.split('$');

        if parts.next()? != "SCRAM-SHA-256" {
            return None;
        }

        let iter_salt = parts.next()?;
        let keys_part = parts.next()?;

        let mut is = iter_salt.split(':');
        let iterations: u16 = is.next()?.parse().ok()?;
        let salt = BASE64_STANDARD.decode(is.next()?).ok()?;

        let mut ks = keys_part.split(':');
        let stored_key = BASE64_STANDARD.decode(ks.next()?).ok()?;
        let server_key = BASE64_STANDARD.decode(ks.next()?).ok()?;

        Some(PasswordInfo::from_stored_keys(
            stored_key, server_key, iterations, salt,
        ))
    }
}

/// SCRAM-SHA-256 server that handles
/// authenticating clients.
pub struct Server {
    provider: Provider,
}

impl Server {
    /// Create new SCRAM server. Any of the given plain text passwords will be
    /// accepted.
    pub fn new(passwords: &[PasswordKind]) -> Self {
        let hash = passwords
            .iter()
            .find(|p| matches!(p, PasswordKind::Hashed(_)));
        if let Some(hash) = hash {
            return Self {
                provider: Provider::Hashed(HashedPassword {
                    hash: hash.to_string(),
                }),
            };
        }

        let salt = rand::rng().random::<[u8; 16]>().to_vec();
        Self {
            provider: Provider::Plain(UserPassword {
                passwords: passwords.iter().map(|s| s.to_string()).collect(),
                salt,
                iterations: 4096,
            }),
        }
    }

    /// Create a new SCRAM server using a prehashed `pg_shadow` style password.
    /// Only the first hash is used; prehashed passwords cannot share salts so
    /// multi-password verification is not supported in this mode.
    pub fn hashed(hash: &str) -> Self {
        Self {
            provider: Provider::Hashed(HashedPassword {
                hash: hash.to_string(),
            }),
        }
    }

    /// Read the next password message from the client, ignoring error
    /// responses by logging them.
    async fn read_password(stream: &mut Stream) -> Result<Option<Password>, Error> {
        let message = stream.read().await?;
        match message.code() {
            'p' => Ok(Some(Password::from_bytes(message.to_bytes()?)?)),
            'E' => {
                let err = ErrorResponse::from_bytes(message.to_bytes()?)?;
                error!("{}", err);
                Ok(None)
            }
            c => Err(Error::UnexpectedMessage(c)),
        }
    }

    /// Handle authentication.
    pub async fn handle(self, stream: &mut Stream) -> Result<bool, Error> {
        let scram = match self.provider {
            Provider::Plain(plain) => Scram::Plain(ScramServer::new(plain)),
            Provider::Hashed(hashed) => Scram::Hashed(ScramServer::new(hashed)),
        };

        // SASLInitialResponse / client-first phase.
        let client_response = match Self::read_password(stream).await? {
            Some(Password::SASLInitialResponse { response, .. }) => response,
            Some(_) => return Ok(false),
            None => return Ok(false),
        };

        let (scram_final, reply) = match &scram {
            Scram::Plain(plain) => {
                let server = plain.handle_client_first(&client_response)?;
                let (client, reply) = server.server_first();
                (ScramFinal::Plain(client), reply)
            }
            Scram::Hashed(hashed) => {
                let server = hashed.handle_client_first(&client_response)?;
                let (client, reply) = server.server_first();
                (ScramFinal::Hashed(client), reply)
            }
        };

        stream
            .send_flush(&Authentication::SaslContinue(reply))
            .await?;

        // Client-final phase.
        let response = match Self::read_password(stream).await? {
            Some(Password::PasswordMessage { response }) => response,
            Some(_) => return Ok(false),
            None => return Ok(false),
        };

        let server_final = match scram_final {
            ScramFinal::Plain(plain) => plain.handle_client_final(&response)?,
            ScramFinal::Hashed(hashed) => hashed.handle_client_final(&response)?,
        };

        let (status, reply) = server_final.server_final();
        if matches!(status, AuthenticationStatus::Authenticated) {
            stream.send(&Authentication::SaslFinal(reply)).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::scram::Client;
    use scram::AuthenticationStatus;

    const SCRAM_HASH: &str = "SCRAM-SHA-256$4096:B6lJyg12n6SawAu1kD9maA==$huWaU6t+WsvcS9ZrDvocZeYtlLJ60hdP46tjszFBbW0=:706OTwYyqH5WpfNpZdgt0gxuP5ff4DPUpHYu3F3w6TY=";

    #[test]
    fn user_password_provider_generates_info() {
        let server = Server::new(&[PasswordKind::Plain("secret".to_string())]);
        let provider = match server.provider {
            Provider::Plain(ref inner) => inner.clone(),
            _ => unreachable!(),
        };

        assert!(
            provider.get_password_for("user").is_some(),
            "plain provider should produce password info"
        );
    }

    #[test]
    fn hashed_password_provider_parses_scram_hash() {
        let provider = HashedPassword {
            hash: SCRAM_HASH.to_string(),
        };
        assert!(
            provider.get_password_for("user").is_some(),
            "hashed provider should produce password info"
        );
    }

    #[test]
    fn hashed_password_rejects_invalid_algo() {
        let hash = "SCRAM-SHA-1$4096:c2FsdA==$c3RvcmVka2V5:c2VydmVya2V5".to_string();
        let provider = HashedPassword { hash };
        assert!(provider.get_password_for("user").is_none());
    }

    /// Drive a full SCRAM handshake between the pgdog Client and a
    /// ScramServer<HashedPassword>, returning the authentication status.
    fn scram_login(user: &str, password: &str, hash: &str) -> AuthenticationStatus {
        let provider = HashedPassword {
            hash: hash.to_string(),
        };
        let scram_server = ScramServer::new(provider);
        let mut client = Client::new(user, password);

        let client_first = client.first().expect("client first");
        let server_first_state = scram_server
            .handle_client_first(&client_first)
            .expect("server handle client first");
        let (server_client_final, server_first_msg) = server_first_state.server_first();

        client
            .server_first(&server_first_msg)
            .expect("client handle server first");
        let client_final = client.last().expect("client final");

        let server_final = server_client_final
            .handle_client_final(&client_final)
            .expect("server handle client final");
        let (status, server_final_msg) = server_final.server_final();

        if status == AuthenticationStatus::Authenticated {
            client
                .server_last(&server_final_msg)
                .expect("client verify server final");
        }

        status
    }

    #[test]
    fn hashed_scram_accepts_correct_password() {
        assert_eq!(
            scram_login("user", "pgdog", SCRAM_HASH),
            AuthenticationStatus::Authenticated,
        );
    }

    #[test]
    fn hashed_scram_rejects_wrong_password() {
        assert_eq!(
            scram_login("user", "wrong", SCRAM_HASH),
            AuthenticationStatus::NotAuthenticated,
        );
    }

    #[test]
    fn hashed_scram_rejects_empty_password() {
        assert_eq!(
            scram_login("user", "", SCRAM_HASH),
            AuthenticationStatus::NotAuthenticated,
        );
    }

    #[test]
    fn generated_hash_accepts_correct_password() {
        let iterations = std::num::NonZeroU32::new(4096).unwrap();
        let salt = b"pgdog_test_salt!";
        let hash = crate::auth::scram::generate_hash("pgdog", iterations, salt);

        assert!(hash.starts_with("SCRAM-SHA-256$4096:"));
        assert_eq!(
            scram_login("user", "pgdog", &hash),
            AuthenticationStatus::Authenticated,
        );
    }

    #[test]
    fn generated_hash_rejects_wrong_password() {
        let iterations = std::num::NonZeroU32::new(4096).unwrap();
        let salt = b"pgdog_test_salt!";
        let hash = crate::auth::scram::generate_hash("pgdog", iterations, salt);

        assert_eq!(
            scram_login("user", "wrong", &hash),
            AuthenticationStatus::NotAuthenticated,
        );
    }
}
