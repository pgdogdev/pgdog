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
}

impl Server {
    /// Create new SCRAM server. Any of the given plain text passwords will be
    /// accepted.
    pub fn new(passwords: &[String]) -> Self {
        let salt = rand::rng().random::<[u8; 16]>().to_vec();
        Self {
            provider: Provider::Plain(UserPassword {
                passwords: passwords.to_vec(),
                salt,
                iterations: 4096,
            }),
        }
    }

    /// Create a new SCRAM server using a prehashed `pg_shadow` style password.
    /// Only the first hash is used; prehashed passwords cannot share salts so
    /// multi-password verification is not supported in this mode.
    pub fn hashed(hashes: &[String]) -> Self {
        let hash = hashes.first().cloned().unwrap_or_default();
        Self {
            provider: Provider::Hashed(HashedPassword { hash }),
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
    use base64::engine::general_purpose::STANDARD;

    #[test]
    fn user_password_provider_generates_info() {
        let server = Server::new(&["secret".to_string()]);
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
        let iterations = std::num::NonZeroU32::new(4096).unwrap();
        let salt = b"testsalt";
        let hash = hash_password("secret", iterations, salt);
        let salt_b64 = STANDARD.encode(salt);
        let hash_b64 = STANDARD.encode(hash.as_ref());
        let scram_hash = format!("SCRAM-SHA-256${}:{salt_b64}:${hash_b64}", iterations.get());

        let provider = HashedPassword { hash: scram_hash };

        assert!(
            provider.get_password_for("user").is_some(),
            "hashed provider should produce password info"
        );
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
