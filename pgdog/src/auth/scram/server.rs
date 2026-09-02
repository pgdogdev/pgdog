//! SCRAM-SHA-256 server.

use crate::frontend::Error;
use crate::net::Stream;
use crate::net::messages::*;

use pgdog_config::users::PasswordKind;
use scram::server::ClientFinal;
use tracing::error;

use rand::Rng;
use scram::{
    AuthenticationProvider, AuthenticationStatus, PasswordInfo, ScramServer, hash_password,
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
pub(crate) struct UserPassword {
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
pub(crate) struct HashedPassword {
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
pub(crate) struct Server {
    provider: Provider,
}

impl Server {
    /// Create new SCRAM server. Any of the given plain text passwords will be
    /// accepted.
    pub(crate) fn new(passwords: &[PasswordKind]) -> Self {
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

    /// Read the next password message from the client, ignoring error
    /// responses by logging them.
    async fn read_password(stream: &mut Stream) -> Result<Option<Password>, Error> {
        let message = stream.read().await?;
        match message.code() {
            'p' => Ok(Some(Password::from_bytes(message.to_bytes())?)),
            'E' => {
                let err = ErrorResponse::from_bytes(message.to_bytes())?;
                error!("{}", err);
                Ok(None)
            }
            c => Err(Error::UnexpectedMessage(c)),
        }
    }

    fn scram_server<P: AuthenticationProvider>(
        provider: P,
        plus: bool,
        cbind: Option<Vec<u8>>,
    ) -> Result<ScramServer<P>, Error> {
        match (plus, cbind) {
            (true, Some(data)) => Ok(ScramServer::new_with_channel_binding(
                provider,
                "tls-server-end-point".to_string(),
                data,
            )),
            (true, None) => Err(Error::UnexpectedMessage('p')),
            (false, _) => Ok(ScramServer::new(provider)),
        }
    }

    /// Handle authentication.
    pub(crate) async fn handle(self, stream: &mut Stream) -> Result<bool, Error> {
        // SASLInitialResponse / client-first phase.
        let (mechanism, client_response) = match Self::read_password(stream).await? {
            Some(Password::SASLInitialResponse { name, response }) => (name, response),
            Some(_) => return Ok(false),
            None => return Ok(false),
        };

        let plus = mechanism == Authentication::SCRAM_SHA_256_PLUS;
        if plus && stream.tls_server_end_point().is_none() {
            return Ok(false);
        }
        if !plus && mechanism != Authentication::SCRAM_SHA_256 {
            return Ok(false);
        }

        let cbind = stream.tls_server_end_point().map(ToOwned::to_owned);
        if cbind.is_some() && client_response.starts_with("y,") {
            return Ok(false);
        }

        let scram = match self.provider {
            Provider::Plain(plain) => Scram::Plain(Self::scram_server(plain, plus, cbind)?),
            Provider::Hashed(hashed) => Scram::Hashed(Self::scram_server(hashed, plus, cbind)?),
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

    fn scram_plus_login(password: &str, hash: &str, cb_data: &[u8]) -> AuthenticationStatus {
        let provider = HashedPassword {
            hash: hash.to_string(),
        };
        let scram_server = Server::scram_server(provider, true, Some(cb_data.to_vec()))
            .expect("PLUS server requires cbind");
        let scram_client = scram::ScramClient::new_with_channel_binding(
            "user",
            password,
            None,
            "tls-server-end-point",
            cb_data.to_vec(),
        );

        let (scram_client, client_first) = scram_client.client_first();
        let server_first_state = scram_server
            .handle_client_first(&client_first)
            .expect("server handle client first");
        let (server_client_final, server_first_msg) = server_first_state.server_first();
        let scram_client = scram_client
            .handle_server_first(&server_first_msg)
            .expect("client handle server first");
        let (scram_client, client_final) = scram_client.client_final();
        let server_final = server_client_final
            .handle_client_final(&client_final)
            .expect("server handle client final");
        let (status, server_final_msg) = server_final.server_final();

        if status == AuthenticationStatus::Authenticated {
            scram_client
                .handle_server_final(&server_final_msg)
                .expect("client verify server final");
        }

        status
    }

    #[test]
    fn hashed_scram_plus_accepts_correct_password() {
        let cb_data = b"tls-server-end-point-bytes";
        assert_eq!(
            scram_plus_login("pgdog", SCRAM_HASH, cb_data),
            AuthenticationStatus::Authenticated,
        );
    }

    #[test]
    fn hashed_scram_plus_rejects_wrong_password() {
        let cb_data = b"tls-server-end-point-bytes";
        assert_eq!(
            scram_plus_login("wrong", SCRAM_HASH, cb_data),
            AuthenticationStatus::NotAuthenticated,
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

    #[test]
    fn scram_server_plus_without_cbind_errors() {
        let provider = HashedPassword {
            hash: SCRAM_HASH.to_string(),
        };
        assert!(matches!(
            Server::scram_server(provider, true, None),
            Err(Error::UnexpectedMessage('p'))
        ));
    }

    async fn connected_pair() -> (Stream, Stream) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move { tokio::net::TcpStream::connect(addr).await.unwrap() });
        let (ours, _) = listener.accept().await.unwrap();
        (
            Stream::plain(ours, 4096),
            Stream::plain(peer.await.unwrap(), 4096),
        )
    }

    fn hashed_server() -> Server {
        Server::new(&[PasswordKind::Hashed(SCRAM_HASH.to_string())])
    }

    async fn read_auth(stream: &mut Stream) -> Authentication {
        let message = stream.read().await.expect("auth message");
        Authentication::from_bytes(message.to_bytes()).expect("parse auth")
    }

    fn sasl_continue(auth: Authentication) -> Option<String> {
        match auth {
            Authentication::SaslContinue(data) => Some(data),
            _ => None,
        }
    }

    fn spawn_handle(
        mut stream: Stream,
        cbind: &[u8],
    ) -> tokio::task::JoinHandle<Result<bool, Error>> {
        stream.set_tls_server_end_point(cbind.to_vec());
        let server = hashed_server();
        tokio::spawn(async move { server.handle(&mut stream).await })
    }

    #[tokio::test]
    async fn handle_plus_accepts_correct_password() {
        let cb_data = b"tls-server-end-point-bytes";
        let (server_stream, mut client_stream) = connected_pair().await;
        let task = spawn_handle(server_stream, cb_data);

        let scram_client = scram::ScramClient::new_with_channel_binding(
            "user",
            "pgdog",
            None,
            "tls-server-end-point",
            cb_data.to_vec(),
        );
        let (scram_client, client_first) = scram_client.client_first();
        client_stream
            .send_flush(&Password::SASLInitialResponse {
                name: Authentication::SCRAM_SHA_256_PLUS.to_string(),
                response: client_first,
            })
            .await
            .unwrap();

        let data = sasl_continue(read_auth(&mut client_stream).await).expect("SaslContinue");
        let scram_client = scram_client
            .handle_server_first(&data)
            .expect("server first");
        let (_scram_client, client_final) = scram_client.client_final();
        client_stream
            .send_flush(&Password::PasswordMessage {
                response: client_final,
            })
            .await
            .unwrap();

        let ok = tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .expect("PLUS handshake timed out")
            .expect("join")
            .expect("handle");
        assert!(ok);
    }

    #[tokio::test]
    async fn handle_plus_without_cbind_refuses() {
        let (mut server_stream, mut client_stream) = connected_pair().await;
        client_stream
            .send_flush(&Password::SASLInitialResponse {
                name: Authentication::SCRAM_SHA_256_PLUS.to_string(),
                response: "p=tls-server-end-point,,n=user,r=nonce".to_string(),
            })
            .await
            .unwrap();

        let ok = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            hashed_server().handle(&mut server_stream),
        )
        .await
        .expect("PLUS without cbind must fail without waiting for a proof")
        .expect("handle");
        assert!(!ok);
    }

    #[tokio::test]
    async fn handle_unknown_mechanism_refuses() {
        let (mut server_stream, mut client_stream) = connected_pair().await;
        server_stream.set_tls_server_end_point(b"tls-server-end-point-bytes".to_vec());

        client_stream
            .send_flush(&Password::SASLInitialResponse {
                name: "SCRAM-SHA-1".to_string(),
                response: "n,,n=user,r=nonce".to_string(),
            })
            .await
            .unwrap();

        let ok = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            hashed_server().handle(&mut server_stream),
        )
        .await
        .expect("unknown mechanism must fail without waiting for a proof")
        .expect("handle");
        assert!(!ok);
    }

    #[tokio::test]
    async fn handle_rejects_y_when_cbind_present() {
        let (mut server_stream, mut client_stream) = connected_pair().await;
        server_stream.set_tls_server_end_point(b"tls-server-end-point-bytes".to_vec());

        client_stream
            .send_flush(&Password::SASLInitialResponse {
                name: Authentication::SCRAM_SHA_256.to_string(),
                response: "y,,n=user,r=nonce".to_string(),
            })
            .await
            .unwrap();

        let ok = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            hashed_server().handle(&mut server_stream),
        )
        .await
        .expect("y downgrade must fail without waiting for a proof")
        .expect("handle");
        assert!(!ok);
    }

    #[tokio::test]
    async fn handle_sha256_n_succeeds_with_cbind() {
        let cb_data = b"tls-server-end-point-bytes";
        let (server_stream, mut client_stream) = connected_pair().await;
        let task = spawn_handle(server_stream, cb_data);

        let mut client = Client::new("user", "pgdog");
        let client_first = client.first().expect("client first");
        assert!(
            client_first.starts_with("n,,"),
            "non-PLUS client first must use GS2 n: {client_first}"
        );

        client_stream
            .send_flush(&Password::SASLInitialResponse {
                name: Authentication::SCRAM_SHA_256.to_string(),
                response: client_first,
            })
            .await
            .unwrap();

        let data = sasl_continue(read_auth(&mut client_stream).await).expect("SaslContinue");
        client.server_first(&data).expect("server first");
        client_stream
            .send_flush(&Password::PasswordMessage {
                response: client.last().expect("client final"),
            })
            .await
            .unwrap();

        let ok = tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .expect("n handshake timed out")
            .expect("join")
            .expect("handle");
        assert!(ok);
    }

    #[test]
    fn sasl_continue_none_unless_continue() {
        assert_eq!(sasl_continue(Authentication::Ok), None);
        assert_eq!(
            sasl_continue(Authentication::SaslContinue("r=nonce".into())).as_deref(),
            Some("r=nonce")
        );
    }

    #[tokio::test]
    async fn handle_rejects_non_sasl_initial() {
        let (mut server_stream, mut client_stream) = connected_pair().await;
        client_stream
            .send_flush(&Password::new_password("pgdog"))
            .await
            .unwrap();

        let ok = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            hashed_server().handle(&mut server_stream),
        )
        .await
        .expect("non-SASL initial must fail without waiting for a proof")
        .expect("handle");
        assert!(!ok);
    }

    #[tokio::test]
    async fn handle_rejects_error_response() {
        let (mut server_stream, mut client_stream) = connected_pair().await;
        client_stream
            .send_flush(&ErrorResponse::syntax("client aborted"))
            .await
            .unwrap();

        let ok = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            hashed_server().handle(&mut server_stream),
        )
        .await
        .expect("ErrorResponse must fail without waiting for a proof")
        .expect("handle");
        assert!(!ok);
    }

    #[tokio::test]
    async fn handle_plus_rejects_wrong_password() {
        let cb_data = b"tls-server-end-point-bytes";
        let (server_stream, mut client_stream) = connected_pair().await;
        let task = spawn_handle(server_stream, cb_data);

        let scram_client = scram::ScramClient::new_with_channel_binding(
            "user",
            "wrong",
            None,
            "tls-server-end-point",
            cb_data.to_vec(),
        );
        let (scram_client, client_first) = scram_client.client_first();
        client_stream
            .send_flush(&Password::SASLInitialResponse {
                name: Authentication::SCRAM_SHA_256_PLUS.to_string(),
                response: client_first,
            })
            .await
            .unwrap();

        let data = sasl_continue(read_auth(&mut client_stream).await).expect("SaslContinue");
        let scram_client = scram_client
            .handle_server_first(&data)
            .expect("server first");
        let (_scram_client, client_final) = scram_client.client_final();
        client_stream
            .send_flush(&Password::PasswordMessage {
                response: client_final,
            })
            .await
            .unwrap();

        let ok = tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .expect("wrong-password PLUS handshake timed out")
            .expect("join")
            .expect("handle");
        assert!(!ok);
    }

    #[tokio::test]
    async fn handle_plain_plus_accepts_correct_password() {
        let cb_data = b"tls-server-end-point-bytes";
        let (mut server_stream, mut client_stream) = connected_pair().await;
        server_stream.set_tls_server_end_point(cb_data.to_vec());
        let server = Server::new(&[PasswordKind::Plain("pgdog".to_string())]);
        let task = tokio::spawn(async move { server.handle(&mut server_stream).await });

        let scram_client = scram::ScramClient::new_with_channel_binding(
            "user",
            "pgdog",
            None,
            "tls-server-end-point",
            cb_data.to_vec(),
        );
        let (scram_client, client_first) = scram_client.client_first();
        client_stream
            .send_flush(&Password::SASLInitialResponse {
                name: Authentication::SCRAM_SHA_256_PLUS.to_string(),
                response: client_first,
            })
            .await
            .unwrap();

        let data = sasl_continue(read_auth(&mut client_stream).await).expect("SaslContinue");
        let scram_client = scram_client
            .handle_server_first(&data)
            .expect("server first");
        let (_scram_client, client_final) = scram_client.client_final();
        client_stream
            .send_flush(&Password::PasswordMessage {
                response: client_final,
            })
            .await
            .unwrap();

        let ok = tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .expect("plain PLUS handshake timed out")
            .expect("join")
            .expect("handle");
        assert!(ok);
    }

    #[tokio::test]
    async fn handle_rejects_sasl_initial_as_client_final() {
        let cb_data = b"tls-server-end-point-bytes";
        let (server_stream, mut client_stream) = connected_pair().await;
        let task = spawn_handle(server_stream, cb_data);

        let mut client = Client::new("user", "pgdog");
        client_stream
            .send_flush(&Password::SASLInitialResponse {
                name: Authentication::SCRAM_SHA_256.to_string(),
                response: client.first().expect("client first"),
            })
            .await
            .unwrap();

        let _data = sasl_continue(read_auth(&mut client_stream).await).expect("SaslContinue");
        client_stream
            .send_flush(&Password::SASLInitialResponse {
                name: Authentication::SCRAM_SHA_256.to_string(),
                response: "n,,n=user,r=nonce".to_string(),
            })
            .await
            .unwrap();

        let ok = tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .expect("client-final SASLInitial must fail without hanging")
            .expect("join")
            .expect("handle");
        assert!(!ok);
    }
}
