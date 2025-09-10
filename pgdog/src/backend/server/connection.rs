//! Server connection establishment and TLS handling.

use bytes::{BufMut, BytesMut};
use rustls_pki_types::ServerName;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tracing::{debug, error, info, warn};

use super::{Address, Error, PreparedStatements, Server, ServerOptions, Stats};
use crate::{
    auth::{md5, scram::Client},
    config::{config, PoolerMode, TlsVerifyMode},
    net::{
        messages::{
            hello::SslReply, Authentication, BackendKeyData, ErrorResponse, FromBytes,
            ParameterStatus, Password, Protocol, Startup, ToBytes,
        },
        parameter::Parameters,
        tls::connector_with_verify_mode,
        tweak, Parameter, Stream,
    },
    stats::memory::MemoryUsage,
};

impl Server {
    /// Create new PostgreSQL server connection.
    pub async fn connect(addr: &Address, options: ServerOptions) -> Result<Self, Error> {
        debug!("=> {}", addr);
        let stream = TcpStream::connect(addr.addr().await?).await?;
        tweak(&stream)?;

        let mut stream = Stream::plain(stream);

        let cfg = config();
        let tls_mode = cfg.config.general.tls_verify;

        // Only attempt TLS if not in Disabled mode
        if tls_mode != TlsVerifyMode::Disabled {
            debug!(
                "requesting TLS connection with verify mode: {:?} [{}]",
                tls_mode, addr,
            );

            // Request TLS.
            stream.write_all(&Startup::tls().to_bytes()?).await?;
            stream.flush().await?;

            let mut ssl = BytesMut::new();
            ssl.put_u8(stream.read_u8().await?);
            let ssl = SslReply::from_bytes(ssl.freeze())?;

            if ssl == SslReply::Yes {
                debug!("server supports TLS, initiating TLS handshake [{}]", addr);

                let connector = connector_with_verify_mode(
                    tls_mode,
                    cfg.config.general.tls_server_ca_certificate.as_ref(),
                )?;
                let plain = stream.take()?;

                let server_name = ServerName::try_from(addr.host.clone())?;
                debug!("connecting with TLS to server name: {:?}", server_name);

                match connector.connect(server_name.clone(), plain).await {
                    Ok(tls_stream) => {
                        debug!("TLS handshake successful with {}", addr.host);
                        let cipher = tokio_rustls::TlsStream::Client(tls_stream);
                        stream = Stream::tls(cipher);
                    }
                    Err(e) => {
                        error!("TLS handshake failed with {:?} [{}]", e, addr);
                        return Err(Error::Io(std::io::Error::new(
                            std::io::ErrorKind::ConnectionRefused,
                            format!("TLS handshake failed: {}", e),
                        )));
                    }
                }
            } else if tls_mode == TlsVerifyMode::VerifyFull || tls_mode == TlsVerifyMode::VerifyCa {
                // If we require TLS but server doesn't support it, fail
                error!("server does not support TLS but it is required [{}]", addr,);
                return Err(Error::TlsRequired);
            } else {
                debug!(
                    "server does not support TLS, continuing without encryption [{}]",
                    addr
                );
            }
        } else {
            debug!(
                "TLS verification mode is None, skipping TLS entirely [{}]",
                addr
            );
        }

        stream
            .write_all(
                &Startup::new(&addr.user, &addr.database_name, options.params.clone())
                    .to_bytes()?,
            )
            .await?;
        stream.flush().await?;

        // Perform authentication.
        let mut scram = Client::new(&addr.user, &addr.password);
        loop {
            let message = stream.read().await?;

            match message.code() {
                'E' => {
                    let error = ErrorResponse::from_bytes(message.payload())?;
                    return Err(Error::ConnectionError(Box::new(error)));
                }
                'R' => {
                    let auth = Authentication::from_bytes(message.payload())?;

                    match auth {
                        Authentication::Ok => break,
                        Authentication::ClearTextPassword => {
                            let password = Password::new_password(&addr.password);
                            stream.send_flush(&password).await?;
                        }
                        Authentication::Sasl(_) => {
                            let initial = Password::sasl_initial(&scram.first()?);
                            stream.send_flush(&initial).await?;
                        }
                        Authentication::SaslContinue(data) => {
                            scram.server_first(&data)?;
                            let response = Password::PasswordMessage {
                                response: scram.last()?,
                            };
                            stream.send_flush(&response).await?;
                        }
                        Authentication::SaslFinal(data) => {
                            scram.server_last(&data)?;
                        }
                        Authentication::Md5(salt) => {
                            let client = md5::Client::new_salt(&addr.user, &addr.password, &salt)?;
                            stream.send_flush(&client.response()).await?;
                        }
                    }
                }

                code => return Err(Error::UnexpectedMessage(code)),
            }
        }

        let mut params = Vec::new();
        let mut key_data: Option<BackendKeyData> = None;

        loop {
            let message = stream.read().await?;

            match message.code() {
                // ReadyForQuery (B)
                'Z' => break,
                // ParameterStatus (B)
                'S' => {
                    let parameter = ParameterStatus::from_bytes(message.payload())?;
                    params.push(Parameter::from(parameter));
                }
                // BackendKeyData (B)
                'K' => {
                    key_data = Some(BackendKeyData::from_bytes(message.payload())?);
                }
                // ErrorResponse (B)
                'E' => {
                    return Err(Error::ConnectionError(Box::new(ErrorResponse::from_bytes(
                        message.to_bytes()?,
                    )?)));
                }
                // NoticeResponse (B)
                'N' => {
                    let notice =
                        crate::net::messages::NoticeResponse::from_bytes(message.payload())?;
                    warn!("{} [{}]", notice.message, addr);
                }

                code => return Err(Error::UnexpectedMessage(code)),
            }
        }

        let id = key_data.ok_or(Error::NoBackendKeyData)?;
        let params: Parameters = params.into();

        info!("new server connection [{}]", addr);

        let mut server = Server {
            addr: addr.clone(),
            stream: Some(stream),
            id,
            stats: Stats::connect(id, addr, &params),
            replication_mode: options.replication_mode(),
            startup_options: options,
            params,
            changed_params: Parameters::default(),
            client_params: Parameters::default(),
            prepared_statements: PreparedStatements::new(),
            dirty: false,
            streaming: false,
            schema_changed: false,
            sync_prepared: false,
            in_transaction: false,
            re_synced: false,
            pooler_mode: PoolerMode::Transaction,
            stream_buffer: BytesMut::with_capacity(1024),
        };

        server.stats.memory_used(server.memory_usage()); // Stream capacity.
        server.stats().update();

        Ok(server)
    }

    /// Request query cancellation for the given backend server identifier.
    pub async fn cancel(addr: &Address, id: &BackendKeyData) -> Result<(), Error> {
        let mut stream = TcpStream::connect(addr.addr().await?).await?;
        stream
            .write_all(
                &Startup::Cancel {
                    pid: id.pid,
                    secret: id.secret,
                }
                .to_bytes()?,
            )
            .await?;
        stream.flush().await?;

        Ok(())
    }

    /// Reconnect to the server with the same options.
    pub async fn reconnect(&self) -> Result<Server, Error> {
        Self::connect(&self.addr, self.startup_options.clone()).await
    }
}

#[cfg(test)]
pub mod test {
    // Connection tests will be moved here
}
