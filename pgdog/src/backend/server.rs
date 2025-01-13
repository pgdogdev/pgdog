//! PostgreSQL serer connection.
use std::time::{Duration, Instant};

use bytes::{BufMut, BytesMut};
use rustls_pki_types::ServerName;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    spawn,
};
use tracing::{debug, info, trace};

use super::{pool::Address, Error};
use crate::net::{parameter::Parameters, tls::connector, Parameter, Stream};
use crate::state::State;
use crate::{
    auth::scram::Client,
    net::messages::{
        hello::SslReply, Authentication, BackendKeyData, ErrorResponse, FromBytes, Message,
        ParameterStatus, Password, Protocol, Query, ReadyForQuery, Startup, Terminate, ToBytes,
    },
    stats::ConnStats,
};

/// PostgreSQL server connection.
pub struct Server {
    addr: Address,
    stream: Option<Stream>,
    id: BackendKeyData,
    params: Parameters,
    state: State,
    created_at: Instant,
    last_used_at: Instant,
    last_healthcheck: Option<Instant>,
    stats: ConnStats,
}

impl Server {
    /// Create new PostgreSQL server connection.
    pub async fn connect(addr: &Address) -> Result<Self, Error> {
        debug!("=> {}", addr);
        let stream = TcpStream::connect(addr.to_string()).await?;

        // Disable the Nagle algorithm.
        stream.set_nodelay(true)?;

        let mut stream = Stream::plain(stream);

        // Request TLS.
        stream.write_all(&Startup::tls().to_bytes()?).await?;
        stream.flush().await?;

        let mut ssl = BytesMut::new();
        ssl.put_u8(stream.read_u8().await?);
        let ssl = SslReply::from_bytes(ssl.freeze())?;

        if ssl == SslReply::Yes {
            let connector = connector()?;
            let plain = stream.take()?;

            let server_name = ServerName::try_from(addr.host.clone())?;

            let cipher =
                tokio_rustls::TlsStream::Client(connector.connect(server_name, plain).await?);

            stream = Stream::tls(cipher);
        }

        stream
            .write_all(&Startup::new(&addr.user, &addr.database_name).to_bytes()?)
            .await?;
        stream.flush().await?;

        // Perform authentication.
        let mut scram = Client::new(&addr.user, &addr.password);
        loop {
            let message = stream.read().await?;

            match message.code() {
                'E' => {
                    let error = ErrorResponse::from_bytes(message.payload())?;
                    return Err(Error::ConnectionError(error));
                }
                'R' => {
                    let auth = Authentication::from_bytes(message.payload())?;

                    match auth {
                        Authentication::Ok => break,
                        Authentication::Sasl(_) => {
                            let initial = Password::sasl_initial(&scram.first()?);
                            stream.send_flush(initial).await?;
                        }
                        Authentication::SaslContinue(data) => {
                            scram.server_first(&data)?;
                            let response = Password::SASLResponse {
                                response: scram.last()?,
                            };
                            stream.send_flush(response).await?;
                        }
                        Authentication::SaslFinal(data) => {
                            scram.server_last(&data)?;
                        }
                    }
                }

                code => return Err(Error::UnexpectedMessage(code)),
            }
        }

        let mut params = Parameters::default();
        let mut key_data: Option<BackendKeyData> = None;

        loop {
            let message = stream.read().await?;

            match message.code() {
                // ReadyForQery (B)
                'Z' => break,
                // ParameterStatus (B)
                'S' => {
                    let parameter = ParameterStatus::from_bytes(message.payload())?;
                    params.push(Parameter {
                        name: parameter.name,
                        value: parameter.value,
                    });
                }
                // BackendKeyData (B)
                'K' => {
                    key_data = Some(BackendKeyData::from_bytes(message.payload())?);
                }

                code => return Err(Error::UnexpectedMessage(code)),
            }
        }

        let id = key_data.ok_or(Error::NoBackendKeyData)?;

        info!("new server connection [{}]", addr);

        Ok(Server {
            addr: addr.clone(),
            stream: Some(stream),
            id,
            params,
            state: State::Idle,
            created_at: Instant::now(),
            last_used_at: Instant::now(),
            last_healthcheck: None,
            stats: ConnStats::default(),
        })
    }

    /// Request query cancellation for the given backend server identifier.
    pub async fn cancel(addr: &str, id: &BackendKeyData) -> Result<(), Error> {
        let mut stream = TcpStream::connect(addr).await?;
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

    /// Send messages to the server.
    pub async fn send(&mut self, messages: Vec<impl Protocol>) -> Result<(), Error> {
        self.state = State::Active;
        let timer = Instant::now();
        match self.stream().send_many(messages).await {
            Ok(sent) => {
                self.stats.bytes_sent += sent;
            }
            Err(err) => {
                self.state = State::Error;
                return Err(err.into());
            }
        };
        trace!(
            "request sent to server [{:.4}ms]",
            timer.elapsed().as_secs_f64() * 1000.0
        );
        Ok(())
    }

    /// Flush all pending messages making sure they are sent to the server immediately.
    pub async fn flush(&mut self) -> Result<(), Error> {
        if let Err(err) = self.stream().flush().await {
            self.state = State::Error;
            Err(err.into())
        } else {
            Ok(())
        }
    }

    /// Read a single message from the server.
    pub async fn read(&mut self) -> Result<Message, Error> {
        let message = match self.stream().read().await {
            Ok(message) => message,
            Err(err) => {
                self.state = State::Error;
                return Err(err.into());
            }
        };

        self.stats.bytes_received += message.len();

        if message.code() == 'Z' {
            self.stats.queries += 1;

            let rfq = ReadyForQuery::from_bytes(message.payload())?;

            match rfq.status {
                'I' => {
                    self.state = State::Idle;
                    self.stats.transactions += 1;
                    self.last_used_at = Instant::now();
                }
                'T' => self.state = State::IdleInTransaction,
                'E' => {
                    self.state = State::TransactionError;
                    self.stats.transactions += 1;
                }
                status => {
                    self.state = State::Error;
                    return Err(Error::UnexpectedTransactionStatus(status));
                }
            }
        }

        Ok(message)
    }

    /// Server sent everything.
    #[inline]
    pub fn done(&self) -> bool {
        self.state == State::Idle
    }

    /// Server connection is synchronized and can receive more messages.
    #[inline]
    pub fn in_sync(&self) -> bool {
        matches!(
            self.state,
            State::IdleInTransaction | State::TransactionError | State::Idle
        )
    }

    /// Server is still inside a transaction.
    #[inline]
    pub fn in_transaction(&self) -> bool {
        matches!(
            self.state,
            State::IdleInTransaction | State::TransactionError
        )
    }

    /// The server connection permanently failed.
    #[inline]
    pub fn error(&self) -> bool {
        self.state == State::Error
    }

    /// Server parameters.
    #[inline]
    pub fn params(&self) -> &Parameters {
        &self.params
    }

    /// Execute a batch of queries and return all results.
    pub async fn execute_batch(&mut self, queries: &[&str]) -> Result<Vec<Message>, Error> {
        if !self.in_sync() {
            return Err(Error::NotInSync);
        }

        let mut messages = vec![];
        let queries = queries
            .iter()
            .map(|query| Query::new(query))
            .collect::<Vec<Query>>();
        let expected = queries.len();

        self.send(queries).await?;

        let mut zs = 0;
        while zs < expected {
            let message = self.read().await?;
            if message.code() == 'Z' {
                zs += 1;
            }
            messages.push(message);
        }

        Ok(messages)
    }

    /// Execute a query on the server and return the result.
    pub async fn execute(&mut self, query: &str) -> Result<Vec<Message>, Error> {
        self.execute_batch(&[query]).await
    }

    /// Perform a healthcheck on this connection using the provided query.
    pub async fn healthcheck(&mut self, query: &str) -> Result<(), Error> {
        debug!("running healthcheck \"{}\" [{}]", query, self.addr);

        self.execute(query).await?;
        self.last_healthcheck = Some(Instant::now());

        Ok(())
    }

    /// Attempt to rollback the transaction on this server, if any has been started.
    pub async fn rollback(&mut self) {
        if self.in_transaction() {
            if let Err(_err) = self.execute("ROLLBACK").await {
                self.state = State::Error;
            }
        }

        if !self.in_sync() {
            self.state = State::Error;
        }
    }

    /// Reset all server parameters and session state.
    pub async fn reset(&mut self) {
        if self.done() {
            if let Err(_err) = self.execute_batch(&["RESET ALL", "DISCARD ALL"]).await {
                self.state = State::Error;
            }
            debug!("connection reset [{}]", self.addr());
        }
    }

    /// Server connection unique identifier.
    #[inline]
    pub fn id(&self) -> &BackendKeyData {
        &self.id
    }

    /// How old this connection is.
    #[inline]
    pub fn age(&self, instant: Instant) -> Duration {
        instant.duration_since(self.created_at)
    }

    /// How long this connection has been idle.
    #[inline]
    pub fn idle_for(&self, instant: Instant) -> Duration {
        instant.duration_since(self.last_used_at)
    }

    /// How long has it been since the last connection healthcheck.
    #[inline]
    pub fn healthcheck_age(&self, instant: Instant) -> Duration {
        if let Some(last_healthcheck) = self.last_healthcheck {
            instant.duration_since(last_healthcheck)
        } else {
            Duration::MAX
        }
    }

    /// Get server address.
    #[inline]
    pub fn addr(&self) -> &Address {
        &self.addr
    }

    #[inline]
    fn stream(&mut self) -> &mut Stream {
        self.stream.as_mut().unwrap()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(mut stream) = self.stream.take() {
            // If you see a lot of these, tell your clients
            // to not send queries unless they are willing to stick
            // around for results.
            let out_of_sync = if self.done() { " " } else { " out of sync " };
            info!("closing{}server connection [{}]", out_of_sync, self.addr,);

            spawn(async move {
                stream.write_all(&Terminate.to_bytes()?).await?;
                stream.flush().await?;
                Ok::<(), Error>(())
            });
        }
    }
}

// Used for testing.
#[cfg(test)]
mod test {
    use super::*;

    impl Default for Server {
        fn default() -> Self {
            Self {
                addr: Address::default(),
                stream: None,
                id: BackendKeyData::default(),
                params: Parameters::default(),
                state: State::Idle,
                created_at: Instant::now(),
                last_used_at: Instant::now(),
                last_healthcheck: None,
                stats: ConnStats::default(),
            }
        }
    }

    impl Server {
        pub fn new_error() -> Server {
            let mut server = Server::default();
            server.state = State::Error;

            server
        }

        // pub(super) fn new_in_transaction() -> Server {
        //     let mut server = Server::default();
        //     server.state = State::IdleInTransaction;

        //     server
        // }

        // pub(super) fn new_active() -> Server {
        //     let mut server = Server::default();
        //     server.state = State::Active;

        //     server
        // }
    }
}
