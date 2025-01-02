//! PostgreSQL serer connection.
use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::spawn;
use tracing::{debug, info};

use super::Error;
use crate::net::messages::{
    Authentication, BackendKeyData, ErrorResponse, Message, ParameterStatus, Query, ReadyForQuery,
};
use crate::net::{
    messages::{hello::SslReply, FromBytes, Protocol, Startup, ToBytes},
    tls::connector,
    Stream,
};
use crate::state::State;

/// PostgreSQL server connection.
pub struct Server {
    stream: Stream,
    id: BackendKeyData,
    params: Vec<(String, String)>,
    state: State,
}

impl Server {
    /// Create new PostgreSQL server connection.
    pub async fn connect(addr: &str) -> Result<Self, Error> {
        debug!("=> {}", addr);
        let mut stream = Stream::plain(TcpStream::connect(addr).await?);

        // Request TLS.
        stream.write_all(&Startup::tls().to_bytes()?).await?;
        stream.flush().await?;

        let mut ssl = BytesMut::new();
        ssl.put_u8(stream.read_u8().await?);
        let ssl = SslReply::from_bytes(ssl.freeze())?;

        if ssl == SslReply::Yes {
            let connector = connector()?;
            let plain = stream.take()?;

            stream = Stream::tls(connector.connect(addr, plain).await?);
        }

        stream.write_all(&Startup::new().to_bytes()?).await?;
        stream.flush().await?;

        // Perform authentication.
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
                    }
                }

                code => return Err(Error::UnexpectedMessage(code)),
            }
        }

        let mut params = vec![];
        let mut key_data: Option<BackendKeyData> = None;

        loop {
            let message = stream.read().await?;

            match message.code() {
                // ReadyForQery (B)
                'Z' => break,
                // ParameterStatus (B)
                'S' => {
                    let parameter = ParameterStatus::from_bytes(message.payload())?;
                    params.push((parameter.name, parameter.value));
                }
                // BackendKeyData (B)
                'K' => {
                    key_data = Some(BackendKeyData::from_bytes(message.payload())?);
                }

                code => return Err(Error::UnexpectedMessage(code)),
            }
        }

        let id = key_data.ok_or(Error::NoBackendKeyData)?;

        info!("💾 {}", addr);

        Ok(Server {
            stream,
            id,
            params,
            state: State::Idle,
        })
    }

    /// Send messages to the server.
    pub async fn send(&mut self, messages: Vec<impl Protocol + ToBytes>) -> Result<(), Error> {
        self.state = State::Active;
        self.stream.send_many(messages).await?;
        Ok(())
    }

    /// Flush all pending messages making sure they are sent to the server immediately.
    pub async fn flush(&mut self) -> Result<(), Error> {
        self.stream.flush().await?;
        Ok(())
    }

    /// Read a single message from the server.
    pub async fn read(&mut self) -> Result<Message, Error> {
        let message = self.stream.read().await?;

        match message.code() {
            'Z' => {
                let rfq = ReadyForQuery::from_bytes(message.payload())?;
                match rfq.status {
                    'I' => self.state = State::Idle,
                    'T' => self.state = State::IdleInTransaction,
                    'E' => self.state = State::TransactionError,
                    status => {
                        self.state = State::Error;
                        return Err(Error::UnexpectedTransactionStatus(status));
                    }
                }
            }

            _ => (),
        }

        Ok(message)
    }

    /// Server sent everything.
    pub fn done(&self) -> bool {
        self.state == State::Idle
    }

    /// Server parameters.
    pub fn params(&self) -> &Vec<(String, String)> {
        &self.params
    }

    /// Execute a query on the server and return the result.
    pub async fn execute(&mut self, query: &str) -> Result<Vec<Message>, Error> {
        if self.state == State::Active {
            return Err(Error::NotInSync);
        }

        self.stream.send(Query::new(query)).await?;

        let mut messages = vec![];

        while !matches!(self.state, State::Idle | State::Error) {
            messages.push(self.read().await?);
        }

        Ok(messages)
    }

    /// Attempt to rollback the transaction on this server, if any has been started.
    pub fn rollback(mut self) {
        spawn(async move {
            match self.state {
                State::IdleInTransaction => {
                    if let Err(_err) = self.execute("ROLLBACK").await {
                        self.state = State::Error;
                    }
                }

                State::Active => {
                    self.state = State::Error;
                }

                _ => (),
            }
        });
    }
}
