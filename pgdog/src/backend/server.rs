//! PostgreSQL server connection.

use std::time::Duration;

use bytes::{BufMut, BytesMut};
use rustls_pki_types::ServerName;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    spawn,
    time::Instant,
};
use tracing::{debug, error, info, trace, warn};

use super::{
    pool::Address, prepared_statements::HandleResult, ConnectReason, DisconnectReason, Error,
    PreparedStatements, ServerOptions, Stats,
};
use crate::{
    auth::{md5, scram::Client},
    backend::pool::stats::MemoryStats,
    config::AuthType,
    frontend::ClientRequest,
    net::{
        messages::{
            hello::SslReply, Authentication, BackendKeyData, ErrorResponse, FromBytes, Message,
            ParameterStatus, Password, Protocol, Query, ReadyForQuery, Startup, Terminate, ToBytes,
        },
        Close, MessageBuffer, Parameter, ProtocolMessage, Sync,
    },
    stats::memory::MemoryUsage,
};
use crate::{
    config::{config, PoolerMode, TlsVerifyMode},
    net::{
        messages::{DataRow, NoticeResponse},
        parameter::Parameters,
        tls::connector_with_verify_mode,
        CommandComplete, Stream,
    },
};
use crate::{net::tweak, state::State};

/// PostgreSQL server connection.
#[derive(Debug)]
pub struct Server {
    addr: Address,
    stream: Option<Stream>,
    id: BackendKeyData,
    params: Parameters,
    changed_params: Parameters,
    client_params: Parameters,
    stats: Stats,
    prepared_statements: PreparedStatements,
    dirty: bool,
    streaming: bool,
    schema_changed: bool,
    sync_prepared: bool,
    in_transaction: bool,
    re_synced: bool,
    replication_mode: bool,
    statement_executed: bool,
    sending_request: bool,
    pooler_mode: PoolerMode,
    stream_buffer: MessageBuffer,
    disconnect_reason: Option<DisconnectReason>,
}

impl MemoryUsage for Server {
    #[inline]
    fn memory_usage(&self) -> usize {
        std::mem::size_of::<BackendKeyData>()
            + self.params.memory_usage()
            + self.changed_params.memory_usage()
            + self.client_params.memory_usage()
            + std::mem::size_of::<Stats>()
            + self.prepared_statements.memory_used()
            + 7 * std::mem::size_of::<bool>()
            + std::mem::size_of::<PoolerMode>()
            + self.stream_buffer.capacity()
    }
}

impl Server {
    /// Create new PostgreSQL server connection.
    pub async fn connect(
        addr: &Address,
        options: ServerOptions,
        connect_reason: ConnectReason,
    ) -> Result<Self, Error> {
        debug!("=> {}", addr);
        let stream = TcpStream::connect(addr.addr().await?).await?;
        tweak(&stream)?;
        let cfg = config();

        let mut stream = Stream::plain(stream, cfg.config.memory.net_buffer);

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
                        stream = Stream::tls(cipher, cfg.config.memory.net_buffer);
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
        let mut auth_type = AuthType::Trust;
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
                            auth_type = AuthType::Scram;
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
                            auth_type = AuthType::Md5;
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
                    let notice = NoticeResponse::from_bytes(message.payload())?;
                    warn!("{} [{}]", notice.message, addr);
                }

                code => return Err(Error::UnexpectedMessage(code)),
            }
        }

        let id = key_data.ok_or(Error::NoBackendKeyData)?;
        let params: Parameters = params.into();

        info!(
            "new server connection [{}, auth: {}, reason: {}] {}",
            addr,
            auth_type,
            connect_reason,
            if stream.is_tls() { "🔒" } else { "" },
        );

        let mut server = Server {
            addr: addr.clone(),
            stream: Some(stream),
            id,
            stats: Stats::connect(id, addr, &params, &options, &cfg.config.memory),
            replication_mode: options.replication_mode(),
            params,
            changed_params: Parameters::default(),
            client_params: Parameters::default(),
            prepared_statements: PreparedStatements::new(),
            dirty: false,
            streaming: false,
            schema_changed: false,
            sync_prepared: false,
            in_transaction: false,
            statement_executed: false,
            re_synced: false,
            sending_request: false,
            pooler_mode: PoolerMode::Transaction,
            stream_buffer: MessageBuffer::new(cfg.config.memory.message_buffer),
            disconnect_reason: None,
        };

        server.stats.memory_used(server.memory_stats()); // Stream capacity.

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

    /// Send messages to the server and flush the buffer.
    pub async fn send(&mut self, client_request: &ClientRequest) -> Result<(), Error> {
        // Request is being sent to the server, so the
        // server connection is in a partial state.
        self.sending_request = true;

        self.stats.state(State::Active);

        for message in client_request.messages.iter() {
            self.send_one(message).await?;
        }
        self.flush().await?;

        // The whole request is now in server's hands.
        // We can recover the connection from this point on.
        self.sending_request = false;

        self.stats.state(State::ReceivingData);

        Ok(())
    }

    /// Send one message to the server but don't flush the buffer,
    /// accelerating bulk transfers.
    pub async fn send_one(&mut self, message: &ProtocolMessage) -> Result<(), Error> {
        self.stats.state(State::Active);

        let result = self.prepared_statements.handle(message)?;

        let queue = match result {
            HandleResult::Drop => [None, None],
            HandleResult::Prepend(ref prepare) => [Some(prepare), Some(message)],
            HandleResult::Forward => [Some(message), None],
        };

        for message in queue.iter().flatten() {
            trace!("{:#?} >>> [{}]", message, self.addr());
        }

        for message in queue.into_iter().flatten() {
            match self.stream().send(message).await {
                Ok(sent) => self.stats.send(sent, message.code() as u8),
                Err(err) => {
                    self.stats.state(State::Error);
                    return Err(err.into());
                }
            }
        }

        Ok(())
    }

    /// Flush all pending messages making sure they are sent to the server immediately.
    pub async fn flush(&mut self) -> Result<(), Error> {
        if let Err(err) = self.stream().flush().await {
            trace!("😳");
            self.stats.state(State::Error);
            Err(err.into())
        } else {
            Ok(())
        }
    }

    /// Read a single message from the server.
    ///
    /// # Cancellation safety
    ///
    /// This method is cancel-safe.
    ///
    pub async fn read(&mut self) -> Result<Message, Error> {
        let message = loop {
            if let Some(message) = self.prepared_statements.state_mut().get_simulated() {
                return Ok(message.backend(self.id));
            }
            match self.stream_buffer.read(self.stream.as_mut().unwrap()).await {
                Ok(message) => {
                    let message = message.stream(self.streaming).backend(self.id);
                    match self.prepared_statements.forward(&message) {
                        Ok(forward) => {
                            if forward {
                                break message;
                            }
                        }
                        Err(err) => {
                            if let Error::ProtocolOutOfSync = err {
                                // conservatively, we do not know for sure if this is recoverable
                                self.stats.state(State::Error);
                            }
                            error!(
                                "{:?} got: {}, extended buffer: {:?}, state: {}",
                                err,
                                message.code(),
                                self.prepared_statements.state(),
                                self.stats.get_state(),
                            );
                            return Err(err);
                        }
                    }
                }

                Err(err) => {
                    self.stats.state(State::Error);
                    return Err(err.into());
                }
            }
        };

        self.stats.receive(message.len(), message.code() as u8);

        match message.code() {
            'Z' => {
                let now = Instant::now();
                let rfq = ReadyForQuery::from_bytes(message.payload())?;

                self.stats.query(now, rfq.status == 'T');
                self.stats.memory_used(self.memory_stats());

                match rfq.status {
                    'I' => {
                        self.in_transaction = false;
                        self.stats.transaction(now);
                    }
                    'T' => {
                        self.in_transaction = true;
                        self.stats.state(State::IdleInTransaction);
                    }
                    'E' => self.stats.transaction_error(now),
                    status => {
                        self.stats.state(State::Error);
                        return Err(Error::UnexpectedTransactionStatus(status));
                    }
                }
                self.stream_buffer.shrink_to_fit();
                self.streaming = false;
                self.statement_executed = false;
            }
            'E' => {
                let error = ErrorResponse::from_bytes(message.to_bytes()?)?;
                self.schema_changed = error.code == "0A000";
                self.stats.error();
            }
            'W' => {
                debug!("streaming replication on [{}]", self.addr());
                self.streaming = true;
            }
            'S' => {
                let ps = ParameterStatus::from_bytes(message.to_bytes()?)?;
                self.changed_params.insert(ps.name, ps.value);
            }
            'C' => {
                let cmd = CommandComplete::from_bytes(message.to_bytes()?)?;
                match cmd.command() {
                    "PREPARE" | "DEALLOCATE" => self.sync_prepared = true,
                    "DEALLOCATE ALL" => self.prepared_statements.clear(),
                    "DISCARD ALL" => {
                        self.prepared_statements.clear();
                        self.client_params.clear();
                    }
                    "RESET" => self.client_params.clear(), // Someone reset params, we're gonna need to re-sync.
                    _ => (),
                }
                self.statement_executed = true;
            }
            's' => self.statement_executed = true,
            'G' => self.stats.copy_mode(),
            '1' => self.stats.parse_complete(),
            '2' => self.stats.bind_complete(),
            _ => (),
        }

        trace!("{:#?} <<< [{}]", message, self.addr());

        Ok(message)
    }

    /// Synchronize parameters between client and server.
    pub async fn link_client(
        &mut self,
        id: &BackendKeyData,
        params: &Parameters,
        start_transaction: Option<&str>,
    ) -> Result<usize, Error> {
        // Sync application_name parameter
        // and update it in the stats.
        let server_name = self.client_params.get_default("application_name", "PgDog");
        let client_name = params.get_default("application_name", "PgDog");

        self.stats.link_client(client_name, server_name, id);

        // Clear any params previously tracked by SET.
        self.changed_params.clear();
        let mut clear_params = false;

        // Compare client and server params.
        let mut executed = if !params.identical(&self.client_params) {
            // Construct client parameter SET queries.
            let tracked = params.tracked();
            // Construct RESET queries to reset any current params
            // to their default values.
            let mut queries = self.client_params.reset_queries();

            // Combine both to create a new, fresh session state
            // on this connection.
            queries.extend(tracked.set_queries(false));

            // Set state on the connection only if
            // there are any params to change.
            if !queries.is_empty() {
                debug!("syncing {} params", queries.len());

                self.execute_batch(&queries).await?;
                clear_params = true;
            }

            // Update params on this connection.
            self.client_params = tracked;

            queries.len()
        } else {
            0
        };

        // Sync any parameters set inside the transaction. These will
        // need to be revered on rollback or commited on commit.
        if let Some(start_transaction) = start_transaction {
            self.execute(start_transaction).await?;
            let transaction_sets = params.set_queries(true);

            if !transaction_sets.is_empty() {
                debug!("syncing {} in-transaction params", transaction_sets.len());

                self.execute_batch(&transaction_sets).await?;
                clear_params = true;

                self.client_params.copy_in_transaction(params);
            }

            executed += transaction_sets.len();
        }

        if clear_params {
            // We can receive ParameterStatus messages here,
            // but we should ignore them since we are managing the session state.
            self.changed_params.clear();
        }

        Ok(executed)
    }

    // Handle COMMIT/ROLLBACK for in-transaction params tracking.
    pub fn transaction_params_hook(&mut self, rollback: bool) {
        if rollback {
            self.client_params.rollback();
        } else {
            self.client_params.commit();
        }
    }

    pub fn changed_params(&self) -> &Parameters {
        &self.changed_params
    }

    pub fn reset_changed_params(&mut self) {
        self.changed_params.clear();
    }

    /// We can disconnect from this server.
    ///
    /// There are no more expected messages from the server connection
    /// and we haven't started an explicit transaction.
    pub fn done(&self) -> bool {
        self.prepared_statements.done() && !self.in_transaction() && !self.statement_executed
    }

    /// Server hasn't finished sending or receiving a complete message.
    pub fn io_in_progress(&self) -> bool {
        self.stream
            .as_ref()
            .map(|stream| stream.io_in_progress())
            .unwrap_or(false)
    }

    /// Server can execute a query.
    pub fn in_sync(&self) -> bool {
        matches!(
            self.stats().get_state(),
            State::Idle | State::IdleInTransaction | State::TransactionError
        )
    }

    /// Server is done executing all queries and is
    /// not inside a transaction.
    pub fn can_check_in(&self) -> bool {
        self.stats().get_state() == State::Idle
    }

    /// Server hasn't sent all messages yet.
    pub fn has_more_messages(&self) -> bool {
        self.prepared_statements.has_more_messages() || self.streaming
    }

    pub fn in_copy_mode(&self) -> bool {
        self.prepared_statements.in_copy_mode()
    }

    /// Server is still inside a transaction.
    #[inline]
    pub fn in_transaction(&self) -> bool {
        self.in_transaction
    }

    /// The server connection permanently failed.
    #[inline]
    pub fn error(&self) -> bool {
        self.stats().get_state() == State::Error
    }

    /// Did the schema change and prepared statements are broken.
    pub fn schema_changed(&self) -> bool {
        self.schema_changed
    }

    /// Prepared statements changed outside of our pipeline,
    /// need to resync from `pg_prepared_statements` view.
    pub fn sync_prepared(&self) -> bool {
        self.sync_prepared
    }

    /// Connection was left with an unfinished query.
    pub fn needs_drain(&self) -> bool {
        !self.in_sync()
    }

    /// A request is being sent by a client.
    pub fn sending_request(&self) -> bool {
        self.sending_request
    }

    /// Close the connection, don't do any recovery.
    pub fn force_close(&self) -> bool {
        self.stats().get_state() == State::ForceClose || self.io_in_progress()
    }

    /// Server parameters.
    #[inline]
    pub fn params(&self) -> &Parameters {
        &self.params
    }

    /// Execute a batch of queries and return all results.
    pub async fn execute_batch(&mut self, queries: &[Query]) -> Result<Vec<Message>, Error> {
        let mut err = None;
        if !self.in_sync() {
            return Err(Error::NotInSync);
        }

        // Empty queries will throw the server out of sync.
        if queries.is_empty() {
            return Ok(vec![]);
        }

        #[cfg(debug_assertions)]
        for query in queries {
            debug!("{} [{}]", query.query(), self.addr());
        }

        let mut messages = vec![];
        let queries = queries
            .iter()
            .map(Clone::clone)
            .map(ProtocolMessage::Query)
            .collect::<Vec<ProtocolMessage>>();
        let expected = queries.len();

        self.send(&queries.into()).await?;

        let mut zs = 0;
        while zs < expected {
            let message = self.read().await?;
            if message.code() == 'Z' {
                zs += 1;
            }

            if message.code() == 'E' {
                err = Some(ErrorResponse::from_bytes(message.to_bytes()?)?);
            }
            messages.push(message);
        }

        if let Some(err) = err {
            Err(Error::ExecutionError(Box::new(err)))
        } else {
            Ok(messages)
        }
    }

    /// Execute a query on the server and return the result.
    pub async fn execute(&mut self, query: impl Into<Query>) -> Result<Vec<Message>, Error> {
        let query = query.into();
        self.execute_batch(&[query]).await
    }

    /// Execute query and raise an error if one is returned by PostgreSQL.
    pub async fn execute_checked(
        &mut self,
        query: impl Into<Query>,
    ) -> Result<Vec<Message>, Error> {
        let messages = self.execute(query).await?;
        let error = messages.iter().find(|m| m.code() == 'E');
        if let Some(error) = error {
            let error = ErrorResponse::from_bytes(error.to_bytes()?)?;
            Err(Error::ExecutionError(Box::new(error)))
        } else {
            Ok(messages)
        }
    }

    /// Execute a query and return all rows.
    pub async fn fetch_all<T: From<DataRow>>(
        &mut self,
        query: impl Into<Query>,
    ) -> Result<Vec<T>, Error> {
        let messages = self.execute_checked(query).await?;
        Ok(messages
            .into_iter()
            .filter(|message| message.code() == 'D')
            .map(|message| message.to_bytes().unwrap())
            .map(DataRow::from_bytes)
            .collect::<Result<Vec<DataRow>, crate::net::Error>>()?
            .into_iter()
            .map(|row| T::from(row))
            .collect())
    }

    /// Perform a healthcheck on this connection using the provided query.
    pub async fn healthcheck(&mut self, query: &str) -> Result<(), Error> {
        debug!("running healthcheck \"{}\" [{}]", query, self.addr);

        self.execute(query).await?;

        self.stats.healthcheck();

        Ok(())
    }

    /// Attempt to rollback the transaction on this server, if any has been started.
    pub(super) async fn rollback(&mut self) -> Result<(), Error> {
        if self.in_transaction() {
            self.execute("ROLLBACK").await?;
            self.stats.rollback();
        }

        // Reset any params left over.
        self.transaction_params_hook(true);

        if !self.done() {
            Err(Error::RollbackFailed)
        } else {
            Ok(())
        }
    }

    /// Drain any remaining messages on the server connection,
    /// attempting to return the connection into a synchronized state.
    pub(super) async fn drain(&mut self) -> Result<(), Error> {
        while self.has_more_messages() {
            self.read().await?;
        }

        if !self.in_sync() {
            self.send(&vec![ProtocolMessage::Sync(Sync)].into()).await?;

            while !self.in_sync() {
                self.read().await?;
            }
        }

        self.re_synced = true;
        Ok(())
    }

    /// Synchronize prepared statements from Postgres.
    pub(super) async fn sync_prepared_statements(&mut self) -> Result<(), Error> {
        let names = self
            .fetch_all::<String>("SELECT name FROM pg_prepared_statements")
            .await?;

        for name in names {
            self.prepared_statements.prepared(&name);
        }

        debug!("prepared statements synchronized [{}]", self.addr());

        let count = self.prepared_statements.len();
        self.stats.set_prepared_statements(count);
        self.sync_prepared = false;

        Ok(())
    }

    /// Close any prepared statements that exceed cache capacity.
    pub(super) fn ensure_prepared_capacity(&mut self) -> Vec<Close> {
        let close = self.prepared_statements.ensure_capacity();
        self.stats
            .close_many(close.len(), self.prepared_statements.len());
        close
    }

    /// Close multiple prepared statements.
    pub(super) async fn close_many(&mut self, close: &[Close]) -> Result<(), Error> {
        if close.is_empty() {
            return Ok(());
        }

        let mut buf = vec![];
        for close in close {
            buf.push(close.message()?);
        }

        buf.push(Sync.message()?);

        debug!(
            "closing {} prepared statements [{}]",
            close.len(),
            self.addr()
        );

        self.stream().send_many(&buf).await?;

        for close in close {
            let response = self.stream().read().await?;
            match response.code() {
                '3' => self.prepared_statements.remove(close.name()),
                'E' => {
                    return Err(Error::PreparedStatementError(Box::new(
                        ErrorResponse::from_bytes(response.to_bytes()?)?,
                    )));
                }
                c => {
                    return Err(Error::UnexpectedMessage(c));
                }
            };
        }

        let rfq = self.stream().read().await?;
        if rfq.code() != 'Z' {
            return Err(Error::UnexpectedMessage(rfq.code()));
        }

        Ok(())
    }

    /// Reset error state caused by schema change.
    #[inline]
    pub fn reset_schema_changed(&mut self) {
        self.schema_changed = false;
        self.prepared_statements.clear();
    }

    #[inline]
    pub fn reset_params(&mut self) {
        self.client_params.clear();
    }

    #[inline]
    pub fn reset_re_synced(&mut self) {
        self.re_synced = false;
    }

    #[inline]
    pub fn re_synced(&self) -> bool {
        self.re_synced
    }

    #[inline]
    pub fn disconnect_reason(&mut self, reason: DisconnectReason) {
        if self.disconnect_reason.is_none() {
            self.disconnect_reason = Some(reason);
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
        instant.duration_since(self.stats().created_at())
    }

    /// How long this connection has been idle.
    #[inline]
    pub fn idle_for(&self, instant: Instant) -> Duration {
        instant.duration_since(self.stats().last_used())
    }

    /// How long has it been since the last connection healthcheck.
    #[inline]
    pub fn healthcheck_age(&self, instant: Instant) -> Duration {
        if let Some(last_healthcheck) = self.stats().last_healthcheck() {
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

    /// Server needs a cleanup because client changed a session variable
    /// of parameter.
    #[inline]
    pub fn dirty(&self) -> bool {
        self.dirty
    }

    #[inline]
    pub fn mark_dirty(&mut self, dirty: bool) {
        self.dirty = dirty;
    }

    /// Server has been cleaned.
    #[inline]
    pub(super) fn cleaned(&mut self) {
        self.dirty = false;
        self.stats.cleaned();
    }

    /// Server is streaming data.
    #[inline]
    pub fn streaming(&self) -> bool {
        self.streaming
    }

    #[inline]
    pub fn stats(&self) -> &Stats {
        &self.stats
    }

    #[inline]
    pub fn stats_mut(&mut self) -> &mut Stats {
        &mut self.stats
    }

    #[inline]
    pub fn set_pooler_mode(&mut self, pooler_mode: PoolerMode) {
        self.pooler_mode = pooler_mode;
    }

    #[inline]
    pub fn pooler_mode(&self) -> &PoolerMode {
        &self.pooler_mode
    }

    #[inline]
    pub fn replication_mode(&self) -> bool {
        self.replication_mode
    }

    #[inline]
    pub fn prepared_statements_mut(&mut self) -> &mut PreparedStatements {
        &mut self.prepared_statements
    }

    #[inline]
    pub fn prepared_statements(&self) -> &PreparedStatements {
        &self.prepared_statements
    }

    #[inline]
    pub fn memory_stats(&self) -> MemoryStats {
        MemoryStats {
            inner: pgdog_stats::MemoryStats {
                buffer: *self.stream_buffer.stats(),
                prepared_statements: self.prepared_statements.memory_used(),
                stream: self
                    .stream
                    .as_ref()
                    .map(|s| s.memory_usage())
                    .unwrap_or_default(),
            },
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stats().disconnect();
        if let Some(mut stream) = self.stream.take() {
            info!(
                "closing server connection [{}, state: {}, reason: {}]",
                self.addr,
                self.stats.get_state(),
                self.disconnect_reason.take().unwrap_or_default(),
            );

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
pub mod test {
    use crate::{config::Memory, frontend::PreparedStatements, net::*};

    use super::{Error, *};

    impl Default for Server {
        fn default() -> Self {
            let id = BackendKeyData::new();
            let addr = Address::default();
            Self {
                stream: None,
                id,
                params: Parameters::default(),
                changed_params: Parameters::default(),
                client_params: Parameters::default(),
                stats: Stats::connect(
                    id,
                    &addr,
                    &Parameters::default(),
                    &ServerOptions::default(),
                    &Memory::default(),
                ),
                prepared_statements: super::PreparedStatements::new(),
                addr,
                dirty: false,
                streaming: false,
                schema_changed: false,
                sync_prepared: false,
                in_transaction: false,
                re_synced: false,
                replication_mode: false,
                pooler_mode: PoolerMode::Transaction,
                stream_buffer: MessageBuffer::new(4096),
                disconnect_reason: None,
                statement_executed: false,
                sending_request: false,
            }
        }
    }

    impl Server {
        pub fn new_error() -> Server {
            let mut server = Server::default();
            server.stats.state(State::Error);

            server
        }
    }

    pub async fn test_server() -> Server {
        Server::connect(
            &Address::new_test(),
            ServerOptions::default(),
            ConnectReason::Other,
        )
        .await
        .unwrap()
    }

    pub async fn test_replication_server() -> Server {
        Server::connect(
            &Address::new_test(),
            ServerOptions::new_replication(),
            ConnectReason::Replication,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn test_simple_query() {
        let mut server = test_server().await;
        for _ in 0..25 {
            server
                .send(&vec![ProtocolMessage::from(Query::new("SELECT 1"))].into())
                .await
                .unwrap();
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), 'T');
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), 'D');
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), 'C');
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), 'Z');
            assert_eq!(server.prepared_statements.state().len(), 0);
            assert!(server.done());
        }

        for _ in 0..25 {
            server
                .send(&vec![ProtocolMessage::from(Query::new("SELECT 1"))].into())
                .await
                .unwrap();
        }
        for _ in 0..25 {
            for c in ['T', 'D', 'C', 'Z'] {
                let msg = server.read().await.unwrap();
                assert_eq!(msg.code(), c);
            }
        }
        assert!(server.done());
    }

    #[tokio::test]
    async fn test_empty_query() {
        let mut server = test_server().await;
        let empty = Query::new(";");
        server
            .send(&vec![ProtocolMessage::from(empty)].into())
            .await
            .unwrap();

        for c in ['I', 'Z'] {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), c);
        }

        assert_eq!(server.prepared_statements.state().len(), 0);
        assert!(server.done());
    }

    #[tokio::test]
    async fn test_set() {
        let mut server = test_server().await;
        server
            .send(
                &vec![ProtocolMessage::from(Query::new(
                    "SET application_name TO 'test'",
                ))]
                .into(),
            )
            .await
            .unwrap();

        for c in ['C', 'S', 'Z'] {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), c);
        }

        assert!(server.done());
    }

    #[tokio::test]
    async fn test_extended_anonymous() {
        let mut server = test_server().await;
        use crate::net::bind::Parameter;
        for _ in 0..25 {
            let bind = Bind::new_params_codes(
                "",
                &[Parameter {
                    len: 1,
                    data: "1".as_bytes().into(),
                }],
                &[Format::Text],
            );

            server
                .send(
                    &vec![
                        ProtocolMessage::from(Parse::new_anonymous("SELECT $1")),
                        ProtocolMessage::from(bind),
                        ProtocolMessage::from(Execute::new()),
                        ProtocolMessage::from(Sync::new()),
                    ]
                    .into(),
                )
                .await
                .unwrap();

            for c in ['1', '2', 'D', 'C', 'Z'] {
                let msg = server.read().await.unwrap();
                assert_eq!(msg.code(), c);
            }

            assert!(server.done());
            assert_eq!(
                server.stats().total().idle_in_transaction_time,
                Duration::ZERO
            );
        }
    }

    #[tokio::test]
    async fn test_prepared() {
        let mut server = test_server().await;
        use crate::net::bind::Parameter;

        for i in 0..25 {
            let name = format!("test_prepared_{}", i);
            let parse = Parse::named(&name, format!("SELECT $1, 'test_{}'", name));
            let (new, new_name) = PreparedStatements::global().write().insert(&parse);
            let name = new_name;
            let parse = parse.rename(&name);
            assert!(new);

            let describe = Describe::new_statement(&name);
            let bind = Bind::new_params(
                &name,
                &[Parameter {
                    len: 1,
                    data: "1".as_bytes().into(),
                }],
            );

            server
                .send(
                    &vec![
                        ProtocolMessage::from(parse.clone()),
                        ProtocolMessage::from(describe.clone()),
                        Flush {}.into(),
                    ]
                    .into(),
                )
                .await
                .unwrap();

            for c in ['1', 't', 'T'] {
                let msg = server.read().await.unwrap();
                assert_eq!(c, msg.code());
            }

            // RowDescription saved.
            let global = server.prepared_statements.parse(&name).unwrap();
            server
                .prepared_statements
                .row_description(global.name())
                .unwrap();

            server
                .send(
                    &vec![
                        ProtocolMessage::from(describe.clone()),
                        ProtocolMessage::from(Flush),
                    ]
                    .into(),
                )
                .await
                .unwrap();
            for code in ['t', 'T'] {
                let msg = server.read().await.unwrap();
                assert_eq!(msg.code(), code);
            }

            assert_eq!(server.prepared_statements.state().len(), 0);

            server
                .send(
                    &vec![
                        ProtocolMessage::from(bind.clone()),
                        ProtocolMessage::from(Execute::new()),
                        ProtocolMessage::from(Sync {}),
                    ]
                    .into(),
                )
                .await
                .unwrap();

            for code in ['2', 'D', 'C', 'Z'] {
                let msg = server.read().await.unwrap();
                assert_eq!(msg.code(), code);
            }

            assert!(server.done());
        }
    }

    #[tokio::test]
    async fn test_prepared_in_cache() {
        use crate::net::bind::Parameter;
        let global = PreparedStatements::global();
        let parse = Parse::named("random_name", "SELECT $1");
        let (new, name) = global.write().insert(&parse);
        assert!(new);
        let parse = parse.rename(&name);
        assert_eq!(parse.name(), "__pgdog_1");

        let mut server = test_server().await;

        for _ in 0..25 {
            server
                .send(
                    &vec![
                        ProtocolMessage::from(Bind::new_params(
                            "__pgdog_1",
                            &[Parameter {
                                len: 1,
                                data: "1".as_bytes().into(),
                            }],
                        )),
                        Execute::new().into(),
                        Sync {}.into(),
                    ]
                    .into(),
                )
                .await
                .unwrap();

            for c in ['2', 'D', 'C', 'Z'] {
                let msg = server.read().await.unwrap();
                assert_eq!(msg.code(), c);
            }

            assert!(server.done());
        }
    }

    #[tokio::test]
    async fn test_bad_parse() {
        let mut server = test_server().await;
        for _ in 0..25 {
            let good_parse = Parse::named("test2", "SELECT $1");
            let parse = Parse::named("test", "SELECT bad syntax;");
            let good_parse_2 = Parse::named("test3", "SELECT $2"); // Will be ignored due to error above.
            server
                .send(
                    &vec![
                        good_parse.into(),
                        ProtocolMessage::from(parse),
                        good_parse_2.into(),
                        Describe::new_statement("test").into(),
                        Sync {}.into(),
                    ]
                    .into(),
                )
                .await
                .unwrap();
            for c in ['1', 'E', 'Z'] {
                let msg = server.read().await.unwrap();
                assert_eq!(msg.code(), c);
            }
            assert!(server.done());
            assert_eq!(server.prepared_statements.len(), 1);
            assert!(server.prepared_statements.contains("test2"));
            server
                .send(&vec![Query::new("SELECT 1").into()].into())
                .await
                .unwrap();

            for c in ['T', 'D', 'C', 'Z'] {
                let msg = server.read().await.unwrap();
                assert_eq!(msg.code(), c);
            }
        }
    }

    #[tokio::test]
    async fn test_bad_bind() {
        let mut server = test_server().await;
        for i in 0..25 {
            let name = format!("test_{}", i);
            let parse = Parse::named(&name, "SELECT $1");
            let describe = Describe::new_statement(&name);
            let bind = Bind::new_statement(&name);
            server
                .send(
                    &vec![
                        ProtocolMessage::from(parse),
                        describe.into(),
                        bind.into(),
                        Sync.into(),
                    ]
                    .into(),
                )
                .await
                .unwrap();

            for c in ['1', 't', 'T', 'E', 'Z'] {
                let msg = server.read().await.unwrap();
                assert_eq!(c, msg.code());
            }

            assert!(server.done());
        }
    }

    #[tokio::test]
    async fn test_already_prepared() {
        let mut server = test_server().await;
        let name = "test".to_string();
        let parse = Parse::named(&name, "SELECT $1");
        let describe = Describe::new_statement(&name);

        for _ in 0..25 {
            server
                .send(
                    &vec![
                        ProtocolMessage::from(parse.clone()),
                        describe.clone().into(),
                        Flush.into(),
                    ]
                    .into(),
                )
                .await
                .unwrap();

            for c in ['1', 't', 'T'] {
                let msg = server.read().await.unwrap();
                assert_eq!(msg.code(), c);
            }
        }
    }

    #[tokio::test]
    async fn test_bad_parse_removed() {
        let mut server = test_server().await;
        let name = "test".to_string();
        let parse = Parse::named(&name, "SELECT bad syntax");

        server
            .send(&vec![ProtocolMessage::from(parse.clone()), Sync.into()].into())
            .await
            .unwrap();
        for c in ['E', 'Z'] {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), c);
        }
        assert!(server.prepared_statements.is_empty());

        server
            .send(
                &vec![
                    ProtocolMessage::from(Parse::named("test", "SELECT $1")),
                    Flush.into(),
                ]
                .into(),
            )
            .await
            .unwrap();

        for c in ['1'] {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), c);
        }

        assert_eq!(server.prepared_statements.len(), 1);

        assert!(server.done());
    }

    #[tokio::test]
    async fn test_execute_checked() {
        let mut server = test_server().await;
        for _ in 0..25 {
            let mut msgs = server
                .execute_checked("SELECT 1")
                .await
                .unwrap()
                .into_iter();
            for c in ['T', 'D', 'C', 'Z'] {
                let msg = msgs.next().unwrap();
                assert_eq!(c, msg.code());
            }
            assert!(server.done());
        }
    }

    #[tokio::test]
    async fn test_multiple_queries() {
        let mut server = test_server().await;
        let q = Query::new("SELECT 1; SELECT 2;");
        server
            .send(&vec![ProtocolMessage::from(q)].into())
            .await
            .unwrap();
        for c in ['T', 'D', 'C', 'T', 'D', 'C', 'Z'] {
            let msg = server.read().await.unwrap();
            if c != 'Z' {
                assert!(!server.done());
            }
            assert_eq!(c, msg.code());
        }
    }

    #[tokio::test]
    async fn test_extended() {
        let mut server = test_server().await;
        let msgs = vec![
            ProtocolMessage::from(Parse::named("test_1", "SELECT $1")),
            Describe::new_statement("test_1").into(),
            Flush.into(),
            Query::new("BEGIN").into(),
            Bind::new_params(
                "test_1",
                &[crate::net::bind::Parameter {
                    len: 1,
                    data: "1".as_bytes().into(),
                }],
            )
            .into(),
            Describe::new_portal("").into(),
            Execute::new().into(),
            Sync.into(),
            Query::new("COMMIT").into(),
        ];
        server.send(&msgs.into()).await.unwrap();

        for c in ['1', 't', 'T', 'C', 'Z', '2', 'T', 'D', 'C', 'Z', 'C'] {
            let msg = server.read().await.unwrap();
            assert_eq!(c, msg.code());
            assert!(!server.done());
        }
        let msg = server.read().await.unwrap();
        assert_eq!(msg.code(), 'Z');

        assert!(server.done());
    }

    #[tokio::test]
    async fn test_delete() {
        let mut server = test_server().await;

        let msgs = vec![
            Query::new("BEGIN").into(),
            Query::new("CREATE TABLE IF NOT EXISTS test_delete (id BIGINT PRIMARY KEY)").into(),
            ProtocolMessage::from(Parse::named("test", "DELETE FROM test_delete")),
            Describe::new_statement("test").into(),
            Bind::new_statement("test").into(),
            Execute::new().into(),
            Sync.into(),
            Query::new("ROLLBACK").into(),
        ];

        server.send(&msgs.into()).await.unwrap();
        for code in ['C', 'Z', 'C', 'Z', '1', 't', 'n', '2', 'C', 'Z', 'C'] {
            assert!(!server.done());
            let msg = server.read().await.unwrap();
            assert_eq!(code, msg.code());
        }
        let msg = server.read().await.unwrap();
        assert_eq!(msg.code(), 'Z');
        assert!(server.done());
    }

    #[tokio::test]
    async fn test_error_in_long_chain() {
        let mut server = test_server().await;

        let msgs = vec![
            ProtocolMessage::from(Query::new("SET statement_timeout TO 5000")),
            Parse::named("test", "SELECT $1").into(),
            Parse::named("test_2", "SELECT $1, $2, $3").into(),
            Describe::new_statement("test_2").into(),
            Bind::new_params(
                "test",
                &[crate::net::bind::Parameter {
                    len: 1,
                    data: "1".as_bytes().into(),
                }],
            )
            .into(),
            Bind::new_statement("test_2").into(),
            Execute::new().into(), // Will be ignored
            Bind::new_statement("test").into(),
            Flush.into(),
        ];

        server.send(&msgs.into()).await.unwrap();

        for c in ['C', 'Z', '1', '1', 't', 'T', '2', 'E'] {
            let msg = server.read().await.unwrap();
            assert_eq!(c, msg.code());
        }

        assert!(!server.done()); // We're not in sync (extended protocol)
        assert_eq!(server.stats().get_state(), State::Idle);
        assert!(server.prepared_statements.state().queue().is_empty()); // Queue is empty
        assert!(!server.prepared_statements.state().in_sync());

        server
            .send(&vec![ProtocolMessage::from(Sync), Query::new("SELECT 1").into()].into())
            .await
            .unwrap();

        for c in ['Z', 'T', 'D', 'C', 'Z'] {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), c);
        }

        assert!(server.done());
    }

    #[tokio::test]
    async fn test_close() {
        let mut server = test_server().await;

        for _ in 0..5 {
            server
                .send(
                    &vec![
                        ProtocolMessage::from(Parse::named("test", "SELECT $1")),
                        Sync.into(),
                    ]
                    .into(),
                )
                .await
                .unwrap();

            assert!(!server.done());
            for c in ['1', 'Z'] {
                let msg = server.read().await.unwrap();
                assert_eq!(c, msg.code());
            }
            assert!(server.done());

            server
                .send(
                    &vec![
                        Bind::new_params(
                            "test",
                            &[crate::net::bind::Parameter {
                                len: 1,
                                data: "1".as_bytes().into(),
                            }],
                        )
                        .into(),
                        Execute::new().into(),
                        Close::named("test_sdf").into(),
                        ProtocolMessage::from(Parse::named("test", "SELECT $1")),
                        Sync.into(),
                    ]
                    .into(),
                )
                .await
                .unwrap();
            assert!(!server.done());
            for c in ['2', 'D', 'C', '3', '1'] {
                let msg = server.read().await.unwrap();
                assert_eq!(msg.code(), c);
                assert!(!server.done());
            }
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), 'Z');
            assert!(server.done());
        }
    }

    #[tokio::test]
    async fn test_just_sync() {
        let mut server = test_server().await;
        server
            .send(&vec![ProtocolMessage::from(Sync)].into())
            .await
            .unwrap();
        assert!(!server.done());
        let msg = server.read().await.unwrap();
        assert_eq!(msg.code(), 'Z');
        assert!(server.done());
    }

    #[tokio::test]
    async fn test_portal() {
        let mut server = test_server().await;
        server
            .send(
                &vec![
                    ProtocolMessage::from(Parse::named("test", "SELECT 1")),
                    Bind::new_name_portal("test", "test1").into(),
                    Execute::new_portal("test1").into(),
                    Close::portal("test1").into(),
                    Sync.into(),
                ]
                .into(),
            )
            .await
            .unwrap();

        for c in ['1', '2', 'D', 'C', '3'] {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), c);
            assert!(!server.done());
            assert!(server.has_more_messages());
        }
        let msg = server.read().await.unwrap();
        assert_eq!(msg.code(), 'Z');
        assert!(server.done());
        assert!(!server.has_more_messages());
    }

    #[tokio::test]
    async fn test_manual_prepared() {
        let mut server = test_server().await;

        let mut prep = PreparedStatements::new();
        let mut parse = Parse::named("test", "SELECT 1::bigint");
        prep.insert_anyway(&mut parse);
        assert_eq!(parse.name(), "__pgdog_1");

        server
            .send(
                &vec![ProtocolMessage::from(Query::new(format!(
                    "PREPARE {} AS {}",
                    parse.name(),
                    parse.query()
                )))]
                .into(),
            )
            .await
            .unwrap();
        for c in ['C', 'Z'] {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), c);
        }
        assert!(server.sync_prepared());
        server.sync_prepared_statements().await.unwrap();
        assert!(server.prepared_statements.contains("__pgdog_1"));

        let describe = Describe::new_statement("__pgdog_1");
        let bind = Bind::new_statement("__pgdog_1");
        let execute = Execute::new();
        server
            .send(
                &vec![
                    describe.clone().into(),
                    bind.into(),
                    execute.into(),
                    ProtocolMessage::from(Sync),
                ]
                .into(),
            )
            .await
            .unwrap();

        for c in ['t', 'T', '2', 'D', 'C', 'Z'] {
            let msg = server.read().await.unwrap();
            assert_eq!(c, msg.code());
        }

        let parse = Parse::named("__pgdog_1", "SELECT 2::bigint");
        let describe = describe.clone();

        server
            .send(&vec![parse.into(), describe.into(), ProtocolMessage::from(Flush)].into())
            .await
            .unwrap();

        for c in ['1', 't', 'T'] {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), c);
        }

        server
            .send(&vec![ProtocolMessage::from(Query::new("EXECUTE __pgdog_1"))].into())
            .await
            .unwrap();
        for c in ['T', 'D', 'C', 'Z'] {
            let msg = server.read().await.unwrap();
            assert_eq!(c, msg.code());
            if c == 'D' {
                let data_row = DataRow::from_bytes(msg.to_bytes().unwrap()).unwrap();
                let result: i64 = data_row.get(0, Format::Text).unwrap();
                assert_eq!(result, 1); // We prepared SELECT 1, SELECT 2 is ignored.
            }
        }
        assert!(server.done());
    }

    #[tokio::test]
    async fn test_sync_params() {
        let mut server = test_server().await;
        let mut params = Parameters::default();
        params.insert("application_name", "test_sync_params");
        let changed = server
            .link_client(&BackendKeyData::new(), &params, None)
            .await
            .unwrap();
        assert_eq!(changed, 1);

        let app_name = server
            .fetch_all::<String>("SHOW application_name")
            .await
            .unwrap();
        assert_eq!(app_name[0], "test_sync_params");

        let changed = server
            .link_client(&BackendKeyData::new(), &params, None)
            .await
            .unwrap();
        assert_eq!(changed, 0);
    }

    #[tokio::test]
    async fn test_rollback() {
        let mut server = test_server().await;
        server
            .send(&vec![Query::new("COMMIT").into()].into())
            .await
            .unwrap();
        loop {
            let msg = server.read().await.unwrap();
            if msg.code() == 'Z' {
                break;
            }
            println!("{:?}", msg);
        }
    }

    #[tokio::test]
    async fn test_partial_state() -> Result<(), Box<dyn std::error::Error>> {
        crate::logger();
        let mut server = test_server().await;

        server
            .send(
                &vec![
                    Parse::named("test", "SELECT $1").into(),
                    Describe::new_statement("test").into(),
                    Flush.into(),
                ]
                .into(),
            )
            .await?;

        for c in ['1', 't', 'T'] {
            let msg = server.read().await?;
            assert_eq!(msg.code(), c);
            if c == 'T' {
                assert!(server.done());
            } else {
                assert!(!server.done());
            }
        }

        assert!(server.needs_drain());
        assert!(!server.has_more_messages());
        assert!(server.done());
        server.drain().await.unwrap();
        assert!(server.in_sync());
        assert!(server.done());
        assert!(!server.needs_drain());

        server
            .send(
                &vec![
                    Query::new("BEGIN").into(),
                    Query::new("syntax error").into(),
                ]
                .into(),
            )
            .await?;

        for c in ['C', 'Z', 'E'] {
            let msg = server.read().await?;
            assert_eq!(msg.code(), c);
        }

        assert!(!server.needs_drain());
        assert!(server.in_transaction());
        server.drain().await.unwrap(); // Nothing will be done.
        assert!(server.in_transaction());
        server.rollback().await.unwrap();
        assert!(server.in_sync());
        assert!(!server.in_transaction());

        Ok(())
    }

    #[tokio::test]
    async fn test_params_sync() -> Result<(), Box<dyn std::error::Error>> {
        let mut params = Parameters::default();
        params.insert("application_name", "test");

        let mut server = test_server().await;

        let changed = server
            .link_client(&BackendKeyData::new(), &params, None)
            .await?;
        assert_eq!(changed, 1);

        let changed = server
            .link_client(&BackendKeyData::new(), &params, None)
            .await?;
        assert_eq!(changed, 0);

        for i in 0..25 {
            let value = format!("apples_{}", i);
            params.insert("application_name", value);

            let changed = server
                .link_client(&BackendKeyData::new(), &params, None)
                .await?;
            assert_eq!(changed, 2); // RESET, SET.

            let changed = server
                .link_client(&BackendKeyData::new(), &params, None)
                .await?;
            assert_eq!(changed, 0);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_protocol() {
        let mut server = test_server().await;

        server.execute("BEGIN").await.unwrap();
        server
            .execute("CREATE TABLE test_copy_t (id BIGINT)")
            .await
            .unwrap();

        server
            .send(&vec![Query::from("COPY test_copy_t (id) FROM STDIN CSV").into()].into())
            .await
            .unwrap();

        for c in ['G'] {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), c);
            assert!(server.has_more_messages());
            assert!(!server.prepared_statements.state().done());
        }

        server
            .send(&vec![CopyData::new(b"1\n").into()].into())
            .await
            .unwrap();

        assert!(!server.done());
        assert!(server.has_more_messages());
        assert!(!server.prepared_statements.state().done());

        server
            .send(&vec![CopyData::new(b"2\n").into(), CopyDone.into()].into())
            .await
            .unwrap();
        assert!(!server.done());
        assert!(server.has_more_messages());

        let cc = server.read().await.unwrap();
        assert_eq!(cc.code(), 'C');
        assert!(server.has_more_messages());
        assert!(!server.done());

        let rfq = server.read().await.unwrap();
        assert_eq!(rfq.code(), 'Z');
        assert!(!server.has_more_messages());
        assert!(!server.done()); // transaction

        server.execute("ROLLBACK").await.unwrap();

        assert!(server.done());
        assert!(!server.has_more_messages());
        assert!(server.in_sync());
    }

    #[tokio::test]
    async fn test_copy_protocol_fail() {
        let mut server = test_server().await;

        server.execute("BEGIN").await.unwrap();
        server
            .execute("CREATE TABLE test_copy_t (id BIGINT)")
            .await
            .unwrap();

        server
            .send(
                &vec![
                    Query::from("COPY test_copy_t (id) FROM STDIN CSV").into(),
                    CopyData::new(b"hello\n").into(),
                ]
                .into(),
            )
            .await
            .unwrap();

        for c in ['G', 'E', 'Z'] {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), c);
        }

        assert!(server.in_sync());

        server.execute("ROLLBACK").await.unwrap();
        assert!(server.in_sync());
    }

    #[tokio::test]
    async fn test_copy_client_fail() {
        let mut server = test_server().await;
        server.execute("BEGIN").await.unwrap();
        server
            .execute("CREATE TABLE IF NOT EXISTS test_copy_t (id BIGINT)")
            .await
            .unwrap();
        server
            .send(
                &vec![
                    Query::new("COPY test_copy_t(id) FROM STDIN CSV").into(),
                    CopyData::new(b"1\n").into(),
                    CopyFail::new("something went wrong").into(),
                ]
                .into(),
            )
            .await
            .unwrap();
        for c in ['G', 'E', 'Z'] {
            if c != 'Z' {
                assert!(!server.done());
                assert!(server.has_more_messages());
            }
            assert_eq!(server.read().await.unwrap().code(), c);
        }

        server.execute("ROLLBACK").await.unwrap();
    }

    #[tokio::test]
    async fn test_query_stats() {
        let mut server = test_server().await;

        assert_eq!(server.stats().last_checkout().queries, 0);
        assert_eq!(server.stats().last_checkout().transactions, 0);

        for i in 1..26 {
            server.execute("SELECT 1").await.unwrap();

            assert_eq!(server.stats().last_checkout().queries, i);
            assert_eq!(server.stats().last_checkout().transactions, i);
            assert_eq!(server.stats().total().queries, i);
            assert_eq!(server.stats().total().transactions, i);
        }

        let counts = server.stats_mut().reset_last_checkout();
        assert_eq!(counts.queries, 25);
        assert_eq!(counts.transactions, 25);

        assert_eq!(server.stats().last_checkout().queries, 0);
        assert_eq!(server.stats().last_checkout().transactions, 0);
        assert_eq!(server.stats().total().queries, 25);
        assert_eq!(server.stats().total().transactions, 25);

        for i in 1..26 {
            server.execute("BEGIN").await.unwrap();
            server.execute("SELECT 1").await.unwrap();
            server.execute("SELECT 2").await.unwrap();
            server.execute("COMMIT").await.unwrap();

            assert_eq!(server.stats().last_checkout().queries, i * 4);
            assert_eq!(server.stats().last_checkout().transactions, i);
            assert_eq!(server.stats().total().queries, 25 + (i * 4));
            assert_eq!(server.stats().total().transactions, 25 + i);
        }

        let counts = server.stats_mut().reset_last_checkout();
        assert_eq!(counts.queries, 25 * 4);
        assert_eq!(counts.transactions, 25);
        assert_eq!(server.stats().total().queries, 25 + (25 * 4));
        assert_eq!(server.stats().total().transactions, 25 + 25);
    }

    #[tokio::test]
    async fn test_anonymous_extended() {
        use crate::net::bind::Parameter;

        let buf = vec![
            Parse::new_anonymous("SELECT pg_advisory_lock($1)").into(),
            Describe::new_statement("").into(),
            Flush.into(),
        ];

        let mut server = test_server().await;

        server.send(&buf.into()).await.unwrap();

        for c in ['1', 't', 'T'] {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), c);
            if c != 'T' {
                assert!(!server.done());
            }
        }

        assert!(server.done());

        let buf = vec![
            Bind::new_params(
                "",
                &[Parameter {
                    len: 4,
                    data: "1234".as_bytes().into(),
                }],
            )
            .into(),
            Describe::new_portal("").into(),
            Execute::new_portal("").into(),
            Sync.into(),
        ];

        server.send(&buf.into()).await.unwrap();

        for c in ['2', 'T', 'D', 'C', 'Z'] {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), c);
            if c != 'Z' {
                assert!(!server.done());
            }
        }

        assert!(server.done());
    }

    #[tokio::test]
    async fn test_close_many() {
        let mut server = test_server().await;

        for _ in 0..5 {
            server
                .close_many(&[Close::named("test"), Close::named("test2")])
                .await
                .unwrap();

            let in_sync = server.fetch_all::<i64>("SELECT 1::bigint").await.unwrap();
            assert_eq!(in_sync[0], 1);

            server.prepared_statements.set_capacity(3);
            assert_eq!(server.prepared_statements.capacity(), 3);

            server
                .send(
                    &vec![
                        Parse::named("__pgdog_1", "SELECT $1::bigint").into(),
                        Parse::named("__pgdog_2", "SELECT 123").into(),
                        Parse::named("__pgdog_3", "SELECT 1234").into(),
                        Parse::named("__pgdog_4", "SELECT 12345").into(),
                        Flush.into(),
                    ]
                    .into(),
                )
                .await
                .unwrap();

            for _ in 0..4 {
                let msg = server.read().await.unwrap();
                assert_eq!(msg.code(), '1');
            }

            assert_eq!(server.prepared_statements.len(), 4);
            let extra = server.prepared_statements.ensure_capacity();
            let name = extra[0].name();
            assert!(name.starts_with("__pgdog_"));

            assert_eq!(extra.len(), 1);
            assert!(server.prepared_statements.ensure_capacity().is_empty());

            server.close_many(&extra).await.unwrap();

            server
                .send(&vec![Parse::named(name, "SELECT $1::bigint").into(), Sync.into()].into())
                .await
                .unwrap();

            for c in ['1', 'Z'] {
                let msg = server.read().await.unwrap();
                assert_eq!(msg.code(), c);
            }
        }
    }

    #[tokio::test]
    async fn test_deallocate_all_clears_cache() {
        let mut server = test_server().await;

        server
            .send(
                &vec![
                    Parse::named("__pgdog_1", "SELECT $1::bigint").into(),
                    Parse::named("__pgdog_2", "SELECT 123").into(),
                    Parse::named("__pgdog_3", "SELECT 1234").into(),
                    Parse::named("__pgdog_4", "SELECT 12345").into(),
                    Flush.into(),
                ]
                .into(),
            )
            .await
            .unwrap();

        for _ in 0..4 {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), '1');
        }

        assert_eq!(server.prepared_statements.len(), 4);

        server
            .send(&vec![Query::new("DEALLOCATE ALL").into(), Sync.into()].into())
            .await
            .unwrap();

        for c in ['C', 'Z', 'Z'] {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), c);
        }

        assert_eq!(server.prepared_statements.len(), 0);
        assert!(server.done());
    }

    #[tokio::test]
    async fn test_discard_all_clears_cache() {
        let mut server = test_server().await;

        server
            .send(
                &vec![
                    Parse::named("__pgdog_1", "SELECT $1::bigint").into(),
                    Parse::named("__pgdog_2", "SELECT 123").into(),
                    Parse::named("__pgdog_3", "SELECT 1234").into(),
                    Parse::named("__pgdog_4", "SELECT 12345").into(),
                    Flush.into(),
                ]
                .into(),
            )
            .await
            .unwrap();

        for _ in 0..4 {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), '1');
        }

        assert_eq!(server.prepared_statements.len(), 4);

        server
            .send(&vec![Query::new("DISCARD ALL").into(), Sync.into()].into())
            .await
            .unwrap();

        for c in ['C', 'Z', 'Z'] {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), c);
        }

        assert_eq!(server.prepared_statements.len(), 0);
        assert!(server.done());
    }

    #[tokio::test]
    async fn test_drain_chaos() {
        use crate::net::bind::Parameter;
        use rand::Rng;

        let mut server = test_server().await;
        let mut rng = rand::rng();

        for iteration in 0..1000 {
            let name = format!("chaos_test_{}", iteration);
            let use_sync = rng.random_bool(0.5);

            if rng.random_bool(0.2) {
                let bad_parse = Parse::named(&name, "SELECT invalid syntax");
                server
                    .send(
                        &vec![
                            ProtocolMessage::from(bad_parse),
                            ProtocolMessage::from(Bind::new_params(&name, &[])),
                            ProtocolMessage::from(Execute::new()),
                            ProtocolMessage::from(Flush),
                        ]
                        .into(),
                    )
                    .await
                    .unwrap();

                let messages_to_read = rng.random_range(0..=1);
                for _ in 0..messages_to_read {
                    let _ = server.read().await;
                }
            } else {
                let parse = Parse::named(&name, "SELECT $1, $2");
                let bind = Bind::new_params(
                    &name,
                    &[
                        Parameter {
                            len: 1,
                            data: "1".as_bytes().into(),
                        },
                        Parameter {
                            len: 1,
                            data: "2".as_bytes().into(),
                        },
                    ],
                );
                let execute = Execute::new();

                let messages = if use_sync {
                    vec![
                        ProtocolMessage::from(parse),
                        ProtocolMessage::from(bind),
                        ProtocolMessage::from(execute),
                        ProtocolMessage::from(Sync),
                    ]
                } else {
                    vec![
                        ProtocolMessage::from(parse),
                        ProtocolMessage::from(bind),
                        ProtocolMessage::from(execute),
                        ProtocolMessage::from(Flush),
                    ]
                };

                server.send(&messages.into()).await.unwrap();

                let expected_messages = if use_sync {
                    vec!['1', '2', 'D', 'C', 'Z']
                } else {
                    vec!['1', '2', 'D', 'C']
                };
                let messages_to_read = rng.random_range(0..expected_messages.len());

                for i in 0..messages_to_read {
                    let msg = server.read().await.unwrap();
                    assert_eq!(msg.code(), expected_messages[i]);
                }
            }

            server.drain().await.unwrap();

            assert!(server.in_sync(), "Server should be in sync after drain");
            assert!(!server.error(), "Server should not be in error state");
            assert!(server.done(), "Server should be done after drain");
        }
    }

    #[tokio::test]
    async fn test_idle_in_transaction() {
        let mut server = test_server().await;

        server.execute("SELECT 1").await.unwrap();
        assert_eq!(
            server.stats().total().idle_in_transaction_time,
            Duration::ZERO,
        );
        assert_eq!(
            server.stats().last_checkout().idle_in_transaction_time,
            Duration::ZERO,
        );

        server.execute("BEGIN").await.unwrap();
        assert_eq!(
            server.stats().total().idle_in_transaction_time,
            Duration::ZERO,
        );

        server.execute("SELECT 1").await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        server.execute("SELECT 2").await.unwrap();

        let idle_time = server.stats().total().idle_in_transaction_time;
        assert!(
            idle_time >= Duration::from_millis(50),
            "Expected idle time >= 50ms, got {:?}",
            idle_time
        );
        assert!(
            idle_time < Duration::from_millis(200),
            "Expected idle time < 200ms, got {:?}",
            idle_time
        );

        tokio::time::sleep(Duration::from_millis(100)).await;

        server.execute("COMMIT").await.unwrap();

        let final_idle_time = server.stats().total().idle_in_transaction_time;
        assert!(
            final_idle_time >= Duration::from_millis(150),
            "Expected final idle time >= 150ms, got {:?}",
            final_idle_time
        );
        assert!(
            final_idle_time < Duration::from_millis(400),
            "Expected final idle time < 400ms, got {:?}",
            final_idle_time
        );

        server.execute("SELECT 3").await.unwrap();
        assert_eq!(
            server.stats().total().idle_in_transaction_time,
            final_idle_time,
        );
    }

    #[tokio::test]
    async fn test_prepare_forces_sync_prepared_flag() {
        let mut server = test_server().await;

        assert!(!server.sync_prepared());

        server
            .send(&vec![Query::new("PREPARE test_stmt AS SELECT $1::bigint").into()].into())
            .await
            .unwrap();

        for c in ['C', 'Z'] {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), c);
        }

        assert!(
            server.sync_prepared(),
            "sync_prepared flag should be set after PREPARE command"
        );

        server.sync_prepared_statements().await.unwrap();

        assert!(
            !server.sync_prepared(),
            "sync_prepared flag should be cleared after sync_prepared_statements()"
        );

        server.execute("SELECT 1").await.unwrap();

        assert!(
            !server.sync_prepared(),
            "sync_prepared flag should remain false after regular queries"
        );
    }

    #[tokio::test]
    async fn test_protocol_out_of_sync_sets_error_state() {
        let mut server = test_server().await;

        server
            .send(&vec![Query::new("SELECT 1").into()].into())
            .await
            .unwrap();

        for c in ['T', 'D'] {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), c);
        }

        // simulate an unlikely, but existent out-of-sync state
        server
            .prepared_statements_mut()
            .state_mut()
            .queue_mut()
            .clear();

        let res = server.read().await;
        assert!(
            matches!(res, Err(Error::ProtocolOutOfSync)),
            "protocol should be out of sync"
        );
        assert!(
            server.stats().get_state() == State::Error,
            "state should be Error after detecting desync"
        )
    }

    #[tokio::test]
    async fn test_reset_clears_client_params() {
        let mut server = test_server().await;
        let mut params = Parameters::default();
        params.insert("application_name", "test_reset");

        // Sync params to server
        let changed = server
            .link_client(&BackendKeyData::new(), &params, None)
            .await
            .unwrap();
        assert_eq!(changed, 1);

        // Same params should not need re-sync
        let changed = server
            .link_client(&BackendKeyData::new(), &params, None)
            .await
            .unwrap();
        assert_eq!(changed, 0);

        // Execute RESET ALL which clears client_params
        server.execute("RESET ALL").await.unwrap();

        // Now link_client should need to re-sync because client_params was cleared
        let changed = server
            .link_client(&BackendKeyData::new(), &params, None)
            .await
            .unwrap();
        assert!(
            changed > 0,
            "expected re-sync after RESET ALL cleared client_params"
        );
    }

    #[tokio::test]
    async fn test_error_decoding() {
        let mut server = test_server().await;
        let err = server
            .execute(Query::new("SELECT * FROM test_error_decoding"))
            .await
            .expect_err("expected this query to fail");
        assert!(
            matches!(err, Error::ExecutionError(_)),
            "expected execution error"
        );
        if let Error::ExecutionError(err) = err {
            assert_eq!(
                err.message,
                "relation \"test_error_decoding\" does not exist"
            );
            assert_eq!(err.severity, "ERROR");
            assert_eq!(err.code, "42P01");
            assert_eq!(err.context, None);
            assert_eq!(err.routine, Some("parserOpenTable".into())); // Might break in the future.
            assert_eq!(err.detail, None);
        }
    }
    #[tokio::test]
    async fn test_copy_text_composite_type_roundtrip() {
        use crate::frontend::router::parser::{CopyFormat, CsvStream};

        let mut server = test_server().await;

        server.execute("BEGIN").await.unwrap();

        // Create the composite type and tables
        server
            .execute(
                "CREATE TYPE test_location_rt AS (
                    street varchar,
                    city varchar,
                    state varchar,
                    country varchar,
                    postal_code varchar
                )",
            )
            .await
            .unwrap();

        server
            .execute(
                "CREATE TABLE test_copy_source (
                    id INTEGER,
                    value_location test_location_rt
                )",
            )
            .await
            .unwrap();

        server
            .execute(
                "CREATE TABLE test_copy_dest (
                    id INTEGER,
                    value_location test_location_rt
                )",
            )
            .await
            .unwrap();

        // Insert a record with the composite type containing quotes
        server
            .execute(
                "INSERT INTO test_copy_source VALUES (1, ROW(NULL, 'Annapolis', 'Maryland', 'United States', NULL)::test_location_rt)",
            )
            .await
            .unwrap();

        // COPY the data out in TEXT format
        server
            .send(&vec![Query::from("COPY test_copy_source TO STDOUT").into()].into())
            .await
            .unwrap();

        // Read CopyOutResponse
        let msg = server.read().await.unwrap();
        assert_eq!(msg.code(), 'H', "Expected CopyOutResponse");

        // Read CopyData messages until CopyDone
        let mut copy_data = Vec::new();
        loop {
            let msg = server.read().await.unwrap();
            match msg.code() {
                'd' => {
                    // CopyData from server - extract the data portion (skip header)
                    let cd = CopyData::try_from(msg).unwrap();
                    copy_data.extend_from_slice(cd.data());
                }
                'c' => {
                    // CopyDone
                    break;
                }
                _ => panic!("Unexpected message code: {}", msg.code()),
            }
        }

        // Read CommandComplete and ReadyForQuery
        let msg = server.read().await.unwrap();
        assert_eq!(msg.code(), 'C');
        let msg = server.read().await.unwrap();
        assert_eq!(msg.code(), 'Z');

        // Parse the data through CsvStream (simulating what PgDog does)
        let mut csv_stream = CsvStream::new('\t', false, CopyFormat::Text, "\\N");
        csv_stream.write(&copy_data);

        let record = csv_stream.record().unwrap().expect("Should have a record");

        // The composite type field should contain quotes around "United States"
        let location_field = record.get(1).expect("Should have location field");
        assert!(
            location_field.contains("\"United States\""),
            "Location field should contain quoted 'United States': {}",
            location_field
        );

        // Re-serialize and COPY back in
        let output = record.to_string();

        // COPY the data into the destination table
        server
            .send(&vec![Query::from("COPY test_copy_dest FROM STDIN").into()].into())
            .await
            .unwrap();

        let msg = server.read().await.unwrap();
        assert_eq!(msg.code(), 'G', "Expected CopyInResponse");

        // Send the parsed and re-serialized data
        server
            .send(&vec![CopyData::new(output.as_bytes()).into(), CopyDone.into()].into())
            .await
            .unwrap();

        // Read response
        let msg = server.read().await.unwrap();
        assert_eq!(
            msg.code(),
            'C',
            "Expected CommandComplete but got: {:?}",
            msg
        );

        let msg = server.read().await.unwrap();
        assert_eq!(msg.code(), 'Z');

        // Verify the data was inserted correctly by reading it back
        server
            .send(&vec![Query::from("SELECT * FROM test_copy_dest").into()].into())
            .await
            .unwrap();

        let msg = server.read().await.unwrap();
        assert_eq!(msg.code(), 'T');
        let msg = server.read().await.unwrap();
        assert_eq!(msg.code(), 'D', "Should have a data row");
        let msg = server.read().await.unwrap();
        assert_eq!(msg.code(), 'C');
        let msg = server.read().await.unwrap();
        assert_eq!(msg.code(), 'Z');

        server.execute("ROLLBACK").await.unwrap();
    }

    #[tokio::test]
    async fn test_extended_execute_flush_not_done_without_sync() {
        let mut server = test_server().await;

        server
            .send(
                &vec![
                    Parse::named("test_no_sync", "SELECT 1").into(),
                    Describe::new_statement("test_no_sync").into(),
                    Flush.into(),
                ]
                .into(),
            )
            .await
            .unwrap();

        for c in ['1', 't', 'T'] {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), c);
        }
        assert!(server.done());

        server
            .send(
                &vec![
                    Bind::new_statement("test_no_sync").into(),
                    Execute::new().into(),
                    Flush.into(),
                ]
                .into(),
            )
            .await
            .unwrap();

        for c in ['2', 'D', 'C'] {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), c);
            assert!(!server.done());
        }

        server
            .send(&vec![ProtocolMessage::from(Sync)].into())
            .await
            .unwrap();
        let msg = server.read().await.unwrap();
        assert_eq!(msg.code(), 'Z');
        assert!(server.done());
    }

    #[tokio::test]
    async fn test_parse_describe_flush_done_without_execute() {
        let mut server = test_server().await;

        server
            .send(
                &vec![
                    Parse::named("test_no_exec", "SELECT 1").into(),
                    Describe::new_statement("test_no_exec").into(),
                    Flush.into(),
                ]
                .into(),
            )
            .await
            .unwrap();

        for c in ['1', 't'] {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), c);
            assert!(!server.done());
        }

        let msg = server.read().await.unwrap();
        assert_eq!(msg.code(), 'T');
        assert!(server.done());
    }

    #[tokio::test]
    async fn test_simple_query_not_done_until_ready_for_query() {
        let mut server = test_server().await;

        server
            .send(&vec![Query::new("SELECT 1").into()].into())
            .await
            .unwrap();

        for c in ['T', 'D', 'C'] {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), c);
            assert!(!server.done());
        }

        let msg = server.read().await.unwrap();
        assert_eq!(msg.code(), 'Z');
        assert!(server.done());
    }

    #[tokio::test]
    async fn test_portal_suspended() {
        let mut server = test_server().await;

        server
            .execute("CREATE TEMP TABLE test_portal_suspended AS SELECT generate_series(1, 5) AS n")
            .await
            .unwrap();

        server
            .send(
                &vec![
                    Parse::named("portal_test", "SELECT n FROM test_portal_suspended").into(),
                    Bind::new_name_portal("portal_test", "portal1").into(),
                    Execute::new_portal_limit("portal1", 1).into(),
                    Flush.into(),
                ]
                .into(),
            )
            .await
            .unwrap();

        let msg = server.read().await.unwrap();
        assert_eq!(msg.code(), '1');
        assert!(!server.done());
        assert!(server.has_more_messages());

        let msg = server.read().await.unwrap();
        assert_eq!(msg.code(), '2');
        assert!(!server.done());
        assert!(server.has_more_messages());

        let msg = server.read().await.unwrap();
        assert_eq!(msg.code(), 'D');
        assert!(!server.done());
        assert!(server.has_more_messages());

        let msg = server.read().await.unwrap();
        assert_eq!(msg.code(), 's');
        assert!(!server.done());
        assert!(!server.has_more_messages());
        assert!(server.needs_drain());

        server
            .send(&vec![Execute::new_portal_limit("portal1", 0).into(), Sync.into()].into())
            .await
            .unwrap();

        for _ in 0..4 {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), 'D');
            assert!(!server.done());
        }

        let msg = server.read().await.unwrap();
        assert_eq!(msg.code(), 'C');
        assert!(!server.done());
        assert!(server.has_more_messages());

        let msg = server.read().await.unwrap();
        assert_eq!(msg.code(), 'Z');
        assert!(server.done());
        assert!(!server.has_more_messages());
        assert!(!server.needs_drain());
    }

    #[tokio::test]
    async fn test_pipelined_independent_syncs() {
        use crate::net::bind::Parameter;
        let mut server = test_server().await;

        server
            .send(
                &vec![
                    Parse::named("pipe1", "SELECT $1::int AS first").into(),
                    Bind::new_params(
                        "pipe1",
                        &[Parameter {
                            len: 1,
                            data: "1".as_bytes().into(),
                        }],
                    )
                    .into(),
                    Execute::new().into(),
                    Sync.into(),
                    Parse::named("pipe2", "SELECT $1::int AS second").into(),
                    Bind::new_params(
                        "pipe2",
                        &[Parameter {
                            len: 1,
                            data: "2".as_bytes().into(),
                        }],
                    )
                    .into(),
                    Execute::new().into(),
                    Sync.into(),
                ]
                .into(),
            )
            .await
            .unwrap();

        for c in ['1', '2', 'D', 'C'] {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), c);
            assert!(!server.done());
            assert!(server.has_more_messages());
        }

        let msg = server.read().await.unwrap();
        assert_eq!(msg.code(), 'Z');
        assert!(!server.done() || server.has_more_messages());

        for c in ['1', '2', 'D', 'C'] {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), c);
            assert!(!server.done());
        }

        let msg = server.read().await.unwrap();
        assert_eq!(msg.code(), 'Z');
        assert!(server.done());
        assert!(!server.has_more_messages());
        assert!(!server.needs_drain());
    }

    #[tokio::test]
    async fn test_empty_query_extended() {
        let mut server = test_server().await;

        server
            .send(
                &vec![
                    Parse::new_anonymous("").into(),
                    Bind::new_statement("").into(),
                    Execute::new().into(),
                    Sync.into(),
                ]
                .into(),
            )
            .await
            .unwrap();

        for c in ['1', '2', 'I'] {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), c);
            assert!(!server.done());
        }

        let msg = server.read().await.unwrap();
        assert_eq!(msg.code(), 'Z');
        assert!(server.done());
        assert!(!server.needs_drain());
    }

    async fn verify_server_usable(server: &mut Server) {
        use rand::Rng;
        let expected: i32 = rand::rng().random_range(1..1_000_000);
        let result = server
            .execute(&format!("SELECT {}", expected))
            .await
            .unwrap();
        let data_row = result
            .iter()
            .find(|m| m.code() == 'D')
            .expect("expected DataRow");
        let data_row = DataRow::from_bytes(data_row.to_bytes().unwrap()).unwrap();
        let value: i32 = data_row.get(0, Format::Text).unwrap();
        assert_eq!(value, expected);
        assert!(server.done());
        assert!(server.in_sync());
    }

    #[tokio::test]
    async fn test_drain_after_zero_reads() {
        use crate::net::bind::Parameter;
        let mut server = test_server().await;

        server
            .send(
                &vec![
                    Parse::named("drain0", "SELECT $1::int").into(),
                    Bind::new_params(
                        "drain0",
                        &[Parameter {
                            len: 1,
                            data: "1".as_bytes().into(),
                        }],
                    )
                    .into(),
                    Execute::new().into(),
                    Sync.into(),
                ]
                .into(),
            )
            .await
            .unwrap();

        server.drain().await.unwrap();

        assert!(server.in_sync());
        assert!(server.done());
        assert!(!server.needs_drain());
        verify_server_usable(&mut server).await;
    }

    #[tokio::test]
    async fn test_drain_after_parse_complete() {
        use crate::net::bind::Parameter;
        let mut server = test_server().await;

        server
            .send(
                &vec![
                    Parse::named("drain1", "SELECT $1::int").into(),
                    Bind::new_params(
                        "drain1",
                        &[Parameter {
                            len: 1,
                            data: "1".as_bytes().into(),
                        }],
                    )
                    .into(),
                    Execute::new().into(),
                    Sync.into(),
                ]
                .into(),
            )
            .await
            .unwrap();

        let msg = server.read().await.unwrap();
        assert_eq!(msg.code(), '1');

        server.drain().await.unwrap();

        assert!(server.in_sync());
        assert!(server.done());
        assert!(!server.needs_drain());
        verify_server_usable(&mut server).await;
    }

    #[tokio::test]
    async fn test_drain_after_bind_complete() {
        use crate::net::bind::Parameter;
        let mut server = test_server().await;

        server
            .send(
                &vec![
                    Parse::named("drain2", "SELECT $1::int").into(),
                    Bind::new_params(
                        "drain2",
                        &[Parameter {
                            len: 1,
                            data: "1".as_bytes().into(),
                        }],
                    )
                    .into(),
                    Execute::new().into(),
                    Sync.into(),
                ]
                .into(),
            )
            .await
            .unwrap();

        for c in ['1', '2'] {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), c);
        }

        server.drain().await.unwrap();

        assert!(server.in_sync());
        assert!(server.done());
        assert!(!server.needs_drain());
        verify_server_usable(&mut server).await;
    }

    #[tokio::test]
    async fn test_drain_after_data_row() {
        use crate::net::bind::Parameter;
        let mut server = test_server().await;

        server
            .send(
                &vec![
                    Parse::named("drain3", "SELECT $1::int").into(),
                    Bind::new_params(
                        "drain3",
                        &[Parameter {
                            len: 1,
                            data: "1".as_bytes().into(),
                        }],
                    )
                    .into(),
                    Execute::new().into(),
                    Sync.into(),
                ]
                .into(),
            )
            .await
            .unwrap();

        for c in ['1', '2', 'D'] {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), c);
        }

        server.drain().await.unwrap();

        assert!(server.in_sync());
        assert!(server.done());
        assert!(!server.needs_drain());
        verify_server_usable(&mut server).await;
    }

    #[tokio::test]
    async fn test_drain_after_command_complete() {
        use crate::net::bind::Parameter;
        let mut server = test_server().await;

        server
            .send(
                &vec![
                    Parse::named("drain4", "SELECT $1::int").into(),
                    Bind::new_params(
                        "drain4",
                        &[Parameter {
                            len: 1,
                            data: "1".as_bytes().into(),
                        }],
                    )
                    .into(),
                    Execute::new().into(),
                    Sync.into(),
                ]
                .into(),
            )
            .await
            .unwrap();

        for c in ['1', '2', 'D', 'C'] {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), c);
        }

        server.drain().await.unwrap();

        assert!(server.in_sync());
        assert!(server.done());
        assert!(!server.needs_drain());
        verify_server_usable(&mut server).await;
    }

    #[tokio::test]
    async fn test_drain_after_error() {
        let mut server = test_server().await;

        server
            .send(
                &vec![
                    Parse::named("drain_err", "SELECT * FROM nonexistent_table_xyz").into(),
                    Bind::new_statement("drain_err").into(),
                    Execute::new().into(),
                    Sync.into(),
                ]
                .into(),
            )
            .await
            .unwrap();

        let msg = server.read().await.unwrap();
        assert_eq!(msg.code(), 'E');

        server.drain().await.unwrap();

        assert!(server.in_sync());
        assert!(server.done());
        assert!(!server.needs_drain());
        verify_server_usable(&mut server).await;
    }

    #[tokio::test]
    async fn test_drain_with_multiple_data_rows() {
        let mut server = test_server().await;

        server
            .execute("CREATE TEMP TABLE drain_rows AS SELECT generate_series(1, 10) AS n")
            .await
            .unwrap();

        server
            .send(
                &vec![
                    Parse::named("drain_multi", "SELECT n FROM drain_rows").into(),
                    Bind::new_statement("drain_multi").into(),
                    Execute::new().into(),
                    Sync.into(),
                ]
                .into(),
            )
            .await
            .unwrap();

        for c in ['1', '2'] {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), c);
        }

        for _ in 0..3 {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), 'D');
        }

        server.drain().await.unwrap();

        assert!(server.in_sync());
        assert!(server.done());
        assert!(!server.needs_drain());
        verify_server_usable(&mut server).await;
    }

    #[tokio::test]
    async fn test_drain_simple_query_after_row_description() {
        let mut server = test_server().await;

        server
            .send(&vec![Query::new("SELECT 1, 2, 3").into()].into())
            .await
            .unwrap();

        let msg = server.read().await.unwrap();
        assert_eq!(msg.code(), 'T');

        server.drain().await.unwrap();

        assert!(server.in_sync());
        assert!(server.done());
        assert!(!server.needs_drain());
        verify_server_usable(&mut server).await;
    }

    #[tokio::test]
    async fn test_drain_simple_query_after_data_row() {
        let mut server = test_server().await;

        server
            .send(&vec![Query::new("SELECT 1").into()].into())
            .await
            .unwrap();

        for c in ['T', 'D'] {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), c);
        }

        server.drain().await.unwrap();

        assert!(server.in_sync());
        assert!(server.done());
        assert!(!server.needs_drain());
        verify_server_usable(&mut server).await;
    }

    #[tokio::test]
    async fn test_drain_after_portal_suspended() {
        let mut server = test_server().await;

        server
            .execute("CREATE TEMP TABLE drain_portal AS SELECT generate_series(1, 5) AS n")
            .await
            .unwrap();

        server
            .send(
                &vec![
                    Parse::named("drain_susp", "SELECT n FROM drain_portal").into(),
                    Bind::new_name_portal("drain_susp", "p1").into(),
                    Execute::new_portal_limit("p1", 1).into(),
                    Sync.into(),
                ]
                .into(),
            )
            .await
            .unwrap();

        for c in ['1', '2', 'D', 's'] {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), c);
        }

        server.drain().await.unwrap();

        assert!(server.in_sync());
        assert!(server.done());
        assert!(!server.needs_drain());
        verify_server_usable(&mut server).await;
    }
}
