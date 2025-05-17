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
    pool::Address, prepared_statements::HandleResult, Error, PreparedStatements, ProtocolMessage,
    ServerOptions, Stats,
};
use crate::{
    auth::{md5, scram::Client},
    frontend::Buffer,
    net::{
        messages::{
            hello::SslReply, Authentication, BackendKeyData, ErrorResponse, FromBytes, Message,
            ParameterStatus, Password, Protocol, Query, ReadyForQuery, Startup, Terminate, ToBytes,
        },
        Parameter, Sync,
    },
};
use crate::{
    config::PoolerMode,
    net::{
        messages::{DataRow, NoticeResponse},
        parameter::Parameters,
        tls::connector,
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
    pooler_mode: PoolerMode,
    stream_buffer: BytesMut,
}

impl Server {
    /// Create new PostgreSQL server connection.
    pub async fn connect(addr: &Address, options: ServerOptions) -> Result<Self, Error> {
        debug!("=> {}", addr);
        let stream = TcpStream::connect(addr.addr()).await?;
        tweak(&stream)?;

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
                    let notice = NoticeResponse::from_bytes(message.payload())?;
                    warn!("{} [{}]", notice.message, addr);
                }

                code => return Err(Error::UnexpectedMessage(code)),
            }
        }

        let id = key_data.ok_or(Error::NoBackendKeyData)?;
        let params: Parameters = params.into();

        info!("new server connection [{}]", addr);

        Ok(Server {
            addr: addr.clone(),
            stream: Some(stream),
            id,
            stats: Stats::connect(id, addr, &params),
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
        })
    }

    /// Request query cancellation for the given backend server identifier.
    pub async fn cancel(addr: &Address, id: &BackendKeyData) -> Result<(), Error> {
        let mut stream = TcpStream::connect(addr.addr()).await?;
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
    pub async fn send(&mut self, messages: &Buffer) -> Result<(), Error> {
        self.stats.state(State::Active);

        for message in messages.iter() {
            self.send_one(message).await?;
        }
        self.flush().await?;

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

        for message in queue.into_iter().flatten() {
            trace!(">>> {:?} [{}]", message, self.addr());

            match self.stream().send(message).await {
                Ok(sent) => self.stats.send(sent),
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
    pub async fn read(&mut self) -> Result<Message, Error> {
        let message = loop {
            if let Some(message) = self.prepared_statements.state_mut().get_simulated() {
                return Ok(message);
            }
            match self
                .stream
                .as_mut()
                .unwrap()
                .read_buf(&mut self.stream_buffer)
                .await
            {
                Ok(message) => {
                    let message = message.stream(self.streaming).backend();
                    match self.prepared_statements.forward(&message) {
                        Ok(forward) => {
                            if forward {
                                break message;
                            }
                        }
                        Err(err) => {
                            error!(
                                "{:?} got: {}, extended buffer: {:?}",
                                err,
                                message.code(),
                                self.prepared_statements.state(),
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

        self.stats.receive(message.len());

        match message.code() {
            'Z' => {
                let now = Instant::now();
                self.stats.query(now);

                let rfq = ReadyForQuery::from_bytes(message.payload())?;

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

                self.streaming = false;
            }
            'E' => {
                let error = ErrorResponse::from_bytes(message.to_bytes()?)?;
                self.schema_changed = error.code == "0A000";
                self.stats.error()
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
                    "RESET" => self.client_params.clear(), // Someone reset params, we're gonna need to re-sync.
                    _ => (),
                }
            }
            'G' => self.stats.copy_mode(),
            '1' => self.stats.parse_complete(),
            '2' => self.stats.bind_complete(),
            _ => (),
        }

        trace!("<<< {:?} [{}]", message, self.addr());

        Ok(message.backend())
    }

    /// Synchronize parameters between client and server.
    pub async fn link_client(&mut self, params: &Parameters) -> Result<usize, Error> {
        // Sync application_name parameter
        // and update it in the stats.
        let default_name = "PgDog";
        let server_name = self
            .client_params
            .get_default("application_name", default_name);
        let client_name = params.get_default("application_name", default_name);
        self.stats.link_client(client_name, server_name);

        // Clear any params previously tracked by SET.
        self.changed_params.clear();

        // Compare client and server params.
        if !params.identical(&self.client_params) {
            debug!("params are different");
            let tracked = params.tracked();
            let mut queries = self.client_params.reset_queries();
            queries.extend(tracked.set_queries());
            if !queries.is_empty() {
                debug!("syncing {} params", queries.len());
                self.execute_batch(&queries).await?;
            }
            self.client_params = tracked;
            Ok(queries.len())
        } else {
            Ok(0)
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
        self.prepared_statements.done() && !self.in_transaction()
    }

    /// Server can execute a query.
    pub fn in_sync(&self) -> bool {
        matches!(
            self.stats.state,
            State::Idle | State::IdleInTransaction | State::TransactionError
        )
    }

    /// Server is done executing all queries and is
    /// not inside a transaction.
    pub fn can_check_in(&self) -> bool {
        self.stats.state == State::Idle
    }

    /// Server hasn't sent all messages yet.
    pub fn has_more_messages(&self) -> bool {
        !self.prepared_statements.done() || self.streaming
    }

    /// Server is still inside a transaction.
    #[inline]
    pub fn in_transaction(&self) -> bool {
        self.in_transaction
    }

    /// The server connection permanently failed.
    #[inline]
    pub fn error(&self) -> bool {
        self.stats.state == State::Error
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

    /// Close the connection, don't do any recovery.
    pub fn force_close(&self) -> bool {
        self.stats.state == State::ForceClose
    }

    /// Server parameters.
    #[inline]
    pub fn params(&self) -> &Parameters {
        &self.params
    }

    /// Execute a batch of queries and return all results.
    pub async fn execute_batch(&mut self, queries: &[Query]) -> Result<Vec<Message>, Error> {
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
                let err = ErrorResponse::from_bytes(message.to_bytes()?)?;
                return Err(Error::ExecutionError(Box::new(err)));
            }
            messages.push(message);
        }

        Ok(messages)
    }

    /// Execute a query on the server and return the result.
    pub async fn execute(&mut self, query: impl Into<Query>) -> Result<Vec<Message>, Error> {
        let query = query.into();
        debug!("[{}] {} ", self.addr(), query.query(),);
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
    pub async fn rollback(&mut self) {
        if self.in_transaction() {
            if let Err(_err) = self.execute("ROLLBACK").await {
                self.stats.state(State::Error);
            }
            self.stats.rollback();
        }

        if !self.done() {
            self.stats.state(State::Error);
        }
    }

    pub async fn drain(&mut self) {
        while self.has_more_messages() {
            if self.read().await.is_err() {
                self.stats.state(State::Error);
                break;
            }
        }

        if !self.in_sync() {
            if self
                .send(&vec![ProtocolMessage::Sync(Sync)].into())
                .await
                .is_err()
            {
                self.stats.state(State::Error);
                return;
            }
            while !self.in_sync() {
                if self.read().await.is_err() {
                    self.stats.state(State::Error);
                    break;
                }
            }

            self.re_synced = true;
        }
    }

    pub async fn sync_prepared_statements(&mut self) -> Result<(), Error> {
        let names = self
            .fetch_all::<String>("SELECT name FROM pg_prepared_statements")
            .await?;

        for name in names {
            self.prepared_statements.prepared(&name);
        }

        debug!("prepared statements synchronized [{}]", self.addr());

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

    /// Server connection unique identifier.
    #[inline]
    pub fn id(&self) -> &BackendKeyData {
        &self.id
    }

    /// How old this connection is.
    #[inline]
    pub fn age(&self, instant: Instant) -> Duration {
        instant.duration_since(self.stats.created_at)
    }

    /// How long this connection has been idle.
    #[inline]
    pub fn idle_for(&self, instant: Instant) -> Duration {
        instant.duration_since(self.stats.last_used)
    }

    /// How long has it been since the last connection healthcheck.
    #[inline]
    pub fn healthcheck_age(&self, instant: Instant) -> Duration {
        if let Some(last_healthcheck) = self.stats.last_healthcheck {
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

    /// Server has been cleaned.
    #[inline]
    pub(super) fn cleaned(&mut self) {
        self.dirty = false;
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
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stats.disconnect();
        if let Some(mut stream) = self.stream.take() {
            // If you see a lot of these, tell your clients
            // to not send queries unless they are willing to stick
            // around for results.
            let out_of_sync = if self.done() {
                " ".into()
            } else {
                format!(" {} ", self.stats.state)
            };
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
pub mod test {
    use crate::{frontend::PreparedStatements, net::*};

    use super::*;

    impl Default for Server {
        fn default() -> Self {
            let id = BackendKeyData::default();
            let addr = Address::default();
            Self {
                stream: None,
                id,
                params: Parameters::default(),
                changed_params: Parameters::default(),
                client_params: Parameters::default(),
                stats: Stats::connect(id, &addr, &Parameters::default()),
                prepared_statements: super::PreparedStatements::new(),
                addr,
                dirty: false,
                streaming: false,
                schema_changed: false,
                sync_prepared: false,
                in_transaction: false,
                re_synced: false,
                pooler_mode: PoolerMode::Transaction,
                stream_buffer: BytesMut::with_capacity(1024),
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
        let address = Address {
            host: "127.0.0.1".into(),
            port: 5432,
            user: "pgdog".into(),
            password: "pgdog".into(),
            database_name: "pgdog".into(),
        };

        Server::connect(&address, ServerOptions::default())
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
            let bind = Bind::test_params_codes(
                "",
                &[Parameter {
                    len: 1,
                    data: "1".as_bytes().to_vec(),
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

            assert!(server.done())
        }
    }

    #[tokio::test]
    async fn test_prepared() {
        let mut server = test_server().await;
        use crate::net::bind::Parameter;

        for i in 0..25 {
            let name = format!("test_prepared_{}", i);
            let parse = Parse::named(&name, format!("SELECT $1, 'test_{}'", name));
            let (new, new_name) = PreparedStatements::global().lock().insert(&parse);
            let name = new_name;
            let parse = parse.rename(&name);
            assert!(new);

            let describe = Describe::new_statement(&name);
            let bind = Bind::test_params(
                &name,
                &[Parameter {
                    len: 1,
                    data: "1".as_bytes().to_vec(),
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
        let (new, name) = global.lock().insert(&parse);
        assert!(new);
        let parse = parse.rename(&name);
        assert_eq!(parse.name(), "__pgdog_1");

        let mut server = test_server().await;

        for _ in 0..25 {
            server
                .send(
                    &vec![
                        ProtocolMessage::from(Bind::test_params(
                            "__pgdog_1",
                            &[Parameter {
                                len: 1,
                                data: "1".as_bytes().to_vec(),
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
            let parse = Parse::named("test", "SELECT bad syntax;");
            server
                .send(
                    &vec![
                        ProtocolMessage::from(parse),
                        Describe::new_statement("test").into(),
                        Sync {}.into(),
                    ]
                    .into(),
                )
                .await
                .unwrap();
            for c in ['E', 'Z'] {
                let msg = server.read().await.unwrap();
                assert_eq!(msg.code(), c);
            }
            assert!(server.done());
            assert!(server.prepared_statements.is_empty());
        }
    }

    #[tokio::test]
    async fn test_bad_bind() {
        let mut server = test_server().await;
        for i in 0..25 {
            let name = format!("test_{}", i);
            let parse = Parse::named(&name, "SELECT $1");
            let describe = Describe::new_statement(&name);
            let bind = Bind::test_statement(&name);
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
            Bind::test_params(
                "test_1",
                &[crate::net::bind::Parameter {
                    len: 1,
                    data: "1".as_bytes().to_vec(),
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
            Bind::test_statement("test").into(),
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
            Bind::test_params(
                "test",
                &[crate::net::bind::Parameter {
                    len: 1,
                    data: "1".as_bytes().to_vec(),
                }],
            )
            .into(),
            Bind::test_statement("test_2").into(),
            Execute::new().into(), // Will be ignored
            Bind::test_statement("test").into(),
            Flush.into(),
        ];

        server.send(&msgs.into()).await.unwrap();

        for c in ['C', 'Z', '1', '1', 't', 'T', '2', 'E'] {
            let msg = server.read().await.unwrap();
            assert_eq!(c, msg.code());
        }

        assert!(!server.done()); // We're not in sync (extended protocol)
        assert_eq!(server.stats().state, State::Idle);
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
                        Bind::test_params(
                            "test",
                            &[crate::net::bind::Parameter {
                                len: 1,
                                data: "1".as_bytes().to_vec(),
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
                    Bind::test_name_portal("test", "test1").into(),
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
        let parse = prep.insert_anyway(Parse::named("test", "SELECT 1::bigint"));
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
        let bind = Bind::test_statement("__pgdog_1");
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
        println!("server state: {}", server.stats().state);
        let changed = server.link_client(&params).await.unwrap();
        assert_eq!(changed, 1);

        let app_name = server
            .fetch_all::<String>("SHOW application_name")
            .await
            .unwrap();
        assert_eq!(app_name[0], "test_sync_params");

        let changed = server.link_client(&params).await.unwrap();
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
        server.drain().await;
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
        server.drain().await; // Nothing will be done.
        assert!(server.in_transaction());
        server.rollback().await;
        assert!(server.in_sync());
        assert!(!server.in_transaction());

        Ok(())
    }

    #[tokio::test]
    async fn test_params_sync() -> Result<(), Box<dyn std::error::Error>> {
        let mut params = Parameters::default();
        params.insert("application_name", "test");

        let mut server = test_server().await;

        let changed = server.link_client(&params).await?;
        assert_eq!(changed, 1);

        let changed = server.link_client(&params).await?;
        assert_eq!(changed, 0);

        for i in 0..25 {
            let value = format!("apples_{}", i);
            params.insert("application_name", value);

            let changed = server.link_client(&params).await?;
            assert_eq!(changed, 2); // RESET, SET.

            let changed = server.link_client(&params).await?;
            assert_eq!(changed, 0);
        }

        Ok(())
    }
}
