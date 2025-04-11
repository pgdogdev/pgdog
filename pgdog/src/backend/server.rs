//! PostgreSQL server connection.
use std::time::{Duration, Instant};

use bytes::{BufMut, BytesMut};
use rustls_pki_types::ServerName;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    spawn,
};
use tracing::{debug, error, info, trace, warn};

use super::{
    pool::Address, prepared_statements::HandleResult, Error, PreparedStatements, ProtocolMessage,
    Stats,
};
use crate::net::{
    messages::{DataRow, NoticeResponse},
    parameter::Parameters,
    tls::connector,
    CommandComplete, Parameter, Stream,
};
use crate::state::State;
use crate::{
    auth::{md5, scram::Client},
    net::messages::{
        hello::SslReply, Authentication, BackendKeyData, ErrorResponse, FromBytes, Message,
        ParameterStatus, Password, Protocol, Query, ReadyForQuery, Startup, Terminate, ToBytes,
    },
};

/// PostgreSQL server connection.
#[derive(Debug)]
pub struct Server {
    addr: Address,
    stream: Option<Stream>,
    id: BackendKeyData,
    params: Parameters,
    original_params: Parameters,
    changed_params: Parameters,
    stats: Stats,
    prepared_statements: PreparedStatements,
    dirty: bool,
    streaming: bool,
    schema_changed: bool,
    sync_prepared: bool,
}

impl Server {
    /// Create new PostgreSQL server connection.
    pub async fn connect(addr: &Address, params: Vec<Parameter>) -> Result<Self, Error> {
        debug!("=> {}", addr);
        let stream = TcpStream::connect(addr.addr()).await?;

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
            .write_all(&Startup::new(&addr.user, &addr.database_name, params).to_bytes()?)
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

        let mut params = Parameters::default();
        let mut key_data: Option<BackendKeyData> = None;

        loop {
            let message = stream.read().await?;

            match message.code() {
                // ReadyForQuery (B)
                'Z' => break,
                // ParameterStatus (B)
                'S' => {
                    let parameter = ParameterStatus::from_bytes(message.payload())?;
                    params.insert(parameter.name, parameter.value);
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

        info!("new server connection [{}]", addr);

        Ok(Server {
            addr: addr.clone(),
            stream: Some(stream),
            id,
            original_params: params.clone(),
            params,
            changed_params: Parameters::default(),
            stats: Stats::connect(id, addr),
            prepared_statements: PreparedStatements::new(),
            dirty: false,
            streaming: false,
            schema_changed: false,
            sync_prepared: false,
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

    /// Send messages to the server and flush the buffer.
    pub async fn send(&mut self, messages: Vec<impl Into<ProtocolMessage>>) -> Result<(), Error> {
        let timer = Instant::now();
        for message in messages {
            self.send_one(message).await?;
        }
        self.flush().await?;
        trace!(
            "request flushed to server [{:.4}ms]",
            timer.elapsed().as_secs_f64() * 1000.0
        );
        Ok(())
    }

    /// Send one message to the server but don't flush the buffer,
    /// accelerating bulk transfers.
    pub async fn send_one(&mut self, message: impl Into<ProtocolMessage>) -> Result<(), Error> {
        self.stats.state(State::Active);
        let message: ProtocolMessage = message.into();
        let result = self.prepared_statements.handle(&message)?;

        let queue = match result {
            HandleResult::Drop => [None, None],
            HandleResult::Prepend(prepare) => [Some(prepare), Some(message)],
            HandleResult::Forward => [Some(message), None],
        };

        for message in queue.into_iter().flatten() {
            trace!("{:#?} → [{}]", message, self.addr());

            match self.stream().send(&message).await {
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
            match self.stream().read().await {
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
                self.stats.query();

                let rfq = ReadyForQuery::from_bytes(message.payload())?;

                match rfq.status {
                    'I' => self.stats.transaction(),
                    'T' => self.stats.state(State::IdleInTransaction),
                    'E' => self.stats.transaction_error(),
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
                match cmd.command.as_str() {
                    "PREPARE" | "DEALLOCATE" => self.sync_prepared = true,
                    _ => (),
                }
            }
            _ => (),
        }

        trace!("{:#?} ← [{}]", message, self.addr());

        Ok(message.backend())
    }

    /// Synchronize parameters between client and server.
    pub async fn sync_params(&mut self, params: &Parameters) -> Result<usize, Error> {
        let diff = params.merge(&mut self.params);
        if diff.changed_params > 0 {
            debug!("syncing {} params", diff.changed_params);
            self.execute_batch(
                &diff
                    .queries
                    .iter()
                    .map(|query| query.query())
                    .collect::<Vec<_>>(),
            )
            .await?;
        }
        Ok(diff.changed_params)
    }

    pub fn changed_params(&self) -> &Parameters {
        &self.changed_params
    }

    pub fn reset_changed_params(&mut self) {
        self.changed_params.clear();
    }

    /// Server sent everything.
    #[inline]
    pub fn done(&self) -> bool {
        self.prepared_statements.done() && !self.in_transaction()
    }

    #[inline]
    pub fn has_more_messages(&self) -> bool {
        !matches!(
            self.stats.state,
            State::Idle | State::IdleInTransaction | State::TransactionError
        ) || !self.prepared_statements.done()
    }

    /// Server connection is synchronized and can receive more messages.
    #[inline]
    pub fn in_sync(&self) -> bool {
        self.prepared_statements.done() && !self.streaming
    }

    /// Server is still inside a transaction.
    #[inline]
    pub fn in_transaction(&self) -> bool {
        matches!(
            self.stats.state,
            State::IdleInTransaction | State::TransactionError
        )
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

    pub fn sync_prepared(&self) -> bool {
        self.sync_prepared
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

        #[cfg(debug_assertions)]
        for query in queries {
            debug!("{} [{}]", query, self.addr());
        }

        let mut messages = vec![];
        let queries = queries.iter().map(Query::new).collect::<Vec<Query>>();
        let expected = queries.len();

        self.send(queries).await?;

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
    pub async fn execute(&mut self, query: &str) -> Result<Vec<Message>, Error> {
        debug!("[{}] {} ", self.addr(), query,);
        self.execute_batch(&[query]).await
    }

    /// Execute query and raise an error if one is returned by PostgreSQL.
    pub async fn execute_checked(&mut self, query: &str) -> Result<Vec<Message>, Error> {
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
    pub async fn fetch_all<T: From<DataRow>>(&mut self, query: &str) -> Result<Vec<T>, Error> {
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

        if !self.in_sync() {
            self.stats.state(State::Error);
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
        self.params = self.original_params.clone();
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
mod test {
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
                original_params: Parameters::default(),
                stats: Stats::connect(id, &addr),
                prepared_statements: super::PreparedStatements::new(),
                addr,
                dirty: false,
                streaming: false,
                schema_changed: false,
                sync_prepared: false,
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

    async fn test_server() -> Server {
        let address = Address {
            host: "127.0.0.1".into(),
            port: 5432,
            user: "pgdog".into(),
            password: "pgdog".into(),
            database_name: "pgdog".into(),
        };

        Server::connect(&address, vec![]).await.unwrap()
    }

    #[tokio::test]
    async fn test_simple_query() {
        let mut server = test_server().await;
        for _ in 0..25 {
            server
                .send(vec![ProtocolMessage::from(Query::new("SELECT 1"))])
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
                .send(vec![ProtocolMessage::from(Query::new("SELECT 1"))])
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
            .send(vec![ProtocolMessage::from(empty)])
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
            .send(vec![ProtocolMessage::from(Query::new(
                "SET application_name TO 'test'",
            ))])
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
            let bind = Bind {
                params: vec![Parameter {
                    len: 1,
                    data: "1".as_bytes().to_vec(),
                }],
                codes: vec![0],
                ..Default::default()
            };
            server
                .send(vec![
                    ProtocolMessage::from(Parse::new_anonymous("SELECT $1")),
                    ProtocolMessage::from(bind),
                    ProtocolMessage::from(Execute::new()),
                    ProtocolMessage::from(Sync::new()),
                ])
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
            let bind = Bind {
                statement: name.clone(),
                params: vec![Parameter {
                    len: 1,
                    data: "1".as_bytes().to_vec(),
                }],
                ..Default::default()
            };

            server
                .send(vec![
                    ProtocolMessage::from(parse.clone()),
                    ProtocolMessage::from(describe.clone()),
                    Flush {}.into(),
                ])
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
                .send(vec![
                    ProtocolMessage::from(describe.clone()),
                    ProtocolMessage::from(Flush),
                ])
                .await
                .unwrap();
            for code in ['t', 'T'] {
                let msg = server.read().await.unwrap();
                assert_eq!(msg.code(), code);
            }

            assert_eq!(server.prepared_statements.state().len(), 0);

            server
                .send(vec![
                    ProtocolMessage::from(bind.clone()),
                    ProtocolMessage::from(Execute::new()),
                    ProtocolMessage::from(Sync {}),
                ])
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
                .send(vec![
                    ProtocolMessage::from(Bind {
                        statement: "__pgdog_1".into(),
                        params: vec![Parameter {
                            len: 1,
                            data: "1".as_bytes().to_vec(),
                        }],
                        ..Default::default()
                    }),
                    Execute::new().into(),
                    Sync {}.into(),
                ])
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
                .send(vec![
                    ProtocolMessage::from(parse),
                    Describe::new_statement("test").into(),
                    Sync {}.into(),
                ])
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
            let bind = Bind {
                statement: name.clone(),
                ..Default::default() // Missing params.
            };
            server
                .send(vec![
                    ProtocolMessage::from(parse),
                    describe.into(),
                    bind.into(),
                    Sync.into(),
                ])
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
                .send(vec![
                    ProtocolMessage::from(parse.clone()),
                    describe.clone().into(),
                    Flush.into(),
                ])
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
            .send(vec![ProtocolMessage::from(parse.clone()), Sync.into()])
            .await
            .unwrap();
        for c in ['E', 'Z'] {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), c);
        }
        assert!(server.prepared_statements.is_empty());

        server
            .send(vec![
                ProtocolMessage::from(Parse::named("test", "SELECT $1")),
                Flush.into(),
            ])
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
        server.send(vec![ProtocolMessage::from(q)]).await.unwrap();
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
            Bind {
                statement: "test_1".into(),
                params: vec![crate::net::bind::Parameter {
                    len: 1,
                    data: "1".as_bytes().to_vec(),
                }],
                ..Default::default()
            }
            .into(),
            Describe {
                statement: "".into(),
                kind: 'P',
            }
            .into(),
            Execute::new().into(),
            Sync.into(),
            Query::new("COMMIT").into(),
        ];
        server.send(msgs).await.unwrap();

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
            Bind {
                statement: "test".into(),
                ..Default::default()
            }
            .into(),
            Execute::new().into(),
            Sync.into(),
            Query::new("ROLLBACK").into(),
        ];

        server.send(msgs).await.unwrap();
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
            Bind {
                statement: "test".into(),
                params: vec![crate::net::bind::Parameter {
                    len: 1,
                    data: "1".as_bytes().to_vec(),
                }],
                ..Default::default()
            }
            .into(),
            Bind {
                // Should error out
                statement: "test_2".into(),
                ..Default::default()
            }
            .into(),
            Execute::new().into(), // Will be ignored
            Bind {
                // Will be ignored
                statement: "test".into(),
                ..Default::default()
            }
            .into(),
            Flush.into(),
        ];

        server.send(msgs).await.unwrap();

        for c in ['C', 'Z', '1', '1', 't', 'T', '2', 'E'] {
            let msg = server.read().await.unwrap();
            assert_eq!(c, msg.code());
        }

        assert!(!server.done()); // We're not in sync (extended protocol)
        assert_eq!(server.stats().state, State::Idle);
        assert!(server.prepared_statements.state().queue().is_empty()); // Queue is empty
        assert!(!server.prepared_statements.state().in_sync());

        server
            .send(vec![
                ProtocolMessage::from(Sync),
                Query::new("SELECT 1").into(),
            ])
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
                .send(vec![
                    ProtocolMessage::from(Parse::named("test", "SELECT $1")),
                    Sync.into(),
                ])
                .await
                .unwrap();

            assert!(!server.done());
            for c in ['1', 'Z'] {
                let msg = server.read().await.unwrap();
                assert_eq!(c, msg.code());
            }
            assert!(server.done());

            server
                .send(vec![
                    Bind {
                        statement: "test".into(),
                        params: vec![crate::net::bind::Parameter {
                            len: 1,
                            data: "1".as_bytes().to_vec(),
                        }],
                        ..Default::default()
                    }
                    .into(),
                    Execute::new().into(),
                    Close::named("test_sdf").into(),
                    ProtocolMessage::from(Parse::named("test", "SELECT $1")),
                    Sync.into(),
                ])
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
            .send(vec![ProtocolMessage::from(Sync)])
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
            .send(vec![
                ProtocolMessage::from(Parse::named("test", "SELECT 1")),
                Bind {
                    statement: "test".into(),
                    portal: "test1".into(),
                    ..Default::default()
                }
                .into(),
                Execute::new_portal("test1").into(),
                Close::portal("test1").into(),
                Sync.into(),
            ])
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
            .send(vec![ProtocolMessage::from(Query::new(format!(
                "PREPARE {} AS {}",
                parse.name(),
                parse.query()
            )))])
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
        let bind = Bind {
            statement: "__pgdog_1".into(),
            ..Default::default()
        };
        let execute = Execute::new();
        server
            .send(vec![
                describe.clone().into(),
                bind.into(),
                execute.into(),
                ProtocolMessage::from(Sync),
            ])
            .await
            .unwrap();

        for c in ['t', 'T', '2', 'D', 'C', 'Z'] {
            let msg = server.read().await.unwrap();
            assert_eq!(c, msg.code());
        }

        let parse = Parse::named("__pgdog_1", "SELECT 2::bigint");
        let describe = describe.clone();

        server
            .send(vec![
                parse.into(),
                describe.into(),
                ProtocolMessage::from(Flush),
            ])
            .await
            .unwrap();

        for c in ['1', 't', 'T'] {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), c);
        }

        server
            .send(vec![ProtocolMessage::from(Query::new("EXECUTE __pgdog_1"))])
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
        params.insert("application_name".into(), "test_sync_params".into());
        let changed = server.sync_params(&params).await.unwrap();
        assert_eq!(changed, 1);

        let app_name = server
            .fetch_all::<String>("SHOW application_name")
            .await
            .unwrap();
        assert_eq!(app_name[0], "test_sync_params");

        let changed = server.sync_params(&params).await.unwrap();
        assert_eq!(changed, 0);
    }
}
