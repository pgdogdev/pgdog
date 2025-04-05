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
    ProtocolState, Stats,
};
use crate::net::{
    messages::{DataRow, NoticeResponse},
    parameter::Parameters,
    tls::connector,
    Parameter, Stream,
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
    changed_params: Parameters,
    stats: Stats,
    prepared_statements: PreparedStatements,
    dirty: bool,
    streaming: bool,
    schema_changed: bool,
    protocol_state: ProtocolState,
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
                    params.push(Parameter {
                        name: parameter.name,
                        value: parameter.value,
                    });
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
                    warn!("{}", notice.message);
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
            changed_params: Parameters::default(),
            stats: Stats::connect(id, addr),
            prepared_statements: PreparedStatements::new(),
            dirty: false,
            streaming: false,
            schema_changed: false,
            protocol_state: ProtocolState::default(),
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

        for message in queue {
            if let Some(message) = message {
                trace!("→ {:#?}", message);

                match self.stream().send(&message).await {
                    Ok(sent) => self.stats.send(sent),
                    Err(err) => {
                        self.stats.state(State::Error);
                        return Err(err.into());
                    }
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
                                "got: {}, extended buffer: {:?}",
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
                self.changed_params.set(&ps.name, &ps.value);
            }
            _ => (),
        }

        trace!("[{}] ← {:#?}", self.addr(), message);

        Ok(message.backend())
    }

    /// Synchronize parameters between client and server.
    pub async fn sync_params(&mut self, params: &Parameters) -> Result<(), Error> {
        let diff = params.merge(&mut self.params);
        if !diff.is_empty() {
            debug!("syncing {} params", diff.len());
            self.execute_batch(&diff.iter().map(|query| query.query()).collect::<Vec<_>>())
                .await?;
        }
        Ok(())
    }

    pub fn changed_params(&mut self) -> Parameters {
        std::mem::take(&mut self.changed_params)
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
        )
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
        let queries = queries.iter().map(Query::new).collect::<Vec<Query>>();
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

    /// Reset error state caused by schema change.
    #[inline]
    pub fn reset_schema_changed(&mut self) {
        self.schema_changed = false;
        self.prepared_statements.clear();
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
                stats: Stats::connect(id, &addr),
                prepared_statements: super::PreparedStatements::new(),
                addr,
                dirty: false,
                streaming: false,
                schema_changed: false,
                protocol_state: ProtocolState::default(),
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
            assert_eq!(server.prepared_statements.state().len(), 3);
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), 'T');
            assert_eq!(server.prepared_statements.state().len(), 2);
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), 'D');
            assert_eq!(server.prepared_statements.state().len(), 2);
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), 'C');
            assert_eq!(server.prepared_statements.state().len(), 1);
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
        let mut total = 3 * 25;
        assert_eq!(server.prepared_statements.state().len(), total);
        for _ in 0..25 {
            for c in ['T', 'D', 'C', 'Z'] {
                let msg = server.read().await.unwrap();
                assert_eq!(msg.code(), c);
            }
            total -= 3;
            assert_eq!(server.prepared_statements.state().len(), total);
        }
        assert_eq!(total, 0);
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
        assert_eq!(server.prepared_statements.state().len(), 3);

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
        assert_eq!(server.prepared_statements.state().len(), 3);

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

            assert_eq!(server.prepared_statements.state().len(), 4);

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
            let parse = Parse::named(&name, &format!("SELECT $1, 'test_{}'", name));
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
            assert_eq!(server.prepared_statements.state().len(), 3);

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
            assert_eq!(server.prepared_statements.state().len(), 2);
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

        for i in 0..25 {
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

            assert_eq!(
                server.prepared_statements.state().len(),
                if i == 0 { 4 } else { 3 }
            );

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
}
