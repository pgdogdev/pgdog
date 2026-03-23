use std::{fmt::Debug, ops::Deref};

use bytes::{BufMut, Bytes, BytesMut};
use pgdog_config::RewriteMode;
use rand::{rng, Rng};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use crate::{
    backend::databases::{reload_from_existing, shutdown},
    config::{config, load_test_replicas, load_test_sharded, set},
    frontend::{
        client::query_engine::QueryEngine,
        router::{parser::Shard, sharding::ContextBuilder},
        Client,
    },
    net::{BackendKeyData, ErrorResponse, Message, Parameters, Protocol, Stream},
};

/// Try to convert a Message to the specified type.
/// If conversion fails and the message is an ErrorResponse, panic with its contents.
#[cfg(test)]
#[macro_export]
macro_rules! expect_message {
    ($message:expr, $ty:ty) => {{
        use $crate::net::Protocol;
        let message: $crate::net::Message = $message;
        match <$ty as TryFrom<$crate::net::Message>>::try_from(message.clone()) {
            Ok(val) => val,
            Err(_) => {
                match <crate::net::ErrorResponse as TryFrom<crate::net::Message>>::try_from(
                    message.clone(),
                ) {
                    Ok(err) => panic!("expected {}, got ErrorResponse: {:?}", stringify!($ty), err),
                    Err(_) => panic!(
                        "expected {}, got message with code '{}'",
                        stringify!($ty),
                        message.code()
                    ),
                }
            }
        }
    }};
}

/// Read one protocol message from a TCP stream.
pub async fn read_message(conn: &mut TcpStream) -> Message {
    let code = conn.read_u8().await.expect("code");
    let len = conn.read_i32().await.expect("len");
    let mut rest = vec![0u8; len as usize - 4];
    conn.read_exact(&mut rest).await.expect("read_exact");

    let mut payload = BytesMut::new();
    payload.put_u8(code);
    payload.put_i32(len);
    payload.put(Bytes::from(rest));

    Message::new(payload.freeze()).backend(BackendKeyData::default())
}

/// Send a protocol message to a TCP stream.
pub async fn send_message(conn: &mut TcpStream, message: impl Protocol) {
    let message = message.to_bytes().expect("message to convert to bytes");
    conn.write_all(&message).await.expect("write_all");
    conn.flush().await.expect("flush");
}

/// Read messages until the given code appears.
pub async fn read_until(conn: &mut TcpStream, code: char) -> Result<Vec<Message>, ErrorResponse> {
    let mut result = vec![];
    loop {
        let message = read_message(conn).await;
        result.push(message.clone());

        if message.code() == code {
            break;
        }

        if message.code() == 'E' && code != 'E' {
            let error = ErrorResponse::try_from(message).unwrap();
            return Err(error);
        }
    }

    Ok(result)
}

/// Create a loopback TCP pair and a `Client` connected to one end.
async fn new_client_pair(params: Parameters) -> (TcpStream, Client) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let connect_handle = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let stream = Stream::plain(stream, 4096);
        Client::new_test(stream, params)
    });

    let conn = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();
    let client = connect_handle.await.unwrap();

    (conn, client)
}

/// Test client.
#[derive(Debug)]
pub struct TestClient {
    pub(crate) client: Client,
    pub(crate) engine: QueryEngine,
    pub(crate) conn: TcpStream,
}

impl TestClient {
    /// Create new test client after the login phase
    /// is complete.
    ///
    /// Config needs to be loaded.
    ///
    async fn new(params: Parameters) -> Self {
        let (conn, client) = new_client_pair(params).await;

        Self {
            conn,
            engine: QueryEngine::from_client(&client).expect("create query engine from client"),
            client,
        }
    }

    /// New sharded client with parameters.
    pub(crate) async fn new_sharded(params: Parameters) -> Self {
        load_test_sharded();
        Self::new(params).await
    }

    /// New client with replicas but not sharded.
    #[allow(dead_code)]
    pub(crate) async fn new_replicas(params: Parameters) -> Self {
        load_test_replicas();
        Self::new(params).await
    }

    /// New client with cross-shard-queries disabled.
    pub(crate) async fn new_cross_shard_disabled(params: Parameters) -> Self {
        load_test_sharded();

        let mut config = config().deref().clone();
        config.config.general.cross_shard_disabled = true;
        set(config).unwrap();
        reload_from_existing().unwrap();

        Self::new(params).await
    }

    /// Create client that will rewrite all queries.
    pub(crate) async fn new_rewrites(params: Parameters) -> Self {
        load_test_sharded();

        let mut config = config().deref().clone();
        config.config.rewrite.enabled = true;
        config.config.rewrite.shard_key = RewriteMode::Rewrite;
        config.config.rewrite.split_inserts = RewriteMode::Rewrite;

        set(config).unwrap();
        reload_from_existing().unwrap();

        Self::new(params).await
    }

    /// Send message to client.
    pub(crate) async fn send(&mut self, message: impl Protocol) {
        send_message(&mut self.conn, message).await;
    }

    /// Send a simple query and panic on any errors.
    pub(crate) async fn send_simple(&mut self, message: impl Protocol) {
        self.try_send_simple(message).await.unwrap()
    }

    /// Try to send a simple query and return the error, if any.
    pub(crate) async fn try_send_simple(
        &mut self,
        message: impl Protocol,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.send(message).await;
        self.try_process().await
    }

    /// Read a message received from the servers.
    pub(crate) async fn read(&mut self) -> Message {
        read_message(&mut self.conn).await
    }

    /// Inspect client state.
    pub(crate) fn client(&mut self) -> &mut Client {
        &mut self.client
    }

    /// Process a request.
    pub(crate) async fn try_process(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.engine.set_test_mode(false);
        self.client.buffer(self.engine.stats().state).await?;
        self.client.client_messages(&mut self.engine).await?;
        self.engine.set_test_mode(true);

        Ok(())
    }

    /// Read all messages until an expected last message.
    pub(crate) async fn read_until(&mut self, code: char) -> Result<Vec<Message>, ErrorResponse> {
        read_until(&mut self.conn, code).await
    }

    /// Check if the backend is connected.
    pub(crate) fn backend_connected(&mut self) -> bool {
        self.engine.backend().connected()
    }

    /// Check if the backend is locked to this client.
    pub(crate) fn backend_locked(&mut self) -> bool {
        self.engine.backend().locked()
    }

    /// Generate a random ID for a given shard.
    pub(crate) fn random_id_for_shard(&mut self, shard: usize) -> i64 {
        let cluster = self.engine.backend().cluster().unwrap().clone();

        loop {
            let id: i64 = rng().random();
            let calc = ContextBuilder::new(cluster.sharded_tables().first().unwrap())
                .data(id)
                .shards(cluster.shards().len())
                .build()
                .unwrap()
                .apply()
                .unwrap();

            if calc == Shard::Direct(shard) {
                return id;
            }
        }
    }
}

impl Drop for TestClient {
    fn drop(&mut self) {
        shutdown();
    }
}

/// Test client that spawns the client into an async task,
/// running the full `spawn_internal` code path (including error handling).
/// Interaction happens purely over the wire.
pub struct SpawnedClient {
    pub conn: TcpStream,
    _handle: tokio::task::JoinHandle<()>,
}

impl SpawnedClient {
    async fn new(params: Parameters) -> Self {
        let (conn, client) = new_client_pair(params).await;

        let handle = tokio::spawn(async move {
            client.spawn_test().await;
        });

        Self {
            conn,
            _handle: handle,
        }
    }

    pub async fn new_sharded(params: Parameters) -> Self {
        load_test_sharded();
        Self::new(params).await
    }

    pub async fn send(&mut self, message: impl Protocol) {
        send_message(&mut self.conn, message).await;
    }

    pub async fn read(&mut self) -> Message {
        read_message(&mut self.conn).await
    }

    pub async fn read_until(&mut self, code: char) -> Vec<Message> {
        read_until(&mut self.conn, code).await.unwrap()
    }
}

impl Drop for SpawnedClient {
    fn drop(&mut self) {
        shutdown();
    }
}
