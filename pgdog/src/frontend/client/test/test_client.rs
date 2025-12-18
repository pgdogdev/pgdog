use std::fmt::Debug;

use bytes::{BufMut, Bytes, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use crate::{
    backend::databases::shutdown,
    config::{load_test_replicas, load_test_sharded},
    frontend::{client::query_engine::QueryEngine, Client},
    net::{Message, Parameters, Protocol, Stream},
};

/// Try to convert a Message to the specified type.
/// If conversion fails and the message is an ErrorResponse, panic with its contents.
#[cfg(test)]
#[macro_export]
macro_rules! expect_message {
    ($message:expr, $ty:ty) => {{
        let message: crate::net::Message = $message;
        match <$ty as TryFrom<crate::net::Message>>::try_from(message.clone()) {
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

/// Test client.
#[derive(Debug)]
pub struct TestClient {
    client: Client,
    engine: QueryEngine,
    conn: TcpStream,
}

impl TestClient {
    /// Create new test client after the login phase
    /// is complete.
    pub async fn new(params: Parameters) -> Self {
        let addr = "127.0.0.1:0".to_string();
        let conn_addr = addr.clone();
        let stream = TcpListener::bind(&conn_addr).await.unwrap();
        let port = stream.local_addr().unwrap().port();
        let connect_handle = tokio::spawn(async move {
            let (stream, _) = stream.accept().await.unwrap();
            let stream = Stream::plain(stream, 4096);

            Client::new_test(stream, params)
        });

        let conn = TcpStream::connect(&format!("127.0.0.1:{}", port))
            .await
            .unwrap();
        let client = connect_handle.await.unwrap();

        Self {
            conn,
            engine: QueryEngine::from_client(&client).expect("create query engine from client"),
            client,
        }
    }

    pub async fn new_sharded(params: Parameters) -> Self {
        load_test_sharded();
        Self::new(params).await
    }

    pub async fn new_replicas(params: Parameters) -> Self {
        load_test_replicas();
        Self::new(params).await
    }

    /// Send message to client.
    pub async fn send(&mut self, message: impl Protocol) {
        let message = message.to_bytes().expect("message to convert to bytes");
        self.conn.write_all(&message).await.expect("write_all");
        self.conn.flush().await.expect("flush");
    }

    pub async fn send_simple(&mut self, message: impl Protocol) {
        self.send(message).await;
        self.process().await;
    }

    /// Read a message received from the servers.
    pub async fn read(&mut self) -> Message {
        let code = self.conn.read_u8().await.expect("code");
        let len = self.conn.read_i32().await.expect("len");
        let mut rest = vec![0u8; len as usize - 4];
        self.conn.read_exact(&mut rest).await.expect("read_exact");

        let mut payload = BytesMut::new();
        payload.put_u8(code);
        payload.put_i32(len);
        payload.put(Bytes::from(rest));

        Message::new(payload.freeze()).backend()
    }

    /// Inspect engine state.
    pub fn engine(&mut self) -> &mut QueryEngine {
        &mut self.engine
    }

    /// Inspect client state.
    pub fn client(&mut self) -> &mut Client {
        &mut self.client
    }

    /// Process a request.
    pub async fn process(&mut self) {
        self.client
            .buffer(self.engine.stats().state)
            .await
            .expect("buffer");
        self.client
            .client_messages(&mut self.engine)
            .await
            .expect("engine");
    }
}

impl Drop for TestClient {
    fn drop(&mut self) {
        shutdown();
    }
}
