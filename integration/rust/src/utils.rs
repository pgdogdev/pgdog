use super::setup::admin_sqlx;
use bytes::{BufMut, Bytes, BytesMut};
use sqlx::{Executor, Row};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
};

pub async fn assert_setting_str(name: &str, expected: &str) {
    let admin = admin_sqlx().await;
    let rows = admin.fetch_all("SHOW CONFIG").await.unwrap();
    let mut found = false;
    for row in rows {
        let db_name: String = row.get(0);
        let value: String = row.get(1);

        if name == db_name {
            found = true;
            assert_eq!(value, expected);
        }
    }

    assert!(found);
}

/// Standard Postgres protocol message.
#[derive(Debug, Clone)]
pub struct Message {
    pub code: char,
    pub payload: Bytes,
}

impl Message {
    pub async fn read(stream: &mut (impl AsyncRead + Unpin)) -> Result<Self, std::io::Error> {
        let code = stream.read_u8().await? as char;
        let len = stream.read_i32().await?;
        let mut payload = vec![0u8; len as usize - 4];
        stream.read_exact(&mut payload).await?;

        Ok(Self {
            code,
            payload: Bytes::from(payload),
        })
    }

    pub async fn send(&self, stream: &mut (impl AsyncWrite + Unpin)) -> Result<(), std::io::Error> {
        let mut payload = BytesMut::new();
        payload.put_u8(self.code as u8);
        payload.put_i32(self.payload.len() as i32 + 4);
        payload.put(self.payload.clone());

        stream.write_all(&payload).await?;
        stream.flush().await?;

        Ok(())
    }

    pub fn new_parse(name: &str, sql: &str) -> Self {
        let mut payload = BytesMut::new();
        payload.put(name.as_bytes());
        payload.put_u8(0);
        payload.put(sql.as_bytes());
        payload.put_u8(0);
        payload.put_i16(0);

        Self {
            payload: payload.freeze(),
            code: 'P',
        }
    }

    pub fn new_flush() -> Self {
        Self {
            code: 'H',
            payload: Bytes::new(),
        }
    }
}

/// Create a startup message.
pub fn startup(user: &str, database: &str) -> Bytes {
    let mut payload = BytesMut::new();
    payload.put_i32(196608);
    payload.put_slice("user\0".as_bytes());
    payload.put_slice(user.as_bytes());
    payload.put_u8(0);
    payload.put_slice("database\0".as_bytes());
    payload.put_slice(database.as_bytes());
    payload.put_u8(0);
    payload.put_u8(0);

    let mut bytes = BytesMut::new();
    bytes.put_i32(payload.len() as i32);
    bytes.put(payload);

    bytes.freeze()
}

pub async fn connect() -> TcpStream {
    let mut stream = TcpStream::connect("127.0.0.1:6432").await.unwrap();
    stream.write_all(&startup("pgdog", "pgdog")).await.unwrap();

    loop {
        let message = Message::read(&mut stream).await.unwrap();

        if message.code == 'Z' {
            break;
        }
    }

    stream
}
