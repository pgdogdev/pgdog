use tokio::{io::AsyncWriteExt, net::TcpStream};

use crate::{
    frontend::client::test::read_messages,
    net::{Query, ToBytes},
};

pub mod prepared;
pub mod simple;

async fn truncate_table(table: &str, stream: &mut TcpStream) {
    let query = Query::new(format!("TRUNCATE {}", table))
        .to_bytes()
        .unwrap();
    stream.write_all(&query).await.unwrap();
    stream.flush().await.unwrap();

    read_messages(stream, &['C', 'Z']).await;
}
