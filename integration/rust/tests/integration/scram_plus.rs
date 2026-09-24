use crate::setup::admin_sqlx;
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use serial_test::serial;
use sqlx::Executor;
use tokio_postgres::{
    Client, SimpleQueryMessage,
    config::{ChannelBinding, SslMode},
};

fn make_tls() -> MakeTlsConnector {
    let connector = TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .danger_accept_invalid_hostnames(true)
        .build()
        .expect("native-tls connector");
    MakeTlsConnector::new(connector)
}

async fn connect(
    user: &str,
    password: &str,
    channel_binding: ChannelBinding,
) -> Result<Client, tokio_postgres::Error> {
    let mut config = tokio_postgres::Config::new();
    config
        .host("127.0.0.1")
        .port(6432)
        .user(user)
        .password(password)
        .dbname("pgdog")
        .ssl_mode(SslMode::Require)
        .channel_binding(channel_binding);

    let (client, connection) = config.connect(make_tls()).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

async fn select_one(client: &Client) {
    let messages = client.simple_query("SELECT 1").await.unwrap();
    let row = messages.iter().find_map(|message| match message {
        SimpleQueryMessage::Row(row) => Some(row),
        _ => None,
    });
    let row = row.expect("SELECT 1 returned a row");
    assert_eq!(row.get(0), Some("1"));
}

#[tokio::test]
#[serial]
async fn rust_client_requires_plus_on_tls() {
    admin_sqlx().await.execute("RELOAD").await.unwrap();

    let client = connect("pgdog", "pgdog", ChannelBinding::Require)
        .await
        .expect("tokio-postgres channel_binding=require must complete PLUS");
    select_one(&client).await;
}

#[tokio::test]
#[serial]
async fn rust_client_requires_plus_with_hashed_password() {
    admin_sqlx().await.execute("RELOAD").await.unwrap();

    let client = connect("pgdog_hashed", "pgdog", ChannelBinding::Require)
        .await
        .expect("hashed SCRAM user must complete PLUS");
    select_one(&client).await;
}

#[tokio::test]
#[serial]
async fn rust_client_require_plus_rejects_wrong_password() {
    admin_sqlx().await.execute("RELOAD").await.unwrap();

    connect("pgdog", "wrong", ChannelBinding::Require)
        .await
        .expect_err("wrong password must fail PLUS");
}

#[tokio::test]
#[serial]
async fn rust_client_can_disable_channel_binding_over_tls() {
    admin_sqlx().await.execute("RELOAD").await.unwrap();

    let client = connect("pgdog", "pgdog", ChannelBinding::Disable)
        .await
        .expect("channel_binding=disable over TLS must still use SCRAM-SHA-256");
    select_one(&client).await;
}
