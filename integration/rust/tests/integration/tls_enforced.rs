use rust::setup::admin_sqlx;
use rust::utils::assert_setting_str;
use sqlx::{ConnectOptions, Connection, Executor, postgres::PgSslMode};

#[tokio::test]
async fn test_tls_enforced() {
    let admin = admin_sqlx().await;
    let conn_base: sqlx::postgres::PgConnectOptions =
        "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog?application_name=sqlx"
            .parse()
            .unwrap();

    let opts_notls = conn_base.clone().ssl_mode(PgSslMode::Disable);
    let opts_tls = conn_base.clone().ssl_mode(PgSslMode::Require);

    {
        let tls = opts_tls.connect().await.unwrap();
        let notls = opts_notls.connect().await.unwrap();
        tls.close().await.unwrap();
        notls.close().await.unwrap();
    }

    admin
        .execute("SET tls_client_required TO 'true'")
        .await
        .unwrap();
    assert_setting_str("tls_client_required", "true").await;

    opts_tls.connect().await.unwrap();
    assert!(opts_notls.connect().await.is_err());
}
