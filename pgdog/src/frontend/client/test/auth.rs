//! Client authentication tests.

use pgdog_config::{AuthType, PassthroughAuth};

use crate::{
    config::{config, set},
    expect_message,
    net::{Authentication, ErrorResponse, Parameters, Password},
};

use super::SpawnedClient;

/// Connect to the admin database and answer the plaintext password
/// request with the given password.
async fn login_admin(password: &str) -> SpawnedClient {
    let cfg = config();
    let mut params = Parameters::default();
    params.insert("user", cfg.config.admin.user.as_str());
    params.insert("database", cfg.config.admin.name.as_str());

    let mut client = SpawnedClient::new_with_login(params).await;

    // Both the admin and the passthrough branches request the password
    // in plaintext; what matters is what happens with the answer.
    let request = expect_message!(client.read().await, Authentication);
    assert!(matches!(request, Authentication::ClearTextPassword));

    client.send(Password::new_password(password)).await;
    client
}

/// Admin connections must be authenticated against the admin password even
/// when passthrough auth is enabled. Regression test for the passthrough
/// branch running first and accepting any password for the admin database.
#[tokio::test]
async fn test_admin_password_checked_with_passthrough_auth() {
    crate::logger();
    crate::config::load_test();

    let mut cfg = (*config()).clone();
    cfg.config.general.auth_type = AuthType::Plain;
    cfg.config.general.passthrough_auth = PassthroughAuth::EnabledPlain;
    cfg.config.admin.password = "admin-password".into();
    set(cfg).unwrap();

    // The wrong password is rejected instead of being passed through.
    let mut client = login_admin("not-the-admin-password").await;
    let error = ErrorResponse::try_from(client.read().await).unwrap();
    assert_eq!(error.code, "28000");
    client.join().await;

    // The correct password is accepted.
    let mut client = login_admin("admin-password").await;
    let response = expect_message!(client.read().await, Authentication);
    assert!(matches!(response, Authentication::Ok));
    client.read_until('Z').await;
    client.join().await;
}
