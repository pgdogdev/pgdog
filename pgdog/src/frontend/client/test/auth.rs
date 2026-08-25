//! Client authentication tests.

use std::num::NonZeroU32;

use crate::{
    auth::scram,
    config::{config, set},
    expect_message,
    frontend::Client,
    net::{Authentication, ErrorResponse, Parameters, Password},
};
use pgdog_config::{AuthType, PassthroughAuth, users::PasswordKind};

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

/// Connect to a regular database user and answer the cleartext credential
/// request used by plugin authentication.
async fn login_user(user: &str, password: &str) -> SpawnedClient {
    let mut params = Parameters::default();
    params.insert("user", user);
    params.insert("database", "pgdog");

    let mut client = SpawnedClient::new_with_login(params).await;
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

#[test]
fn test_cleartext_password_supports_plain_and_scram_verifiers() {
    let verifier = scram::generate_hash(
        "hashed-password",
        NonZeroU32::new(4096).expect("iterations are non-zero"),
        b"pgdog_test_salt!",
    );
    let passwords = [
        PasswordKind::Plain("plain-password".into()),
        PasswordKind::Hashed(verifier),
    ];

    assert_eq!(
        Client::check_cleartext_password(&passwords, "plain-password"),
        crate::auth::AuthResult::Ok
    );
    assert_eq!(
        Client::check_cleartext_password(&passwords, "hashed-password"),
        crate::auth::AuthResult::Ok
    );
    assert_eq!(
        Client::check_cleartext_password(&passwords, "wrong-password"),
        crate::auth::AuthResult::NoPasswordMatch
    );
    assert_eq!(
        Client::check_cleartext_password(&[], "anything"),
        crate::auth::AuthResult::NoPasswordConfig
    );
}

#[tokio::test]
async fn test_plugin_skip_falls_back_to_configured_password() {
    crate::logger();
    crate::config::load_test();

    let mut cfg = (*config()).clone();
    cfg.config.general.auth_type = AuthType::Plugin;
    set(cfg).unwrap();

    let mut client = login_user("pgdog", "pgdog").await;
    let response = expect_message!(client.read().await, Authentication);
    assert!(matches!(response, Authentication::Ok));
    client.read_until('Z').await;
}

#[tokio::test]
async fn test_plugin_skip_rejects_wrong_configured_password() {
    crate::logger();
    crate::config::load_test();

    let mut cfg = (*config()).clone();
    cfg.config.general.auth_type = AuthType::Plugin;
    set(cfg).unwrap();

    let mut client = login_user("pgdog", "wrong-password").await;
    let error = ErrorResponse::try_from(client.read().await).unwrap();
    assert_eq!(error.code, "28000");
    client.join().await;
}

#[tokio::test]
async fn test_plugin_skip_falls_back_to_passthrough() {
    crate::logger();
    crate::config::load_test();

    let mut cfg = (*config()).clone();
    cfg.config.general.auth_type = AuthType::Plugin;
    cfg.config.general.passthrough_auth = PassthroughAuth::EnabledPlain;
    set(cfg).unwrap();

    let mut client = login_user("pgdog", "pgdog").await;
    let response = expect_message!(client.read().await, Authentication);
    assert!(matches!(response, Authentication::Ok));
    client.read_until('Z').await;
}

#[tokio::test]
async fn test_plugin_skip_does_not_bootstrap_password_for_plugin_only_user() {
    crate::logger();
    crate::config::load_test();

    let mut cfg = (*config()).clone();
    cfg.config.general.auth_type = AuthType::Plugin;
    cfg.config.general.passthrough_auth = PassthroughAuth::EnabledPlain;
    cfg.users.users[0].password = None;
    cfg.users.users[0].server_password = Some("pgdog".into());
    set(cfg).unwrap();
    crate::backend::databases::reload_from_existing().unwrap();

    let mut client = login_user("pgdog", "arbitrary-password").await;
    let error = ErrorResponse::try_from(client.read().await).unwrap();
    assert_eq!(error.code, "28000");
    client.join().await;
}
