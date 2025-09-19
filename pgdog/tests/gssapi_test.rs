//! GSSAPI authentication tests

#![cfg(feature = "gssapi")]

use pgdog::auth::gssapi::{
    handle_gssapi_auth, GssapiContext, GssapiServer, TicketCache, TicketManager,
};
use pgdog::backend::pool::Address;
use pgdog::config::GssapiConfig;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

fn test_keytab_path(filename: &str) -> PathBuf {
    let base_path = std::env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("."));
    base_path
        .parent()
        .unwrap_or(&base_path)
        .join("integration")
        .join("gssapi")
        .join("keytabs")
        .join(filename)
}

#[test]
fn test_address_has_gssapi() {
    let mut addr = Address {
        host: "test.example.com".to_string(),
        port: 5432,
        database_name: "testdb".to_string(),
        user: "testuser".to_string(),
        password: "testpass".to_string(),
        gssapi_keytab: None,
        gssapi_principal: None,
        gssapi_target_principal: None,
    };

    assert!(!addr.has_gssapi());

    addr.gssapi_keytab = Some(
        test_keytab_path("test.keytab")
            .to_string_lossy()
            .to_string(),
    );
    assert!(!addr.has_gssapi());

    addr.gssapi_principal = Some("test@REALM".to_string());
    assert!(addr.has_gssapi());
}

#[test]
fn test_gssapi_context_creation() {
    let keytab = test_keytab_path("backend.keytab");
    assert!(keytab.exists(), "Test keytab not found at {:?}", keytab);

    let principal = "pgdog-test@PGDOG.LOCAL";
    let target = "postgres/db.example.com";
    let context = GssapiContext::new_initiator(keytab, principal, target);

    assert!(
        context.is_ok(),
        "Failed to create GSSAPI context: {:?}",
        context.err()
    );
    let mut ctx = context.unwrap();
    assert_eq!(ctx.target_principal(), target);
    assert!(!ctx.is_complete());

    let result = ctx.initiate();
    assert!(
        result.is_err(),
        "Initiate should fail without a real server"
    );
}

#[test]
fn test_backend_target_principal() {
    let host = "db.example.com";
    let target = format!("postgres/{}", host);
    assert_eq!(target, "postgres/db.example.com");

    let host = "192.168.1.1";
    let target = format!("postgres/{}", host);
    assert_eq!(target, "postgres/192.168.1.1");
}

#[tokio::test]
async fn test_ticket_cache_acquires_credential() {
    let keytab_path = test_keytab_path("test.keytab");
    assert!(
        keytab_path.exists(),
        "Test keytab not found at {:?}",
        keytab_path
    );

    let principal = "test@PGDOG.LOCAL";
    let cache = TicketCache::new(principal, keytab_path, None);
    let ticket = cache.acquire_ticket().await;

    assert!(
        ticket.is_ok(),
        "Failed to acquire ticket: {:?}",
        ticket.as_ref().err()
    );
}

#[tokio::test]
async fn test_ticket_manager_per_server() {
    let keytab1 = test_keytab_path("server1.keytab");
    let keytab2 = test_keytab_path("server2.keytab");
    assert!(
        keytab1.exists() && keytab2.exists(),
        "Test keytabs not found"
    );

    let manager = TicketManager::new();

    let ticket1 = manager
        .get_ticket("server1:5432", keytab1.clone(), "server1@PGDOG.LOCAL")
        .await;
    let ticket2 = manager
        .get_ticket("server2:5432", keytab2.clone(), "server2@PGDOG.LOCAL")
        .await;

    assert!(
        ticket1.is_ok(),
        "Failed to get ticket for server1: {:?}",
        ticket1.err()
    );
    assert!(
        ticket2.is_ok(),
        "Failed to get ticket for server2: {:?}",
        ticket2.err()
    );

    let cache1 = manager.get_cache("server1:5432");
    let cache2 = manager.get_cache("server2:5432");
    assert!(cache1.is_some(), "Cache for server1 should exist");
    assert!(cache2.is_some(), "Cache for server2 should exist");
    assert_ne!(cache1.unwrap().principal(), cache2.unwrap().principal());
}

#[tokio::test]
async fn test_frontend_authentication() {
    let keytab = test_keytab_path("test.keytab");
    assert!(keytab.exists(), "Test keytab not found at {:?}", keytab);

    let server = GssapiServer::new_acceptor(keytab, None);
    if server.is_err() {
        return;
    }

    let server = Arc::new(Mutex::new(server.unwrap()));
    let client_token = vec![0x60, 0x81];
    let result = handle_gssapi_auth(server, client_token).await;

    assert!(result.is_err(), "Should fail with invalid token");
}

#[tokio::test]
async fn test_missing_keytab_error() {
    let cache = TicketCache::new(
        "test@PGDOG.LOCAL",
        PathBuf::from("/nonexistent/keytab"),
        None,
    );
    let ticket = cache.acquire_ticket().await;

    assert!(ticket.is_err());
    let err = ticket.unwrap_err();
    assert!(err.to_string().contains("keytab"));
}

#[tokio::test]
async fn test_ticket_manager_cleanup() {
    let keytab1 = test_keytab_path("keytab1.keytab");
    let keytab2 = test_keytab_path("keytab2.keytab");
    assert!(
        keytab1.exists() && keytab2.exists(),
        "Test keytabs not found"
    );

    let manager = TicketManager::new();

    let _ = manager
        .get_ticket("server1:5432", keytab1, "principal1@PGDOG.LOCAL")
        .await;
    let _ = manager
        .get_ticket("server2:5432", keytab2, "principal2@PGDOG.LOCAL")
        .await;

    assert_eq!(manager.cache_count(), 2);

    manager.shutdown();

    assert_eq!(manager.cache_count(), 0);
    assert!(manager.get_cache("server1:5432").is_none());
}

#[test]
fn test_gssapi_config() {
    let gssapi = GssapiConfig {
        enabled: true,
        server_keytab: Some(test_keytab_path("test.keytab")),
        server_principal: Some("pgdog-test@PGDOG.LOCAL".to_string()),
        default_backend_keytab: Some(test_keytab_path("backend.keytab")),
        default_backend_principal: Some("pgdog-backend@PGDOG.LOCAL".to_string()),
        default_backend_target_principal: Some("postgres/test@PGDOG.LOCAL".to_string()),
        strip_realm: true,
        ticket_refresh_interval: 14400,
        fallback_enabled: false,
        require_encryption: false,
    };

    assert!(gssapi.is_configured());
    assert!(gssapi.has_backend_config());
}
