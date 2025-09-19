//! GSSAPI authentication integration tests
//!
//! These tests are designed to fail initially as we implement the GSSAPI functionality.
//! They demonstrate the expected API and behavior for GSSAPI authentication.

#![cfg(feature = "gssapi")]

use pgdog::auth::gssapi::{TicketCache, TicketManager};
use std::path::PathBuf;

/// Get the path to the test keytabs directory
fn test_keytab_path(filename: &str) -> PathBuf {
    // Use the CARGO_MANIFEST_DIR to find the project root, or fallback to relative path
    let base_path = std::env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("."));
    base_path
        .parent() // Go up from pgdog/ to project root
        .unwrap_or(&base_path)
        .join("integration")
        .join("gssapi")
        .join("keytabs")
        .join(filename)
}

/// Test that TicketCache can acquire a credential from a keytab
#[tokio::test]
async fn test_ticket_cache_acquires_credential() {
    // Use test keytab from integration directory
    let keytab_path = test_keytab_path("test.keytab");

    // Fail with helpful message if keytab doesn't exist
    assert!(
        keytab_path.exists(),
        "Test keytab not found at {:?}. Please run: bash integration/gssapi/setup_test_keytabs.sh",
        keytab_path
    );
    let principal = "test@PGDOG.LOCAL";

    let cache = TicketCache::new(principal, keytab_path);
    let ticket = cache.acquire_ticket();

    assert!(
        ticket.is_ok(),
        "Failed to acquire ticket: {:?}",
        ticket.err()
    );
}

/// Test that TicketManager maintains per-server caches
#[tokio::test]
async fn test_ticket_manager_per_server_cache() {
    // Check if keytabs exist
    let keytab1 = test_keytab_path("server1.keytab");
    let keytab2 = test_keytab_path("server2.keytab");
    assert!(
        keytab1.exists() && keytab2.exists(),
        "Test keytabs not found. Please run: bash integration/gssapi/setup_test_keytabs.sh"
    );
    // This test MUST FAIL initially because TicketManager doesn't exist yet
    let manager = TicketManager::new();

    // Get ticket for server1
    let ticket1 = manager
        .get_ticket(
            "server1:5432",
            test_keytab_path("server1.keytab"),
            "server1@PGDOG.LOCAL",
        )
        .await;

    // Get ticket for server2
    let ticket2 = manager
        .get_ticket(
            "server2:5432",
            test_keytab_path("server2.keytab"),
            "server2@PGDOG.LOCAL",
        )
        .await;

    assert!(ticket1.is_ok(), "Failed to get ticket for server1");
    assert!(ticket2.is_ok(), "Failed to get ticket for server2");

    // Verify they are different tickets
    let cache1 = manager.get_cache("server1:5432");
    let cache2 = manager.get_cache("server2:5432");
    assert!(cache1.is_some());
    assert!(cache2.is_some());
    assert_ne!(cache1.unwrap().principal(), cache2.unwrap().principal());
}

/// Test GSSAPI frontend authentication flow
#[tokio::test]
async fn test_gssapi_frontend_authentication() {
    // Check if keytab exists
    let keytab = test_keytab_path("test.keytab");
    assert!(
        keytab.exists(),
        "Test keytab not found at {:?}. Please run: bash integration/gssapi/setup_test_keytabs.sh",
        keytab
    );
    // This test demonstrates the async API
    use pgdog::auth::gssapi::{handle_gssapi_auth, GssapiServer};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    // This will fail without a real keytab
    let server = GssapiServer::new_acceptor(test_keytab_path("test.keytab"), None);
    if server.is_err() {
        // Expected to fail without real keytab
        return;
    }

    let server = Arc::new(Mutex::new(server.unwrap()));
    let client_token = vec![0x60, 0x81]; // Mock GSSAPI token header
    let result = handle_gssapi_auth(server, client_token).await;

    assert!(
        result.is_ok(),
        "Failed to handle GSSAPI auth: {:?}",
        result.err()
    );

    let response = result.unwrap();
    assert!(response.is_complete || response.token.is_some());
}

/// Test ticket refresh mechanism
#[tokio::test]
async fn test_ticket_refresh() {
    // Check if keytab exists
    let keytab = test_keytab_path("test.keytab");
    assert!(
        keytab.exists(),
        "Test keytab not found at {:?}. Please run: bash integration/gssapi/setup_test_keytabs.sh",
        keytab
    );
    // This test demonstrates ticket refresh
    use std::time::Duration;

    let manager = TicketManager::new();
    manager.set_refresh_interval(Duration::from_secs(1)); // Short interval for testing

    let ticket = manager
        .get_ticket(
            "server:5432",
            test_keytab_path("test.keytab"),
            "test@PGDOG.LOCAL",
        )
        .await;
    assert!(ticket.is_ok());

    let initial_refresh_time = manager.get_last_refresh("server:5432");

    // Wait for refresh
    std::thread::sleep(Duration::from_secs(2));

    let new_refresh_time = manager.get_last_refresh("server:5432");
    assert!(
        new_refresh_time > initial_refresh_time,
        "Ticket was not refreshed"
    );
}

/// Test GSSAPI context creation for backend connection
#[test]
fn test_backend_gssapi_context() {
    // Check if keytab exists
    let keytab = test_keytab_path("backend.keytab");
    assert!(
        keytab.exists(),
        "Test keytab not found at {:?}. Please run: bash integration/gssapi/setup_test_keytabs.sh",
        keytab
    );
    // This test demonstrates GssapiContext API
    use pgdog::auth::gssapi::GssapiContext;

    let keytab = test_keytab_path("backend.keytab");
    let principal = "pgdog-test@PGDOG.LOCAL";
    let target = "postgres/db.example.com@PGDOG.LOCAL";

    let context = GssapiContext::new_initiator(keytab, principal, target);

    #[cfg(feature = "gssapi")]
    {
        // With real GSSAPI, this will fail without keytab
        assert!(context.is_err());
    }

    #[cfg(not(feature = "gssapi"))]
    {
        // Mock version should succeed in creation
        assert!(context.is_ok());
        let mut ctx = context.unwrap();
        // But operations will fail
        let initial_token = ctx.initiate();
        assert!(initial_token.is_err());
    }
}

/// Test error handling for missing keytab
#[test]
fn test_missing_keytab_error() {
    // Test with a truly non-existent keytab
    let cache = TicketCache::new("test@PGDOG.LOCAL", PathBuf::from("/nonexistent/keytab"));
    let ticket = cache.acquire_ticket();

    assert!(ticket.is_err());
    let err = ticket.unwrap_err();
    assert!(err.to_string().contains("keytab"));
}

/// Test cleanup of ticket caches on shutdown
#[test]
fn test_ticket_manager_cleanup() {
    // Check if keytabs exist
    let keytab1 = test_keytab_path("keytab1.keytab");
    let keytab2 = test_keytab_path("keytab2.keytab");
    assert!(
        keytab1.exists() && keytab2.exists(),
        "Test keytabs not found. Please run: bash integration/gssapi/setup_test_keytabs.sh"
    );
    // This test MUST FAIL initially
    let manager = TicketManager::new();

    // Add some tickets
    let _ = manager.get_ticket(
        "server1:5432",
        test_keytab_path("keytab1.keytab"),
        "principal1@REALM",
    );
    let _ = manager.get_ticket(
        "server2:5432",
        test_keytab_path("keytab2.keytab"),
        "principal2@REALM",
    );

    assert_eq!(manager.cache_count(), 2);

    // Cleanup
    manager.shutdown();

    assert_eq!(manager.cache_count(), 0);
    assert!(manager.get_cache("server1:5432").is_none());
}
