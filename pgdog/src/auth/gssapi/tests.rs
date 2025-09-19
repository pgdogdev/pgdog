//! Unit tests for GSSAPI authentication module

#[cfg(test)]
mod tests {
    use super::super::*;
    use std::path::PathBuf;

    #[test]
    fn test_gssapi_response_creation() {
        let response = GssapiResponse {
            is_complete: false,
            token: Some(vec![1, 2, 3, 4]),
            principal: None,
        };
        assert!(!response.is_complete);
        assert!(response.token.is_some());
        assert!(response.principal.is_none());
    }

    #[test]
    fn test_gssapi_response_complete() {
        let response = GssapiResponse {
            is_complete: true,
            token: None,
            principal: Some("user@REALM".to_string()),
        };
        assert!(response.is_complete);
        assert!(response.token.is_none());
        assert_eq!(response.principal, Some("user@REALM".to_string()));
    }

    #[test]
    fn test_principal_realm_stripping() {
        let principal = "user@EXAMPLE.COM";
        let stripped = principal.split('@').next().unwrap_or(principal);
        assert_eq!(stripped, "user");

        let principal_no_realm = "user";
        let stripped = principal_no_realm
            .split('@')
            .next()
            .unwrap_or(principal_no_realm);
        assert_eq!(stripped, "user");
    }

    #[test]
    fn test_gssapi_error_types() {
        let err = GssapiError::KeytabNotFound(PathBuf::from("/etc/test.keytab"));
        assert!(matches!(err, GssapiError::KeytabNotFound(_)));

        let err = GssapiError::InvalidPrincipal("bad@principal".to_string());
        assert!(matches!(err, GssapiError::InvalidPrincipal(_)));

        let err = GssapiError::CredentialAcquisitionFailed("test failure".to_string());
        assert!(matches!(err, GssapiError::CredentialAcquisitionFailed(_)));

        let err = GssapiError::ContextError("context failed".to_string());
        assert!(matches!(err, GssapiError::ContextError(_)));

        let err = GssapiError::LibGssapi("lib error".to_string());
        assert!(matches!(err, GssapiError::LibGssapi(_)));
    }

    #[cfg(not(feature = "gssapi"))]
    #[test]
    fn test_mock_gssapi_server() {
        // When GSSAPI feature is disabled, ensure we get appropriate errors
        let result = GssapiServer::new_acceptor("/etc/test.keytab", Some("test@REALM"));
        assert!(result.is_err());
        if let Err(err) = result {
            match err {
                GssapiError::LibGssapi(msg) => {
                    assert!(msg.contains("not compiled"));
                }
                _ => panic!("Expected LibGssapi error"),
            }
        }
    }

    #[cfg(not(feature = "gssapi"))]
    #[test]
    fn test_mock_gssapi_context() {
        let result =
            GssapiContext::new_initiator("/etc/test.keytab", "client@REALM", "service@REALM");
        // Mock version should succeed in creation
        assert!(result.is_ok());

        let mut ctx = result.unwrap();
        assert_eq!(ctx.target_principal(), "service@REALM");
        assert!(!ctx.is_complete());

        // But operations should fail
        let result = ctx.initiate();
        assert!(result.is_err());
    }

    #[cfg(not(feature = "gssapi"))]
    #[tokio::test]
    async fn test_mock_ticket_cache() {
        let cache = TicketCache::new("test@REALM", PathBuf::from("/etc/test.keytab"), None);
        assert_eq!(cache.principal(), "test@REALM");
        assert_eq!(cache.keytab_path(), &PathBuf::from("/etc/test.keytab"));
        assert!(!cache.needs_refresh());

        let result = cache.acquire_ticket().await;
        assert!(result.is_err());
    }

    #[test]
    fn test_ticket_manager_singleton() {
        let manager1 = TicketManager::global();
        let manager2 = TicketManager::global();

        // Should be the same instance
        assert!(std::sync::Arc::ptr_eq(&manager1, &manager2));
    }

    #[cfg(feature = "gssapi")]
    #[test]
    fn test_gssapi_server_with_feature() {
        // With GSSAPI feature enabled, test should fail due to missing keytab
        let result = GssapiServer::new_acceptor("/nonexistent/test.keytab", Some("test@REALM"));
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_handle_gssapi_auth_mock() {
        // This test works regardless of feature flag
        #[cfg(not(feature = "gssapi"))]
        {
            let server = GssapiServer::new_acceptor("/test.keytab", None);
            assert!(server.is_err());
        }

        #[cfg(feature = "gssapi")]
        {
            // With real GSSAPI, this will fail without a keytab
            let server = GssapiServer::new_acceptor("/test.keytab", None);
            assert!(server.is_err());
        }
    }
}
