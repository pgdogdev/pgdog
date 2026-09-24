//! Tests for the two_pc function in binding.rs

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use crate::{
        backend::{
            pool::{Pool, PoolConfig, connection::binding::Binding},
            server::test::test_server,
        },
        frontend::{
            ClientRequest,
            client::query_engine::{TwoPcPhase, two_pc::TwoPcTransaction},
            router::{
                Route,
                parser::{Shard, ShardWithPriority},
            },
        },
        net::{Protocol, Query},
    };

    use super::super::binding::{AdminBinding, BindingDirect, BindingMulti};
    use tokio::time::Instant;

    #[tokio::test]
    async fn admin_disconnect_preserves_pending_messages() {
        let mut binding = Binding::Admin(AdminBinding::new());
        let request = ClientRequest::from(vec![Query::new("NOT AN ADMIN COMMAND").into()]);
        binding.send(&request).await.expect("admin query is queued");

        binding.disconnect();
        binding.force_close();

        assert!(matches!(binding, Binding::Admin(_)));
        assert!(binding.connected());
        assert!(binding.has_more_messages());
        assert_eq!(binding.read().await.expect("error response").code(), 'E');
        assert_eq!(binding.read().await.expect("ready response").code(), 'Z');
        assert!(binding.done());
    }

    #[tokio::test]
    async fn disconnected_binding_delegates_without_a_server() {
        let route = Route::write(ShardWithPriority::new_default_unset(Shard::All));
        let mut binding = Binding::MultiShard(Box::new(BindingMulti::new(Vec::new(), &route)));
        binding.disconnect();

        assert!(matches!(binding, Binding::NotConnected(_)));
        assert!(!binding.connected());
        assert_eq!(binding.connected_servers(), 0);
        assert!(matches!(
            binding.shards(),
            Err(crate::backend::Error::NotConnected)
        ));
        assert!(matches!(
            binding.send(&ClientRequest::from(Vec::new())).await,
            Err(crate::backend::Error::NotConnected)
        ));
        assert!(matches!(
            binding.send_copy(Vec::new()).await,
            Err(crate::backend::Error::CopyNotConnected)
        ));
        assert!(futures::poll!(pin!(binding.read())).is_pending());
        binding.force_close();
        assert!(matches!(binding, Binding::NotConnected(_)));
    }

    async fn create_multishard_binding() -> Binding {
        // Create multiple test servers and pools to simulate shards
        let server1 = Box::new(test_server().await);
        let server2 = Box::new(test_server().await);
        let server3 = Box::new(test_server().await);

        // Create pools for each server using their addresses
        let pool1 = Pool::new(&PoolConfig {
            address: server1.addr().clone(),
            config: crate::backend::pool::Config::default(),
        });

        let pool2 = Pool::new(&PoolConfig {
            address: server2.addr().clone(),
            config: crate::backend::pool::Config::default(),
        });

        let pool3 = Pool::new(&PoolConfig {
            address: server3.addr().clone(),
            config: crate::backend::pool::Config::default(),
        });

        let now = Instant::now();
        let guards = vec![
            crate::backend::pool::Guard::new(pool1, server1, now),
            crate::backend::pool::Guard::new(pool2, server2, now),
            crate::backend::pool::Guard::new(pool3, server3, now),
        ];

        let route = Route::write(ShardWithPriority::new_default_unset(Shard::All));
        let mut binding = Binding::MultiShard(Box::new(BindingMulti::new(guards, &route)));

        // Start transaction on all shards for two-phase commit tests
        let _result = binding
            .execute("BEGIN")
            .await
            .expect("BEGIN should succeed");

        binding
    }

    #[tokio::test]
    async fn test_two_pc_with_direct_binding_fails() {
        use crate::backend::server::test::test_server;

        // Create a Direct binding instead of MultiShard
        let server = Box::new(test_server().await);
        let pool = Pool::new(&PoolConfig {
            address: server.addr().clone(),
            config: crate::backend::pool::Config::default(),
        });

        let guard = crate::backend::pool::Guard::new(pool, server, Instant::now());
        let mut binding = Binding::Direct(BindingDirect::new(guard));

        let result = binding
            .two_pc(TwoPcTransaction::new(), TwoPcPhase::Phase1, false)
            .await;

        // Should fail with TwoPcMultiShardOnly error
        assert!(result.is_err());
        if let Err(error) = result {
            // Check that it's the expected error type
            assert!(matches!(error, crate::backend::Error::TwoPcMultiShardOnly));
        }
    }

    #[tokio::test]
    async fn test_two_pc_with_admin_binding_fails() {
        // Create an Admin binding
        let mut binding = Binding::Admin(AdminBinding::new());

        let result = binding
            .two_pc(TwoPcTransaction::new(), TwoPcPhase::Phase1, false)
            .await;

        // Should fail with TwoPcMultiShardOnly error
        assert!(result.is_err());
        if let Err(error) = result {
            assert!(matches!(error, crate::backend::Error::TwoPcMultiShardOnly));
        }
    }

    #[tokio::test]
    async fn test_two_pc_phase1_prepare() {
        let mut binding = create_multishard_binding().await;
        let transaction = TwoPcTransaction::new();

        // Test Phase1 - PREPARE TRANSACTION
        let result = binding.two_pc(transaction, TwoPcPhase::Phase1, false).await;

        // Should succeed
        if let Err(ref error) = result {
            eprintln!("Error in test_two_pc_phase1_prepare: {:?}", error);
        }
        assert!(result.is_ok());

        // Cleanup: Rollback the prepared transaction to avoid leaving dangling transactions
        let _cleanup = binding
            .two_pc(transaction, TwoPcPhase::Rollback, false)
            .await;
    }

    #[tokio::test]
    async fn test_two_pc_phase2_commit() {
        let mut binding = create_multishard_binding().await;
        let transaction = TwoPcTransaction::new();

        // First prepare the transaction
        binding
            .two_pc(transaction, TwoPcPhase::Phase1, false)
            .await
            .expect("Phase1 should succeed");

        // Then commit it
        let result = binding.two_pc(transaction, TwoPcPhase::Phase2, false).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_two_pc_rollback() {
        let mut binding = create_multishard_binding().await;
        let transaction = TwoPcTransaction::new();

        // First prepare the transaction
        binding
            .two_pc(transaction, TwoPcPhase::Phase1, false)
            .await
            .expect("Phase1 should succeed");

        // Then rollback
        let result = binding
            .two_pc(transaction, TwoPcPhase::Rollback, false)
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_two_pc_commit_after_prepare_and_commit() {
        let mut binding = create_multishard_binding().await;
        let transaction = TwoPcTransaction::new();

        // First prepare the transaction
        binding
            .two_pc(transaction, TwoPcPhase::Phase1, false)
            .await
            .expect("Phase1 should succeed");

        // Then commit it
        binding
            .two_pc(transaction, TwoPcPhase::Phase2, true)
            .await
            .expect("Phase2 should succeed");

        let result = binding.two_pc(transaction, TwoPcPhase::Phase2, true).await;
        assert!(
            result.is_ok(),
            "Committing non-existent prepared transaction should be skipped"
        );
    }

    #[tokio::test]
    async fn test_two_pc_rollback_after_prepare_and_rollback() {
        let mut binding = create_multishard_binding().await;
        let transaction = TwoPcTransaction::new();

        // First prepare the transaction
        binding
            .two_pc(transaction, TwoPcPhase::Phase1, true)
            .await
            .expect("Phase1 should succeed");

        // Then rollback it
        binding
            .two_pc(transaction, TwoPcPhase::Rollback, true)
            .await
            .expect("Rollback should succeed");

        // Try to rollback again - should succeed because skip_missing is true for Rollback
        let result = binding
            .two_pc(transaction, TwoPcPhase::Rollback, true)
            .await;
        assert!(
            result.is_ok(),
            "Rolling back non-existent prepared transaction should be skipped"
        );

        // Cleanup: End the transaction
        let _cleanup = binding.execute("ROLLBACK").await;
    }

    #[tokio::test]
    async fn test_two_pc_transaction_lifecycle() {
        let mut binding = create_multishard_binding().await;
        let transaction = TwoPcTransaction::new();

        // 1. Prepare transaction
        let result = binding.two_pc(transaction, TwoPcPhase::Phase1, false).await;
        assert!(result.is_ok(), "Phase1 preparation should succeed");

        // 2. Try to prepare the same transaction again - PostgreSQL behavior may vary
        let _result = binding.two_pc(transaction, TwoPcPhase::Phase1, true).await;

        // 3. Commit the prepared transaction
        let result = binding.two_pc(transaction, TwoPcPhase::Phase2, true).await;
        assert!(result.is_ok(), "Phase2 commit should succeed");

        let result = binding.two_pc(transaction, TwoPcPhase::Phase2, true).await;
        assert!(
            result.is_ok(),
            "Committing non-existent transaction should be skipped"
        );
    }

    #[tokio::test]
    async fn test_two_pc_prepare_then_rollback() {
        let mut binding = create_multishard_binding().await;
        let transaction = TwoPcTransaction::new();

        // 1. Prepare transaction
        let result = binding.two_pc(transaction, TwoPcPhase::Phase1, false).await;
        assert!(result.is_ok(), "Phase1 preparation should succeed");

        // 2. Rollback the prepared transaction
        let result = binding
            .two_pc(transaction, TwoPcPhase::Rollback, true)
            .await;
        assert!(result.is_ok(), "Rollback should succeed");

        // 3. Try to commit after rollback
        let result = binding.two_pc(transaction, TwoPcPhase::Phase2, true).await;
        assert!(
            result.is_ok(),
            "Committing rolled back transaction should be skipped"
        );
    }

    #[tokio::test]
    async fn test_prepared_error_not_found() {
        let mut server = test_server().await;
        let err = server
            .execute("ROLLBACK PREPARED 'test'")
            .await
            .err()
            .unwrap();
        match err {
            crate::backend::Error::ExecutionError(err) => {
                assert_eq!(err.code, "42704");
                assert_eq!(
                    err.message,
                    r#"prepared transaction with identifier "test" does not exist"#
                );
            }

            _ => panic!("not an error"),
        };
    }
}
