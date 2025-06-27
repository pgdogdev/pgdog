//! Test replication lag detection functionality.

use std::time::Duration;

use crate::backend::pool::{Config, Error, Pool, PoolConfig, Address, Replicas, ReplicationLagChecker};
use crate::config::LoadBalancingStrategy;

#[test]
fn test_replication_lag_config() {
    // Test that replication lag configuration is properly set
    let config = Config::default();
    
    assert_eq!(config.replication_lag_check_interval(), Duration::from_millis(10_000));
    assert_eq!(config.max_replication_lag_bytes(), 1024 * 1024);
}

#[test]
fn test_replication_lag_error() {
    // Test that ReplicationLag error is properly defined
    let error = Error::ReplicationLag;
    assert_eq!(format!("{}", error), "replication lag");
}

#[test]
fn test_lag_checker_creation() {
    // Test that ReplicationLagChecker can be created
    let primary_config = PoolConfig {
        address: Address {
            host: "127.0.0.1".into(),
            port: 5432,
            user: "postgres".into(),
            password: "password".into(),
            database_name: "test".into(),
        },
        config: Config::default(),
    };
    
    let replica_config = PoolConfig {
        address: Address {
            host: "127.0.0.1".into(),
            port: 5433,
            user: "postgres".into(),
            password: "password".into(),
            database_name: "test".into(),
        },
        config: Config::default(),
    };
    
    let primary = Pool::new(&primary_config);
    let replicas = Replicas::new(&[replica_config], LoadBalancingStrategy::Random);
    
    let max_lag_bytes = 1024 * 1024;
    let _checker = ReplicationLagChecker::new(&primary, &replicas, max_lag_bytes);
    
    // If we get here without panicking, the checker was created successfully
    assert!(true);
}

#[test]
fn test_lsn_parsing() {
    // Test LSN parsing logic
    use super::super::lag_check::ReplicationLagChecker;
    
    let primary_config = PoolConfig {
        address: Address {
            host: "127.0.0.1".into(),
            port: 5432,
            user: "postgres".into(),
            password: "password".into(),
            database_name: "test".into(),
        },
        config: Config::default(),
    };
    
    let primary = Pool::new(&primary_config);
    let replicas = Replicas::new(&[], LoadBalancingStrategy::Random);
    let checker = ReplicationLagChecker::new(&primary, &replicas, 1024);
    
    // Test valid LSN parsing
    assert_eq!(checker.parse_lsn("0/12345678"), Some(0x12345678));
    assert_eq!(checker.parse_lsn("1/0"), Some(0x100000000));
    
    // Debug the FF/FFFFFFFF case
    let result = checker.parse_lsn("FF/FFFFFFFF");
    println!("FF/FFFFFFFF parsed to: {:?}", result);
    // This should be 0xFF << 32 | 0xFFFFFFFF = 0xFF00000000 + 0xFFFFFFFF = 0xFFFFFFFFFF
    assert_eq!(result, Some(0xFFFFFFFFFF));
    
    // Test invalid LSN parsing
    assert_eq!(checker.parse_lsn("invalid"), None);
    assert_eq!(checker.parse_lsn("1/2/3"), None);
    assert_eq!(checker.parse_lsn(""), None);
}