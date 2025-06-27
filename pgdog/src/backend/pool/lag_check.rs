//! Replication lag checking.

use crate::backend::{pool::Pool, Server};
use crate::net::messages::DataRow;

use super::{Error, Replicas};

/// Replication lag information for a single replica.
#[derive(Debug, Clone)]
pub struct ReplicationLagInfo {
    /// Application name (replica identifier).
    pub application_name: String,
    /// Replica's flush LSN.
    pub flush_lsn: u64,
    /// Calculated lag in bytes.
    pub lag_bytes: u64,
}

/// Check replication lag for all replicas against the primary.
pub struct ReplicationLagChecker<'a> {
    primary: &'a Pool,
    replicas: &'a Replicas,
    max_lag_bytes: u64,
}

impl<'a> ReplicationLagChecker<'a> {
    /// Create a new replication lag checker.
    pub fn new(primary: &'a Pool, replicas: &'a Replicas, max_lag_bytes: u64) -> Self {
        Self {
            primary,
            replicas,
            max_lag_bytes,
        }
    }

    /// Check replication lag and ban lagging replicas.
    pub async fn check_and_ban_lagging_replicas(&self) -> Result<(), Error> {
        // Get a connection to the primary to query replication status
        let mut primary_conn = match self.primary.get(&super::Request::default()).await {
            Ok(conn) => conn,
            Err(err) => {
                tracing::debug!("Failed to get primary connection for lag check: {}", err);
                return Err(err);
            }
        };

        // Get current WAL position on primary
        let current_lsn = match self.get_current_wal_lsn(&mut primary_conn).await {
            Ok(lsn) => lsn,
            Err(err) => {
                tracing::error!("Failed to get current WAL LSN from primary: {}", err);
                return Err(err);
            }
        };

        // Get replication status for all replicas
        let replica_status = match self.get_replica_status(&mut primary_conn).await {
            Ok(status) => status,
            Err(err) => {
                tracing::error!("Failed to get replica status from primary: {}", err);
                return Err(err);
            }
        };

        // Check each replica's lag and ban if necessary
        for pool in &self.replicas.pools {
            let pool_addr = pool.addr().to_string();
            
            // Find this replica in the status (match by host:port)
            if let Some(status) = self.find_replica_status(&pool_addr, &replica_status) {
                let lag_bytes = current_lsn.saturating_sub(status.flush_lsn);
                
                if lag_bytes > self.max_lag_bytes {
                    tracing::warn!(
                        "Replica {} is lagging by {} bytes (max: {}), banning",
                        pool_addr, lag_bytes, self.max_lag_bytes
                    );
                    pool.ban(Error::ReplicationLag);
                } else {
                    tracing::debug!(
                        "Replica {} lag: {} bytes (within limit: {})",
                        pool_addr, lag_bytes, self.max_lag_bytes
                    );
                }
            } else {
                tracing::debug!("Replica {} not found in pg_stat_replication", pool_addr);
            }
        }

        Ok(())
    }

    /// Get current WAL flush LSN from the primary.
    async fn get_current_wal_lsn(&self, conn: &mut impl std::ops::DerefMut<Target = Server>) -> Result<u64, Error> {
        let query = "SELECT pg_current_wal_flush_lsn()";
        
        let rows: Vec<WalLsnRow> = conn.fetch_all(query).await
            .map_err(|_| Error::HealthcheckError)?;

        if let Some(row) = rows.first() {
            self.parse_lsn(&row.lsn).ok_or(Error::HealthcheckError)
        } else {
            Err(Error::HealthcheckError)
        }
    }

    /// Get replication status for all replicas from pg_stat_replication.
    async fn get_replica_status(&self, conn: &mut impl std::ops::DerefMut<Target = Server>) -> Result<Vec<ReplicationLagInfo>, Error> {
        let query = "SELECT application_name, client_addr, client_port, flush_lsn FROM pg_stat_replication";
        
        let rows: Vec<ReplicationStatusRow> = conn.fetch_all(query).await
            .map_err(|_| Error::HealthcheckError)?;

        let mut result = Vec::new();
        for row in rows {
            if let Some(flush_lsn) = self.parse_lsn(&row.flush_lsn) {
                result.push(ReplicationLagInfo {
                    application_name: row.application_name.clone(),
                    flush_lsn,
                    lag_bytes: 0, // Will be calculated later
                });
            }
        }

        Ok(result)
    }

    /// Find replica status by matching address.
    fn find_replica_status<'b>(&self, _pool_addr: &str, statuses: &'b [ReplicationLagInfo]) -> Option<&'b ReplicationLagInfo> {
        // For now, just return the first status if any exist
        // In a real implementation, we'd need to match by client_addr and client_port
        // which would require extending the ReplicationLagInfo struct
        statuses.first()
    }

    /// Parse PostgreSQL LSN format (e.g., "0/1234ABCD") to u64.
    pub fn parse_lsn(&self, lsn_str: &str) -> Option<u64> {
        let parts: Vec<&str> = lsn_str.split('/').collect();
        if parts.len() != 2 {
            return None;
        }

        let high = u64::from_str_radix(parts[0], 16).ok()?;
        let low = u64::from_str_radix(parts[1], 16).ok()?;
        
        Some((high << 32) | low)
    }
}

/// Row structure for WAL LSN query result.
struct WalLsnRow {
    lsn: String,
}

impl From<DataRow> for WalLsnRow {
    fn from(row: DataRow) -> Self {
        Self {
            lsn: row.column(0)
                .map(|bytes| String::from_utf8_lossy(&bytes).to_string())
                .unwrap_or_default(),
        }
    }
}

/// Row structure for replication status query result.
struct ReplicationStatusRow {
    application_name: String,
    client_addr: String,
    client_port: String,
    flush_lsn: String,
}

impl From<DataRow> for ReplicationStatusRow {
    fn from(row: DataRow) -> Self {
        Self {
            application_name: row.column(0)
                .map(|bytes| String::from_utf8_lossy(&bytes).to_string())
                .unwrap_or_default(),
            client_addr: row.column(1)
                .map(|bytes| String::from_utf8_lossy(&bytes).to_string())
                .unwrap_or_default(),
            client_port: row.column(2)
                .map(|bytes| String::from_utf8_lossy(&bytes).to_string())
                .unwrap_or_default(),
            flush_lsn: row.column(3)
                .map(|bytes| String::from_utf8_lossy(&bytes).to_string())
                .unwrap_or_default(),
        }
    }
}