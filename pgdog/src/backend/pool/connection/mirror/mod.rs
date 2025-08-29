//! Client request mirroring.

use std::time::Duration;

use rand::{thread_rng, Rng};
use tokio::select;
use tokio::time::{sleep, Instant};
use tokio::{spawn, sync::mpsc::*};
use tracing::{debug, error, trace};

use crate::backend::Cluster;
use crate::config::{config, ConfigAndUsers};
use crate::frontend::client::query_engine::{QueryEngine, QueryEngineContext};
use crate::frontend::client::timeouts::Timeouts;
use crate::frontend::client::TransactionType;
use crate::frontend::comms::comms;
use crate::frontend::PreparedStatements;
use crate::net::{Parameter, Parameters, Stream};
use crate::stats::mirror::{categorize_error, MirrorErrorType, MirrorStats};
use std::sync::Arc;

use crate::frontend::ClientRequest;

use super::Error;

pub mod buffer_with_delay;
pub mod handler;
pub mod request;

pub use buffer_with_delay::*;
pub use handler::*;
pub use request::*;

/// Mirror handler. One is created for each client connected
/// to PgDog.
pub struct Mirror {
    /// Mirror's prepared statements. Should be similar
    /// to client's statements, if exposure is high.
    pub prepared_statements: PreparedStatements,
    /// Mirror connection parameters.
    pub params: Parameters,
    /// Timeouts.
    pub timeouts: Timeouts,
    /// Stream that absorbs all data.
    pub stream: Stream,
    /// Transaction state.
    pub transaction: Option<TransactionType>,
    /// Cross-shard queries.
    pub cross_shard_disabled: bool,
    /// Reference to destination cluster for health checks.
    pub cluster: Arc<Cluster>,
}

impl Mirror {
    fn new(params: &Parameters, config: &ConfigAndUsers, cluster: Arc<Cluster>) -> Self {
        Self {
            prepared_statements: PreparedStatements::new(),
            params: params.clone(),
            timeouts: Timeouts::from_config(&config.config.general),
            stream: Stream::DevNull,
            transaction: None,
            cross_shard_disabled: config.config.general.cross_shard_disabled,
            cluster,
        }
    }

    /// Spawn mirror task in the background.
    ///
    /// # Arguments
    ///
    /// * `cluster`: Destination cluster for mirrored traffic.
    ///
    /// # Return
    ///
    /// Handler for sending queries to the background task.
    ///
    pub fn spawn(cluster: &Cluster) -> Result<MirrorHandler, Error> {
        let config = config();
        Self::spawn_with_config(
            cluster,
            config.config.general.mirror_exposure,
            config.config.general.mirror_queue,
        )
    }

    pub fn spawn_with_config(
        cluster: &Cluster,
        exposure: f32,
        queue_depth: usize,
    ) -> Result<MirrorHandler, Error> {
        let config = config();
        let params = Parameters::from(vec![
            Parameter {
                name: "user".into(),
                value: cluster.user().into(),
            },
            Parameter {
                name: "database".into(),
                value: cluster.name().into(),
            },
        ]);

        // Same query engine as the client, except with a potentially different database config.
        let mut query_engine = QueryEngine::new(&params, &comms(), false, &None)?;

        // Create cluster Arc for sharing with mirror task
        let cluster_arc = Arc::new(cluster.clone());

        // Mirror traffic handler.
        let mut mirror = Self::new(&params, &config, cluster_arc.clone());

        // Get the database and user names for stats tracking
        let database_name = cluster.name().to_string();
        let user_name = cluster.user().to_string();

        // Mirror queue.
        let (tx, mut rx) = channel(queue_depth);
        let handler = MirrorHandler::new(tx, exposure, database_name.clone(), user_name.clone());

        spawn(async move {
            loop {
                select! {
                    req = rx.recv() => {
                        if let Some(mut req) = req {
                            // TODO: timeout these.
                            let start = Instant::now();
                            trace!("Mirror task: Processing request with {} buffers", req.buffer.len());
                            match mirror.handle(&mut req, &mut query_engine).await {
                                Ok(_) => {
                                    let latency_ms = start.elapsed().as_millis() as u64;
                                    trace!("Mirror task: Success, recording as mirrored");
                                    MirrorStats::instance().record_success(&database_name, &user_name, latency_ms);
                                }
                                Err(err) => {
                                    let error_type = categorize_mirror_error(&err);
                                    trace!("Mirror task: Error occurred, recording as error type: {:?}", error_type);
                                    MirrorStats::instance().record_error(&database_name, &user_name, error_type);
                                    error!("mirror error: {} (type: {:?})", err, error_type);
                                }
                            }
                        } else {
                            debug!("mirror client shutting down");
                            break;
                        }
                    }
                }
            }
        });

        Ok(handler)
    }

    /// Handle a single mirror request.
    pub async fn handle(
        &mut self,
        request: &mut MirrorRequest,
        query_engine: &mut QueryEngine,
    ) -> Result<(), Error> {
        // Check if destination cluster has any available pools
        // Check all shards for availability
        trace!(
            "Mirror: Checking {} shards for availability",
            self.cluster.shards().len()
        );
        let pools_available = self.cluster.shards().iter().any(|shard| {
            let pools = shard.pools();
            trace!("Mirror: Shard has {} pools", pools.len());
            let available = pools.iter().any(|pool| {
                let banned = pool.banned();
                trace!("Mirror: Pool {} banned: {}", pool.addr(), banned);
                !banned
            });
            available
        });

        if !pools_available {
            // All pools are banned, return connection error
            trace!("Mirror: All pools banned, returning error");
            return Err(Error::Pool(crate::backend::pool::Error::Banned));
        }

        debug!("mirroring {} client requests", request.buffer.len());
        for req in &mut request.buffer {
            if req.delay > Duration::ZERO {
                sleep(req.delay).await;
            }

            let mut context = QueryEngineContext::new_mirror(self, &mut req.buffer);
            query_engine.handle(&mut context).await?;
            self.transaction = context.transaction();
        }

        Ok(())
    }
}

/// Categorize a mirror error into a specific error type.
fn categorize_mirror_error(err: &Error) -> MirrorErrorType {
    use crate::backend::pool::Error as PoolError;

    match err {
        Error::Pool(pool_err) => match pool_err {
            PoolError::ConnectTimeout
            | PoolError::CheckoutTimeout
            | PoolError::ReplicaCheckoutTimeout
            | PoolError::HealthcheckTimeout => MirrorErrorType::Timeout,
            PoolError::ServerError
            | PoolError::ManualBan
            | PoolError::Banned
            | PoolError::Offline
            | PoolError::NoPrimary
            | PoolError::AllReplicasDown
            | PoolError::NoDatabases => MirrorErrorType::Connection,
            _ => {
                // Other pool errors - categorize by string
                categorize_error(&pool_err.to_string())
            }
        },
        Error::ReadTimeout => MirrorErrorType::Timeout,

        Error::NotConnected | Error::NotInSync | Error::ConnectionError(_) => {
            MirrorErrorType::Connection
        }

        // IO errors are typically connection-related
        Error::Io(_) => MirrorErrorType::Connection,

        // Fall back to string categorization for other error types
        _ => {
            let error_str = err.to_string();
            categorize_error(&error_str)
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{backend::pool::Request, config, net::Query};

    use super::*;

    #[tokio::test]
    async fn test_mirror_exposure() {
        let (tx, rx) = channel(25);
        let mut handle = MirrorHandler::new(
            tx.clone(),
            1.0,
            "test_db".to_string(),
            "test_user".to_string(),
        );

        for _ in 0..25 {
            assert!(
                handle.send(&vec![].into()),
                "did not to mirror with 1.0 exposure"
            );
            assert!(handle.flush(), "flush didn't work with 1.0 exposure");
        }

        assert_eq!(rx.len(), 25);

        let (tx, rx) = channel(25);

        let mut handle = MirrorHandler::new(
            tx.clone(),
            0.5,
            "test_db".to_string(),
            "test_user".to_string(),
        );
        let dropped = (0..25)
            .into_iter()
            .map(|_| handle.send(&vec![].into()) && handle.send(&vec![].into()) && handle.flush())
            .filter(|s| !s)
            .count();
        let received = 25 - dropped;
        assert_eq!(
            rx.len(),
            received,
            "received more than should of with 50% exposure: {}",
            received
        );
        assert!(
            dropped <= 25 && dropped > 15,
            "dropped should be somewhere near 50%, but actually is {}",
            dropped
        );
    }

    #[tokio::test]
    async fn test_mirror() {
        config::test::load_test();
        let cluster = Cluster::new_test();
        cluster.launch();
        let mut mirror = Mirror::spawn(&cluster).unwrap();
        let mut conn = cluster.primary(0, &Request::default()).await.unwrap();

        for _ in 0..3 {
            assert!(
                mirror.send(&vec![Query::new("BEGIN").into()].into()),
                "mirror didn't send BEGIN"
            );
            assert!(
                mirror.send(
                    &vec![
                        Query::new("CREATE TABLE IF NOT EXISTS pgdog.test_mirror(id BIGINT)")
                            .into()
                    ]
                    .into()
                ),
                "mirror didn't send SELECT 1"
            );
            assert!(
                mirror.send(&vec![Query::new("COMMIT").into()].into()),
                "mirror didn't send commit"
            );
            assert_eq!(
                mirror.buffer().len(),
                3,
                "mirror buffer should have 3 requests"
            );
            sleep(Duration::from_millis(50)).await;
            // Nothing happens until we flush.
            assert!(
                conn.execute("DROP TABLE pgdog.test_mirror").await.is_err(),
                "table pgdog.test_mirror shouldn't exist yet"
            );
            assert!(mirror.flush(), "mirror didn't flush");
            sleep(Duration::from_millis(50)).await;
            assert!(
                conn.execute("DROP TABLE pgdog.test_mirror").await.is_ok(),
                "pgdog.test_mirror should exist"
            );
            assert!(mirror.buffer().is_empty(), "mirror buffer should be empty");
        }

        cluster.shutdown();
    }
}
