//! Client request mirroring.

use std::time::Duration;

use rand::{rng, Rng};
use tokio::select;
use tokio::time::{sleep, Instant};
use tokio::{spawn, sync::mpsc::*};
use tracing::{debug, error, warn};

use crate::backend::Cluster;
use crate::config::{config, ConfigAndUsers};
use crate::frontend::client::query_engine::{QueryEngine, QueryEngineContext};
use crate::frontend::client::timeouts::Timeouts;
use crate::frontend::client::TransactionType;
use crate::frontend::{ClientComms, PreparedStatements};
use crate::net::{BackendKeyData, Parameter, Parameters, Stream};

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
#[derive(Debug)]
pub struct Mirror {
    /// Random identifier for this mirror connection.
    pub id: BackendKeyData,
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
}

impl Mirror {
    fn new(params: &Parameters, config: &ConfigAndUsers) -> Self {
        Self {
            id: BackendKeyData::new(),
            prepared_statements: PreparedStatements::new(),
            params: params.clone(),
            timeouts: Timeouts::from_config(&config.config.general),
            stream: Stream::dev_null(),
            transaction: None,
            cross_shard_disabled: config.config.general.cross_shard_disabled,
        }
    }

    /// Spawn mirror task in the background.
    ///
    /// # Arguments
    ///
    /// * `source_db`: Source database name for mirrored traffic.
    /// * `cluster`: Destination cluster for mirrored traffic.
    /// * `mirror_config`: Optional precomputed mirror configuration.
    ///
    /// # Return
    ///
    /// Handler for sending queries to the background task.
    ///
    pub fn spawn(
        _source_db: &str,
        cluster: &Cluster,
        mirror_config: Option<&crate::config::MirrorConfig>,
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
        let mut query_engine = QueryEngine::new(
            &params,
            &ClientComms::new(&BackendKeyData::new()),
            false,
            &None,
        )?;

        // Mirror must read server responses to keep the connection synchronized,
        // so disable test_mode which skips reading responses.
        #[cfg(test)]
        query_engine.set_test_mode(false);

        // Mirror traffic handler.
        let mut mirror = Self::new(&params, &config);

        // Use provided mirror config or fall back to global defaults
        let mirror_config = mirror_config
            .cloned()
            .unwrap_or_else(|| crate::config::MirrorConfig {
                queue_length: config.config.general.mirror_queue,
                exposure: config.config.general.mirror_exposure,
            });

        // Mirror queue.
        let (tx, mut rx) = channel(mirror_config.queue_length);
        let handler = MirrorHandler::new(tx, mirror_config.exposure, cluster.stats());

        let stats_for_errors = cluster.stats();
        spawn(async move {
            loop {
                select! {
                    req = rx.recv() => {
                        if let Some(mut req) = req {
                            // Decrement queue_length when we receive a message from the channel
                            {
                                let mut stats = stats_for_errors.lock();
                                stats.counts.queue_length = stats.counts.queue_length.saturating_sub(1);
                            }
                            // TODO: timeout these.
                            if let Err(err) = mirror.handle(&mut req, &mut query_engine).await {
                                error!("mirror error: {}", err);
                                // Increment error count on mirror handling error
                                let mut stats = stats_for_errors.lock();
                                stats.counts.error_count += 1;
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

#[cfg(test)]
mod test {
    use crate::{backend::pool::Request, config, net::Query};

    use super::*;

    #[tokio::test]
    async fn test_mirror_exposure() {
        use crate::backend::pool::MirrorStats;
        use parking_lot::Mutex;
        use std::sync::Arc;

        let (tx, rx) = channel(25);
        let stats = Arc::new(Mutex::new(MirrorStats::default()));
        let mut handle = MirrorHandler::new(tx.clone(), 1.0, stats.clone());

        for _ in 0..25 {
            assert!(
                handle.send(&vec![].into()),
                "did not to mirror with 1.0 exposure"
            );
            assert!(handle.flush(), "flush didn't work with 1.0 exposure");
        }

        assert_eq!(rx.len(), 25);

        let (tx, rx) = channel(25);
        let stats2 = Arc::new(Mutex::new(MirrorStats::default()));
        let mut handle = MirrorHandler::new(tx.clone(), 0.5, stats2);
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
        config::load_test();
        let cluster = Cluster::new_test(&config());
        cluster.launch();
        let mut mirror = Mirror::spawn("pgdog", &cluster, None).unwrap();
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
            sleep(Duration::from_millis(100)).await;
            // Nothing happens until we flush.
            assert!(
                conn.execute("DROP TABLE pgdog.test_mirror").await.is_err(),
                "table pgdog.test_mirror shouldn't exist yet"
            );
            assert!(mirror.flush(), "mirror didn't flush");
            sleep(Duration::from_millis(100)).await;
            assert!(
                conn.execute("DROP TABLE pgdog.test_mirror").await.is_ok(),
                "pgdog.test_mirror should exist"
            );
            assert!(mirror.buffer().is_empty(), "mirror buffer should be empty");
        }

        cluster.shutdown();
    }

    #[tokio::test]
    async fn test_mirror_stats_tracking() {
        config::load_test();
        let cluster = Cluster::new_test(&config());
        cluster.launch();

        // Get initial stats
        let initial_stats = {
            let stats_arc = cluster.stats();
            let stats = stats_arc.lock();
            stats.counts
        };

        let mut mirror = Mirror::spawn("pgdog", &cluster, None).unwrap();

        // Send a simple transaction
        assert!(mirror.send(&vec![Query::new("BEGIN").into()].into()));
        assert!(mirror.send(&vec![Query::new("SELECT 1").into()].into()));
        assert!(mirror.send(&vec![Query::new("COMMIT").into()].into()));

        // Flush should increment stats
        assert!(mirror.flush());

        // Wait for async processing
        sleep(Duration::from_millis(100)).await;

        // Verify stats were incremented
        let final_stats = {
            let stats_arc = cluster.stats();
            let stats = stats_arc.lock();
            stats.counts
        };

        assert_eq!(
            final_stats.total_count,
            initial_stats.total_count + 1,
            "total_count should be incremented by 1, was {} now {}",
            initial_stats.total_count,
            final_stats.total_count
        );
        assert_eq!(
            final_stats.mirrored_count,
            initial_stats.mirrored_count + 1,
            "mirrored_count should be incremented by 1, was {} now {}",
            initial_stats.mirrored_count,
            final_stats.mirrored_count
        );

        cluster.shutdown();
    }
}
