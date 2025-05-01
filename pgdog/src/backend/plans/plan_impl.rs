use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::sleep;
use tracing::{debug, error};

use crate::backend::{Pool, Server, ServerOptions};

use super::{Error, PlanCache, PlanRequest};

#[derive(Debug, Clone)]
pub struct Plans {
    pool: Pool,
    tx: Sender<PlanRequest>,
    cache: Arc<Mutex<PlanCache>>,
}

impl Plans {
    pub(crate) fn new(pool: &Pool) -> Self {
        let (req_tx, req_rx) = channel(4096);

        let pool = pool.clone();
        let task_pool = pool.clone();
        let cache = Arc::new(Mutex::new(PlanCache::new()));
        let task_cache = cache.clone();

        tokio::spawn(async move {
            Self::run(task_pool, req_rx, task_cache).await;
        });

        Self {
            pool,
            tx: req_tx,
            cache,
        }
    }

    pub(crate) async fn plan(&self, req: PlanRequest) -> Result<(), Error> {
        self.tx.send(req).await?;
        Ok(())
    }

    async fn run(pool: Pool, mut rx: Receiver<PlanRequest>, cache: Arc<Mutex<PlanCache>>) {
        loop {
            if !pool.online() {
                break;
            }

            if pool.banned() {
                sleep(Duration::from_secs(1)).await;
                continue;
            }

            debug!("plan cache running");

            let mut conn =
                if let Ok(conn) = Server::connect(pool.addr(), ServerOptions::default()).await {
                    conn
                } else {
                    sleep(Duration::from_secs(1)).await;
                    continue;
                };

            loop {
                let req = rx.recv().await;

                if let Some(req) = req {
                    match req.load(&mut conn).await {
                        Ok(plan) => {
                            cache.lock().insert(req, plan.into());
                        }

                        Err(err) => {
                            error!("plan execution error: {}", err);
                            break; // reconnect.
                        }
                    }
                } else {
                    return;
                }
            }
        }
    }
}
