use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::sleep;
use tracing::{debug, error};

use crate::backend::pool::Request;
use crate::backend::{Pool, Server, ServerOptions};
use crate::frontend::Buffer;

use super::cache::Key;
use super::{Error, PlanCache, PlanRequest, QueryPlan, Value};

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

    pub(crate) async fn plan(
        &self,
        req: &Request,
        buffer: &Buffer,
    ) -> Result<Option<Arc<QueryPlan>>, Error> {
        let plan_req = PlanRequest::from_buffer(buffer)?;
        let key = Key::from(&plan_req);
        if let Some(existing) = self.cache.lock().get(&key) {
            match existing {
                Value::Request(requested_at) => {
                    if req.created_at.duration_since(requested_at) < Duration::from_secs(5) {
                        return Ok(None);
                    }
                }

                Value::Plan(plan) => return Ok(Some(plan)),
            }
        }
        // Create a request to prevent a thundering herd.
        self.cache.lock().requested(key, req.created_at);
        self.tx.send(plan_req).await?;
        Ok(None)
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
                            cache.lock().insert(&req, plan);
                        }

                        Err(err) => {
                            cache.lock().remove(&req);
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
