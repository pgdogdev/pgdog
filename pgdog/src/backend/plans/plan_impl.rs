use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::Notify;
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
    launch: Arc<Notify>,
}

impl Plans {
    pub(crate) fn new(pool: &Pool) -> Self {
        let (tx, rx) = channel(4096);

        let pool = pool.clone();
        let task_pool = pool.clone();
        let cache = Arc::new(Mutex::new(PlanCache::new()));
        let task_cache = cache.clone();
        let launch = Arc::new(Notify::new());
        let launch_cache = launch.clone();

        tokio::spawn(async move {
            launch_cache.notified().await;
            Self::run(task_pool, rx, task_cache).await;
        });

        Self {
            pool,
            tx,
            cache,
            launch,
        }
    }

    pub(crate) fn launch(&self) {
        self.launch.notify_one();
    }

    pub(crate) fn shutdown(&self) {
        // The pool will be offline when this happens.
        self.launch();
    }

    pub(crate) async fn plan(&self, buffer: &Buffer) -> Result<Option<Arc<QueryPlan>>, Error> {
        let plan_req = PlanRequest::from_buffer(buffer)?;
        if plan_req.skip() {
            return Ok(None);
        }

        let key = Key::from(&plan_req);
        let now = Instant::now();
        let plan = {
            let mut guard = self.cache.lock();
            let plan = if let Some(existing) = guard.get_mut(&key) {
                match existing {
                    Value::Request(requested_at) => {
                        // A plan was requested recently, don't request another one.
                        if now.duration_since(*requested_at) < Duration::from_secs(5) {
                            return Ok(None);
                        }

                        None
                    }

                    Value::Plan { plan, created_at } => {
                        // Age the plan and request a new one.
                        if now.duration_since(*created_at) > Duration::from_secs(300) {
                            *created_at = now;
                        }

                        Some(plan.clone())
                    }
                }
            } else {
                None
            };

            // Create a request to prevent a thundering herd.
            guard.requested(key, now);

            plan
        };
        self.tx.send(plan_req).await?;
        Ok(plan)
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

            debug!("plans cache running [{}]", pool.addr());

            let mut conn =
                if let Ok(conn) = Server::connect(pool.addr(), ServerOptions::default()).await {
                    conn
                } else {
                    sleep(Duration::from_secs(1)).await;
                    continue;
                };

            loop {
                let req = rx.recv().await;

                debug!("plans request {:?} [{}]", req, pool.addr());

                if let Some(req) = req {
                    match req.load(&mut conn).await {
                        Ok(plan) => {
                            let created_at = Instant::now();
                            cache.lock().insert(&req, plan, created_at);
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
