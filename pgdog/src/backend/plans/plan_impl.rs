use std::{
    sync::mpsc::{Receiver, Sender},
    time::Duration,
};

use tokio::time::sleep;

use crate::backend::{pool::Address, Pool, Server, ServerOptions};

use super::{PlanRequest, QueryPlan};

#[derive(Debug)]
pub struct Plans {
    pool: Pool,
    tx: Sender<PlanRequest>,
    rx: Receiver<QueryPlan>,
}

impl Plans {
    async fn run(pool: Pool, rx: Receiver<PlanRequest>, tx: Sender<QueryPlan>) {
        loop {
            if pool.banned() {
                sleep(Duration::from_secs(1)).await;
                continue;
            }

            if !pool.online() {
                break;
            }

            let mut conn =
                if let Ok(conn) = Server::connect(pool.addr(), ServerOptions::default()).await {
                    conn
                } else {
                    sleep(Duration::from_secs(1)).await;
                    continue;
                };
        }
    }
}
