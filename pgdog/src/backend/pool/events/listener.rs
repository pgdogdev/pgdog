use crate::backend::Error;
use crate::backend::Pool;
use crate::net::{DataRow, ErrorResponse, FromBytes, PgLsn, Protocol, ToBytes};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::watch;
use tokio::time::{interval, sleep, Duration};
use tokio::{select, spawn};
use tracing::{error, info};

use super::Connection;

#[derive(Debug)]
pub struct Listener {
    lsn: watch::Receiver<PgLsn>,
    online: Arc<AtomicBool>,
}

impl Listener {
    pub fn lsn(&self) -> PgLsn {
        *self.lsn.borrow()
    }

    pub async fn lsn_changed(&mut self) -> Result<PgLsn, super::Error> {
        self.lsn.changed().await?;
        Ok(*self.lsn.borrow_and_update())
    }

    pub fn online(&self) -> bool {
        self.online.load(Ordering::Relaxed)
    }

    pub fn shutdown(&self) {
        self.online.store(false, Ordering::Relaxed);
    }

    pub fn new(pool: &Pool) -> Self {
        let (lsn_tx, lsn_rx) = watch::channel(PgLsn::default());
        let listener = Self {
            lsn: lsn_rx,
            online: Arc::new(AtomicBool::new(true)),
        };

        let spawn_listener = listener.clone();
        let pool = pool.clone();
        spawn(async move {
            loop {
                if !pool.banned() {
                    if let Err(err) = spawn_listener.spawn(&pool, lsn_tx.clone()).await {
                        error!("listener error: {:?}", err);
                        if !spawn_listener.online() {
                            break;
                        }
                    }
                }
                sleep(Duration::from_secs(1)).await;
            }
        });

        listener
    }

    pub async fn spawn(&self, pool: &Pool, lsn_tx: watch::Sender<PgLsn>) -> Result<(), Error> {
        let mut conn = Connection::new(pool);
        conn.reconnect().await?;

        info!("listener connected [{}]", pool.addr());

        let mut interval = interval(Duration::from_millis(50));
        let mut replica: Option<bool> = None;
        let mut exit = false;

        loop {
            select! {
                _ = interval.tick() => {
                    if !self.online() {
                        break;
                    }
                    conn.execute("is_replica").await?;
                    match replica {
                        Some(true) => conn.execute("lsn_replica").await?,
                        Some(false) => conn.execute("lsn_primary").await?,
                        _ => (),
                    }
                }


                message = conn.read() => {
                    let message = message?;

                    if !self.online() {
                        break;
                    }

                    match message.code() {
                        'D' => {
                            let data_row = DataRow::from_bytes(message.to_bytes()?)?;

                            if let Some(pg_lsn) = Option::<PgLsn>::from(data_row.clone()) {
                                exit = lsn_tx.send(pg_lsn).is_err();
                            }

                            if let Some(r) = Option::<bool>::from(data_row) {
                                replica = Some(r);
                            }
                        }

                        'E' => {
                            let err = ErrorResponse::from_bytes(message.to_bytes()?)?;
                            error!("{} [{}]", err, conn.addr()?);
                            conn.reconnect().await?;
                        }

                        _ => (),
                    }
                }
            }

            if exit {
                break;
            }
        }

        Ok(())
    }
}

impl Clone for Listener {
    fn clone(&self) -> Self {
        Self {
            lsn: self.lsn.clone(),
            online: self.online.clone(),
        }
    }
}
