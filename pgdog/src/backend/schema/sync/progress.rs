use std::fmt::Display;
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio::{select, spawn};
use tracing::info;

use super::Statement;
use parking_lot::Mutex;

#[derive(Clone)]
pub enum Item {
    Index {
        schema: String,
        table: String,
        name: String,
    },
    Table {
        schema: String,
        name: String,
    },
    Other {
        sql: String,
    },
}

impl Display for Item {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Index {
                schema,
                table,
                name,
            } => write!(
                f,
                "index \"{}\" on table \"{}\".\"{}\"",
                name, schema, table
            ),

            Self::Table { schema, name } => write!(f, "table \"{}\".\"{}\"", schema, name),
            Self::Other { sql } => write!(f, "\"{}\"", sql),
        }
    }
}

impl Item {
    fn action(&self) -> &str {
        match self {
            Self::Index { .. } => "creating",
            Self::Table { .. } => "creating",
            Self::Other { .. } => "executing",
        }
    }
}

impl From<&Statement<'_>> for Item {
    fn from(value: &Statement<'_>) -> Self {
        match value {
            Statement::Index { table, name, .. } => Item::Index {
                schema: table.schema.as_ref().unwrap_or(&"").to_string(),
                table: table.name.to_string(),
                name: name.to_string(),
            },
            Statement::Table { table, .. } => Item::Table {
                schema: table.schema.as_ref().unwrap_or(&"").to_string(),
                name: table.name.to_string(),
            },
            Statement::Other { sql } => Item::Other {
                sql: sql.to_string(),
            },
        }
    }
}

#[derive(Clone)]
pub struct Progress {
    item: Arc<Mutex<Item>>,
    updated: Arc<Notify>,
    shutdown: Arc<Notify>,
    total: usize,
    timer: Arc<Mutex<Instant>>,
}

impl Progress {
    pub fn new(total: usize) -> Self {
        let me = Self {
            item: Arc::new(Mutex::new(Item::Other { sql: "".into() })),
            updated: Arc::new(Notify::new()),
            shutdown: Arc::new(Notify::new()),
            total,
            timer: Arc::new(Mutex::new(Instant::now())),
        };

        let task = me.clone();

        spawn(async move {
            task.listen().await;
        });

        me
    }

    pub fn done(&self) {
        let elapsed = self.timer.lock().elapsed();
        let item = self.item.lock().clone();

        info!("{:.3}s {}", elapsed.as_secs(), item);
    }

    pub fn next(&self, item: impl Into<Item>) {
        *self.item.lock() = item.into().clone();
        *self.timer.lock() = Instant::now();
        self.updated.notify_one();
    }

    async fn listen(&self) {
        let mut counter = 1;

        loop {
            select! {
                _ = self.updated.notified() => {
                    let item = self.item.lock().clone();
                    info!("{} {} [{}/{}]", item.action(), item, counter, self.total);
                    counter += 1;
                }

                _ = self.shutdown.notified() => {
                    break;
                }
            }
        }
    }
}

impl Drop for Progress {
    fn drop(&mut self) {
        self.shutdown.notify_one();
    }
}
