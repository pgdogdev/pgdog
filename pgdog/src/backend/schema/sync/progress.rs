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
    // SequenceOwner {
    //     sequence: String,
    //     owner: String,
    // },
    Other {
        sql: String,
    },
}

// Remove pg_dump comments.
// Only for displaying purposes! Don't use for executing queries.
fn no_comments(sql: &str) -> String {
    let mut output = String::new();
    for line in sql.lines() {
        if line.trim().starts_with("--") || line.trim().is_empty() {
            continue;
        }
        output.push_str(line);
        output.push('\n');
    }

    output.trim().to_string()
}

impl Default for Item {
    fn default() -> Self {
        Self::Other { sql: "".into() }
    }
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
            Self::Other { sql } => write!(f, "\"{}\"", no_comments(sql)),
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
            Statement::SequenceOwner { sql, .. } => Item::Other {
                sql: sql.to_string(),
            },
            Statement::SequenceSetMax { sql, .. } => Item::Other {
                sql: sql.to_string(),
            },
        }
    }
}

struct ItemTracker {
    item: Item,
    timer: Instant,
}

impl ItemTracker {
    fn new() -> Self {
        Self {
            item: Item::default(),
            timer: Instant::now(),
        }
    }
}

struct Comms {
    updated: Notify,
    shutdown: Notify,
}
impl Comms {
    fn new() -> Self {
        Self {
            updated: Notify::new(),
            shutdown: Notify::new(),
        }
    }
}

#[derive(Clone)]
pub struct Progress {
    item: Arc<Mutex<ItemTracker>>,
    comms: Arc<Comms>,
    total: usize,
}

impl Progress {
    pub fn new(total: usize) -> Self {
        let me = Self {
            item: Arc::new(Mutex::new(ItemTracker::new())),
            comms: Arc::new(Comms::new()),
            total,
        };

        let task = me.clone();

        spawn(async move {
            task.listen().await;
        });

        me
    }

    pub fn done(&self) {
        let elapsed = self.item.lock().timer.elapsed();

        info!("finished in {:.3}s", elapsed.as_secs_f64());
    }

    pub fn next(&self, item: impl Into<Item>) {
        {
            let mut guard = self.item.lock();
            guard.item = item.into().clone();
            guard.timer = Instant::now();
        }
        self.comms.updated.notify_one();
    }

    async fn listen(&self) {
        let mut counter = 1;

        loop {
            select! {
                _ = self.comms.updated.notified() => {
                    let item = self.item.lock().item.clone();
                    info!("[{}/{}] {} {}", counter, self.total, item.action(), item);
                    counter += 1;
                }

                _ = self.comms.shutdown.notified() => {
                    break;
                }
            }
        }
    }
}

impl Drop for Progress {
    fn drop(&mut self) {
        self.comms.shutdown.notify_one();
    }
}
