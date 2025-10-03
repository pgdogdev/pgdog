use std::fmt::Display;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio::{select, spawn};
use tracing::info;

use super::Statement;

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
    TableDump {
        schema: String,
        name: String,
    },
    Other {
        sql: String,
    },
}

fn no_comments(sql: &str) -> String {
    let mut output = String::new();
    for line in sql.lines() {
        if line.trim().starts_with("--") || line.trim().is_empty() {
            continue;
        }
        output.push_str(line);
        output.push('\n');
    }

    let result = output.trim().replace("\n", " ");

    let mut prev_space = false;
    let mut collapsed = String::new();
    for c in result.chars() {
        if c == ' ' {
            if !prev_space {
                collapsed.push(c);
                prev_space = true;
            }
        } else {
            collapsed.push(c);
            prev_space = false;
        }
    }

    collapsed
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
            Self::TableDump { schema, name } => write!(f, "table \"{}\".\"{}\"", schema, name),
        }
    }
}

impl Item {
    fn action(&self) -> &str {
        match self {
            Self::Index { .. } => "creating",
            Self::Table { .. } => "creating",
            Self::Other { .. } => "executing",
            Self::TableDump { .. } => "fetching schema for",
        }
    }
}

impl From<&Statement<'_>> for Item {
    fn from(value: &Statement<'_>) -> Self {
        match value {
            Statement::Index { sql, .. } => Item::Other { sql: sql.clone() },
            Statement::Table { table, .. } => Item::Table {
                schema: table.schema.as_ref().unwrap_or(&"").to_string(),
                name: table.name.to_string(),
            },
            Statement::Other { sql, .. } => Item::Other {
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

enum Message {
    Next(Item),
    Shutdown,
}

#[derive(Clone)]
pub struct Progress {
    tx: mpsc::UnboundedSender<Message>,
    timer: Instant,
}

impl Progress {
    pub fn new(total: usize) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let timer = Instant::now();

        spawn(async move {
            Self::listen(rx, total).await;
        });

        Self { tx, timer }
    }

    pub fn done(&mut self) {
        let elapsed = self.timer.elapsed();
        info!("finished in {:.3}s", elapsed.as_secs_f64());
        self.timer = Instant::now();
    }

    pub fn next(&self, item: impl Into<Item>) {
        let _ = self.tx.send(Message::Next(item.into()));
    }

    async fn listen(mut rx: mpsc::UnboundedReceiver<Message>, total: usize) {
        let mut counter = 1;

        loop {
            select! {
                msg = rx.recv() => {
                    match msg {
                        Some(Message::Next(item)) => {
                            info!("[{}/{}] {} {}", counter, total, item.action(), item);
                            counter += 1;
                        }
                        Some(Message::Shutdown) | None => {
                            break;
                        }
                    }
                }
            }
        }
    }
}

impl Drop for Progress {
    fn drop(&mut self) {
        let _ = self.tx.send(Message::Shutdown);
    }
}
