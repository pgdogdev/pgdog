//! Integration tests asserting the output of admin commands.
//!
//! Each submodule connects to the live PgDog admin database (see
//! `rust::setup::admin_sqlx`) and verifies the shape and contents of a
//! command's output over the wire.
pub mod show_config;
pub mod show_version;
pub mod tasks;

use sqlx::{Column, Executor, Pool, Postgres, Row, TypeInfo};

/// Wire layout expected from `SHOW TASKS`.
const SHOW_TASKS_LAYOUT: &[(&str, &str)] = &[
    ("id", "INT8"),
    ("type", "TEXT"),
    ("started_at", "TEXT"),
    ("elapsed", "TEXT"),
    ("elapsed_ms", "INT8"),
];

/// A single row returned by `SHOW TASKS` with all fields already parsed and
/// validated. Construction only succeeds through [`Tasks::fetch`], which
/// checks the wire layout and field invariants before handing rows out.
#[derive(Debug, Clone)]
pub struct Task {
    pub id: i64,
    pub kind: String,
    pub started_at: String,
    pub elapsed: String,
    pub elapsed_ms: i64,
}

/// Parsed result of a `SHOW TASKS` admin command.
///
/// Call [`Tasks::fetch`] to issue the command, validate column layout and
/// every row's field invariants in one shot, and get back a typed collection
/// you can query with [`Tasks::find`] or iterate over [`Tasks::rows`].
pub struct Tasks {
    pub rows: Vec<Task>,
}

impl Tasks {
    /// Issue `SHOW TASKS` against `pool`, assert the wire layout, parse and
    /// validate every row, and return the collection.
    ///
    /// Panics on any layout mismatch, unexpected wire type, or field that
    /// violates an invariant (empty timestamp, negative elapsed_ms).
    pub async fn fetch(pool: &Pool<Postgres>) -> Self {
        let raw = pool.fetch_all("SHOW TASKS").await.unwrap();

        // assert_layout requires at least one row; skip when empty (valid — no tasks running).
        if !raw.is_empty() {
            assert_layout(&raw, SHOW_TASKS_LAYOUT);
        }

        let rows = raw
            .iter()
            .map(|row| {
                let id: i64 = row.get("id");
                let started_at: String = row.get("started_at");
                let elapsed: String = row.get("elapsed");
                let elapsed_ms: i64 = row.get("elapsed_ms");

                assert!(!started_at.is_empty(), "task {id}: started_at is empty");
                assert!(!elapsed.is_empty(), "task {id}: elapsed is empty");
                assert!(elapsed_ms >= 0, "task {id}: elapsed_ms is negative");

                Task {
                    id,
                    kind: row.get("type"),
                    started_at,
                    elapsed,
                    elapsed_ms,
                }
            })
            .collect();

        Self { rows }
    }

    /// Return the task with the given id, if present.
    pub fn find(&self, id: i64) -> Option<&Task> {
        self.rows.iter().find(|t| t.id == id)
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// Assert that `rows` is non-empty and that the first row's column layout
/// (name, wire type) matches `expected` exactly, in order.
///
/// Used by submodule tests for commands other than `SHOW TASKS`.
pub fn assert_layout(rows: &[sqlx::postgres::PgRow], expected: &[(&str, &str)]) {
    assert!(!rows.is_empty(), "expected at least one row");
    let actual: Vec<(&str, &str)> = rows[0]
        .columns()
        .iter()
        .map(|col| (col.name(), col.type_info().name()))
        .collect();
    assert_eq!(actual, expected, "column layout mismatch");
}
