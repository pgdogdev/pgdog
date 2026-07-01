//! Integration tests asserting admin command output over the wire.
//!
//! Each submodule connects to the live PgDog admin database (`rust::setup::admin_sqlx`).
pub mod show_config;
pub mod show_version;
pub mod tasks;

use sqlx::{Column, Executor, Pool, Postgres, Row, TypeInfo};

/// Wire layout expected from `SHOW TASKS`.
const SHOW_TASKS_LAYOUT: &[(&str, &str)] = &[
    ("id", "INT8"),
    ("scope", "TEXT"),
    ("type", "TEXT"),
    ("status", "TEXT"),
    ("inner_status", "TEXT"),
    ("started_at", "TEXT"),
    ("updated_at", "TEXT"),
    ("elapsed", "TEXT"),
    ("elapsed_ms", "INT8"),
];

/// A parsed, validated `SHOW TASKS` row. Built only via [`Tasks::fetch`].
#[derive(Debug, Clone)]
pub struct Task {
    pub id: Option<i64>,
    pub scope: String,
    pub kind: String,
    pub status: String,
    pub inner_status: String,
    pub started_at: String,
    pub updated_at: String,
    pub elapsed: String,
    pub elapsed_ms: i64,
}

/// Parsed result of `SHOW TASKS`; query with [`Tasks::find`] or [`Tasks::rows`].
pub struct Tasks {
    pub rows: Vec<Task>,
}

impl Tasks {
    /// Run `SHOW TASKS`, assert the wire layout and per-row invariants, and
    /// return the parsed rows. Panics on any layout/type/invariant violation.
    pub async fn fetch(pool: &Pool<Postgres>) -> Self {
        let raw = pool.fetch_all("SHOW TASKS").await.unwrap();

        // assert_layout needs a row; an empty result is valid (no tasks).
        if !raw.is_empty() {
            assert_layout(&raw, SHOW_TASKS_LAYOUT);
        }

        let rows = raw
            .iter()
            .map(|row| {
                let id: Option<i64> = row.get("id");
                let scope: String = row.get("scope");
                let status: String = row.get("status");
                let started_at: String = row.get("started_at");
                let updated_at: String = row.get("updated_at");
                let elapsed: String = row.get("elapsed");
                let elapsed_ms: i64 = row.get("elapsed_ms");

                assert!(!started_at.is_empty(), "task {id:?}: started_at is empty");
                assert!(!updated_at.is_empty(), "task {id:?}: updated_at is empty");
                assert!(!elapsed.is_empty(), "task {id:?}: elapsed is empty");
                assert!(!status.is_empty(), "task {id:?}: status is empty");
                assert!(elapsed_ms >= 0, "task {id:?}: elapsed_ms is negative");
                assert!(
                    scope == "root" || scope == "subtask",
                    "task {id:?}: unexpected scope {scope:?}"
                );

                Task {
                    id,
                    scope,
                    kind: row.get("type"),
                    status,
                    inner_status: row.get("inner_status"),
                    started_at,
                    updated_at,
                    elapsed,
                    elapsed_ms,
                }
            })
            .collect();

        Self { rows }
    }

    /// Return the (root) task with the given id, if present.
    pub fn find(&self, id: i64) -> Option<&Task> {
        self.rows.iter().find(|t| t.id == Some(id))
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// Assert the first row's column layout (name, wire type) matches `expected` exactly.
pub fn assert_layout(rows: &[sqlx::postgres::PgRow], expected: &[(&str, &str)]) {
    assert!(!rows.is_empty(), "expected at least one row");
    let actual: Vec<(&str, &str)> = rows[0]
        .columns()
        .iter()
        .map(|col| (col.name(), col.type_info().name()))
        .collect();
    assert_eq!(actual, expected, "column layout mismatch");
}
