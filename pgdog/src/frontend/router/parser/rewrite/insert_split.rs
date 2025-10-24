use std::collections::{BTreeMap, BTreeSet};

use crate::frontend::router::parser::{route::Route, table::OwnedTable};

#[derive(Debug, Clone)]
pub struct InsertSplitRow {
    shard: usize,
    values: Vec<String>,
}

impl InsertSplitRow {
    pub fn new(shard: usize, values: Vec<String>) -> Self {
        Self { shard, values }
    }

    pub fn shard(&self) -> usize {
        self.shard
    }

    pub fn values(&self) -> &[String] {
        &self.values
    }
}

#[derive(Debug, Clone)]
pub struct InsertSplitPlan {
    route: Route,
    table: OwnedTable,
    columns: Vec<String>,
    rows: Vec<InsertSplitRow>,
    total_rows: usize,
    shard_list: Vec<usize>,
}

impl InsertSplitPlan {
    pub fn new(
        route: Route,
        table: OwnedTable,
        columns: Vec<String>,
        rows: Vec<InsertSplitRow>,
    ) -> Self {
        let total_rows = rows.len();
        let shard_set: BTreeSet<usize> = rows.iter().map(|row| row.shard()).collect();
        let shard_list = shard_set.into_iter().collect();

        Self {
            route,
            table,
            columns,
            rows,
            total_rows,
            shard_list,
        }
    }

    pub fn route(&self) -> &Route {
        &self.route
    }

    pub fn table(&self) -> &OwnedTable {
        &self.table
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    pub fn rows(&self) -> &[InsertSplitRow] {
        &self.rows
    }

    pub fn shard_list(&self) -> &[usize] {
        &self.shard_list
    }

    pub fn total_rows(&self) -> usize {
        self.total_rows
    }

    pub fn values_by_shard(&self) -> BTreeMap<usize, Vec<&InsertSplitRow>> {
        let mut map: BTreeMap<usize, Vec<&InsertSplitRow>> = BTreeMap::new();
        for row in &self.rows {
            map.entry(row.shard()).or_default().push(row);
        }
        map
    }

    pub fn values_sql_for_shard(&self, shard: usize) -> Option<String> {
        let rows = self
            .rows
            .iter()
            .filter(|row| row.shard() == shard)
            .collect::<Vec<_>>();

        if rows.is_empty() {
            return None;
        }

        let values = rows
            .iter()
            .map(|row| format!("({})", row.values().join(", ")))
            .collect::<Vec<_>>()
            .join(", ");

        Some(values)
    }
}
