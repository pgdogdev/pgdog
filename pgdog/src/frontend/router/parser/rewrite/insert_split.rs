use std::collections::BTreeSet;

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

#[cfg(test)]
mod test {
    use super::*;
    use crate::frontend::router::parser::{route::Route, Shard};

    #[test]
    fn values_sql_grouped_by_shard() {
        let rows = vec![
            InsertSplitRow::new(0, vec!["1".into(), "'a'".into()]),
            InsertSplitRow::new(1, vec!["11".into(), "'b'".into()]),
            InsertSplitRow::new(0, vec!["2".into(), "'c'".into()]),
        ];
        let plan = InsertSplitPlan::new(
            Route::write(Shard::Multi(vec![0, 1])),
            OwnedTable {
                name: "sharded".into(),
                ..Default::default()
            },
            vec!["\"id\"".into(), "\"value\"".into()],
            rows,
        );

        assert_eq!(plan.total_rows(), 3);
        assert_eq!(plan.shard_list(), &[0, 1]);
        assert_eq!(
            plan.values_sql_for_shard(0).as_deref(),
            Some("(1, 'a'), (2, 'c')")
        );
        assert_eq!(plan.values_sql_for_shard(1).as_deref(), Some("(11, 'b')"));
        assert!(plan.values_sql_for_shard(2).is_none());
    }
}
