//! Sharding key of the table a SELECT reads.
//!
//! A sharding key value lives on one shard, so shards never share one. Each
//! shard's `DISTINCT` aggregate over the key covers values no other shard
//! has, and a group that includes the key is complete on its shard.

use itertools::Itertools;
use pg_raw_parse::{Node, nodes};

use crate::backend::replication::ShardedTables;
use crate::frontend::router::parser::{AggregateFunction, Column, Function, Table};

/// SELECT list positions of `count(DISTINCT key)`, where `key` is the sharding key.
///
/// No two shards count the same key, so their counts add up to the distinct
/// count across all of them, with or without `GROUP BY`.
pub(crate) fn disjoint_distinct(select: &nodes::SelectStmt, tables: &ShardedTables) -> Vec<usize> {
    // Resolved on the first DISTINCT aggregate; most queries have none.
    let mut key = None;

    select
        .target_list()
        .iter()
        .enumerate()
        .filter(|(_, target)| {
            let Some(call) = Function::extract_func_call(target.val()) else {
                return false;
            };
            let is_count = call
                .funcname()
                .iter()
                .filter_map(Node::as_str)
                .next_back()
                .and_then(AggregateFunction::from_builtin_name)
                == Some(AggregateFunction::Count);
            if !call.agg_distinct || !is_count {
                return false;
            }
            let Ok(argument) = call.args().iter().exactly_one() else {
                return false;
            };

            key.get_or_insert_with(|| ShardingKey::new(select, tables))
                .as_ref()
                .is_some_and(|key| key.matches(argument))
        })
        .map(|(position, _)| position)
        .collect()
}

/// Answers whether an expression is the sharding key of the queried table.
struct ShardingKey<'a> {
    table: Table<'a>,
    tables: &'a ShardedTables,
}

impl<'a> ShardingKey<'a> {
    /// `None` unless the SELECT reads exactly one plain table. Joins,
    /// subqueries and functions in `FROM` can repeat a key on several shards,
    /// and a CTE can shadow the table's name.
    fn new(select: &'a nodes::SelectStmt, tables: &'a ShardedTables) -> Option<Self> {
        if select.with_clause().is_some() {
            return None;
        }
        let Ok(Node::RangeVar(range_var)) = select.from_clause().iter().exactly_one() else {
            return None;
        };

        Some(Self {
            table: Table::from(range_var),
            tables,
        })
    }

    /// Whether `node` is a bare reference to the sharding key column.
    /// Expressions over the key, e.g. `user_id % 2`, don't count:
    /// two shards can produce the same result.
    fn matches(&self, node: Node<'_>) -> bool {
        let Node::ColumnRef(column) = node else {
            return false;
        };
        let Ok(mut column) = Column::try_from(column) else {
            return false;
        };
        if !self.qualifier_matches(&column) {
            return false;
        }
        column.table = Some(self.table.name);
        column.schema = self.table.schema;

        self.tables.sharding_key(column).is_some()
    }

    /// Whether `GROUP BY` includes the sharding key, by name or by SELECT
    /// list position, so that no group spans shards.
    // Not called yet: the NULL key group spans shards and needs its own handling.
    #[cfg_attr(not(test), expect(dead_code))]
    fn in_group_by(&self, select: &nodes::SelectStmt) -> bool {
        select.group_clause().iter().any(|item| match item {
            // Postgres counts SELECT list positions from 1.
            Node::A_Const(ordinal) => ordinal
                .val()
                .and_then(|value| value.numeric_value::<i32>())
                .and_then(|ordinal| usize::try_from(ordinal).ok()?.checked_sub(1))
                .and_then(|position| select.target_list().iter().nth(position))
                .is_some_and(|target| self.matches(target.val())),
            item => self.matches(item),
        })
    }

    /// An aliased table is only known by its alias.
    fn qualifier_matches(&self, column: &Column<'_>) -> bool {
        match (column.schema, column.table, self.table.alias) {
            (None, None, _) => true,
            (None, Some(qualifier), Some(alias)) => qualifier == alias,
            (None, Some(qualifier), None) => qualifier == self.table.name,
            (Some(schema), Some(qualifier), None) => {
                qualifier == self.table.name && self.table.schema == Some(schema)
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frontend::router::sharding::ShardedTable;
    use pg_raw_parse::{Owned, make};
    use pgdog_config::{OmnishardedTable, SystemCatalogsBehavior};

    fn select(sql: &str) -> Owned<nodes::SelectStmt> {
        match pg_raw_parse::parse(sql)
            .expect("valid SQL")
            .stmts()
            .next()
            .expect("one statement")
        {
            Node::SelectStmt(stmt) => make::owned(|mem| mem.make_unique(stmt)),
            _ => panic!("not a select"),
        }
    }

    fn config(name: Option<&str>, column: &str) -> ShardedTable {
        ShardedTable {
            name: name.map(Into::into),
            column: column.into(),
            ..Default::default()
        }
    }

    fn orders_by_user() -> ShardedTables {
        ShardedTables::from([config(Some("orders"), "user_id")].as_slice())
    }

    /// Whether the first SELECT list item and the GROUP BY are on the sharding key.
    fn check(sql: &str, tables: &ShardedTables) -> Option<(bool, bool)> {
        let select = select(sql);
        let key = ShardingKey::new(&select, tables)?;
        let first = select.target_list().iter().next().expect("one target");

        Some((key.matches(first.val()), key.in_group_by(&select)))
    }

    #[test]
    fn finds_count_distinct_on_key() {
        let tables = orders_by_user();

        for (sql, expected) in [
            ("SELECT count(DISTINCT user_id) FROM orders", vec![0]),
            (
                "SELECT count(DISTINCT o.user_id)::int FROM orders o",
                vec![0],
            ),
            (
                "SELECT status, count(DISTINCT user_id), count(*) FROM orders GROUP BY status",
                vec![1],
            ),
            (
                "SELECT count(DISTINCT user_id) FILTER (WHERE status = 'paid') FROM orders",
                vec![0],
            ),
            ("SELECT count(user_id) FROM orders", vec![]),
            ("SELECT count(DISTINCT product_id) FROM orders", vec![]),
            ("SELECT count(DISTINCT user_id % 2) FROM orders", vec![]),
            (
                "SELECT count(DISTINCT (user_id, status)) FROM orders",
                vec![],
            ),
            // Summing the keys of each shard is a later change.
            ("SELECT sum(DISTINCT user_id) FROM orders", vec![]),
            (
                "SELECT count(DISTINCT user_id) FROM orders JOIN users ON users.id = orders.user_id",
                vec![],
            ),
        ] {
            assert_eq!(disjoint_distinct(&select(sql), &tables), expected, "{sql}");
        }
    }

    #[test]
    fn matches_bare_key_column() {
        let tables = orders_by_user();

        for (sql, expected) in [
            ("SELECT user_id FROM orders", true),
            ("SELECT orders.user_id FROM orders", true),
            ("SELECT public.orders.user_id FROM public.orders", true),
            ("SELECT o.user_id FROM orders o", true),
            ("SELECT product_id FROM orders", false),
            // Two shards can produce the same value.
            ("SELECT user_id % 2 FROM orders", false),
            ("SELECT user_id::text FROM orders", false),
            // The alias hides the table name.
            ("SELECT orders.user_id FROM orders o", false),
            ("SELECT other.user_id FROM orders", false),
            ("SELECT other.orders.user_id FROM public.orders", false),
            ("SELECT user_id FROM users", false),
        ] {
            assert_eq!(
                check(sql, &tables).map(|(first, _)| first),
                Some(expected),
                "{sql}"
            );
        }
    }

    #[test]
    fn requires_one_plain_table() {
        let tables = orders_by_user();

        for sql in [
            "SELECT user_id FROM orders JOIN users ON users.id = orders.user_id",
            "SELECT user_id FROM orders, users",
            "SELECT user_id FROM (SELECT user_id FROM orders) AS orders",
            "SELECT user_id FROM generate_series(1, 3) AS orders(user_id)",
            "WITH orders AS (SELECT 1 AS user_id) SELECT user_id FROM orders",
            "SELECT user_id FROM orders UNION SELECT user_id FROM orders",
            "SELECT 1",
        ] {
            assert!(check(sql, &tables).is_none(), "{sql}");
        }
    }

    #[test]
    fn finds_key_in_group_by() {
        let tables = orders_by_user();

        for (sql, expected) in [
            ("SELECT count(*) FROM orders GROUP BY user_id", true),
            (
                "SELECT count(*) FROM orders GROUP BY status, o.user_id",
                false,
            ),
            (
                "SELECT count(*) FROM orders o GROUP BY status, o.user_id",
                true,
            ),
            (
                "SELECT status, user_id, count(*) FROM orders GROUP BY 1, 2",
                true,
            ),
            ("SELECT status, count(*) FROM orders GROUP BY 1", false),
            ("SELECT count(*) FROM orders GROUP BY status", false),
            ("SELECT count(*) FROM orders GROUP BY user_id % 2", false),
            ("SELECT count(*) FROM orders", false),
            // The grand total group spans shards.
            (
                "SELECT count(*) FROM orders GROUP BY ROLLUP (user_id)",
                false,
            ),
        ] {
            assert_eq!(
                check(sql, &tables).map(|(_, group)| group),
                Some(expected),
                "{sql}"
            );
        }
    }

    #[test]
    fn follows_sharded_table_config() {
        // A config without a table name covers the column on every table.
        let column_only = ShardedTables::from([config(None, "user_id")].as_slice());
        assert_eq!(
            check("SELECT user_id FROM anything", &column_only),
            Some((true, false))
        );

        let omnisharded = ShardedTables::new(
            vec![config(None, "user_id")],
            vec![OmnishardedTable {
                name: "settings".into(),
                sticky_routing: false,
            }],
            false,
            SystemCatalogsBehavior::default(),
        );
        assert_eq!(
            check("SELECT user_id FROM settings", &omnisharded),
            Some((false, false))
        );
    }
}
