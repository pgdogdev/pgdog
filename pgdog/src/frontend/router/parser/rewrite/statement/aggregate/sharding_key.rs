//! Sharding key of the table a SELECT reads.
//!
//! A sharding key value lives on one shard, so shards never share one. Each
//! shard's `DISTINCT` aggregate over the key covers values no other shard
//! has, and a group that includes the key is complete on its shard.

use itertools::Itertools;
use pg_raw_parse::{Node, nodes};
use pgdog_stats::Relation;

use crate::backend::replication::ShardedTables;
use crate::backend::schema::Schema;
use crate::frontend::router::parser::rewrite::statement::projection::Positions;
use crate::frontend::router::parser::{AggregateFunction, Column, Function, Table};

/// SELECT list positions of `count`, `sum` and `avg` over `DISTINCT key`,
/// where `key` is the sharding key.
///
/// No two shards hold the same key, so each shard aggregates keys no other
/// shard has and the results merge like the plain aggregate's, with or
/// without `GROUP BY`.
///
/// Variance and standard deviation are left out: their helper sums
/// `DISTINCT key * key`, and two keys can share a square.
pub(crate) fn disjoint_distinct(select: &nodes::SelectStmt, tables: &ShardedTables) -> Positions {
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
            // Checked first: it's the cheapest test and most aggregates fail it.
            if !call.agg_distinct {
                return false;
            }
            let function = call
                .funcname()
                .iter()
                .filter_map(Node::as_str)
                .next_back()
                .and_then(AggregateFunction::from_builtin_name);
            if !matches!(
                function,
                Some(AggregateFunction::Count | AggregateFunction::Sum | AggregateFunction::Avg)
            ) {
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

/// Whether every group of the SELECT is on one shard: `GROUP BY` includes the
/// sharding key and the key can't be `NULL`.
///
/// Each shard then returns complete groups, with nothing left to merge:
/// aggregates and `HAVING` are final as the shard computed them.
///
/// Rows with a `NULL` key are written to every shard, so their group would
/// come back from all of them.
pub(crate) fn groups_stay_on_one_shard(
    select: &nodes::SelectStmt,
    schema: &Schema,
    tables: &ShardedTables,
) -> bool {
    // Most queries have no GROUP BY and never look at FROM.
    !select.group_clause().is_empty()
        && ShardingKey::new(select, tables).is_some_and(|key| {
            key.group_by_key(select)
                .is_some_and(|column| key.is_not_null(column, schema))
        })
}

/// Answers whether an expression is the sharding key of the queried table.
struct ShardingKey<'a> {
    table: Table<'a>,
    tables: &'a ShardedTables,
}

impl<'a> ShardingKey<'a> {
    /// `None` unless the SELECT reads exactly one plain table. Joins,
    /// subqueries and functions in `FROM` can repeat a key on several shards,
    /// a CTE can shadow the table's name, and `AS t(a, b)` renames its columns.
    fn new(select: &'a nodes::SelectStmt, tables: &'a ShardedTables) -> Option<Self> {
        if select.with_clause().is_some() {
            return None;
        }
        let Ok(Node::RangeVar(range_var)) = select.from_clause().iter().exactly_one() else {
            return None;
        };
        if range_var
            .alias()
            .is_some_and(|alias| !alias.colnames().is_empty())
        {
            return None;
        }

        Some(Self {
            table: Table::from(range_var),
            tables,
        })
    }

    /// Name of the sharding key column, if `node` is a bare reference to it.
    /// Expressions over the key, e.g. `user_id % 2`, don't count:
    /// two shards can produce the same result.
    fn key_column(&self, node: Node<'_>) -> Option<&'a str> {
        let Node::ColumnRef(column) = node else {
            return None;
        };
        let mut column = Column::try_from(column).ok()?;
        if !self.qualifier_matches(&column) {
            return None;
        }
        column.table = Some(self.table.name);
        column.schema = self.table.schema;

        self.tables
            .sharding_key(column)
            .map(|table| table.column.as_str())
    }

    /// Whether `node` is a bare reference to the sharding key column.
    fn matches(&self, node: Node<'_>) -> bool {
        self.key_column(node).is_some()
    }

    /// The sharding key column `GROUP BY` includes, by name or by SELECT
    /// list position.
    fn group_by_key(&self, select: &nodes::SelectStmt) -> Option<&'a str> {
        select.group_clause().iter().find_map(|item| match item {
            // Postgres counts SELECT list positions from 1.
            Node::A_Const(ordinal) => ordinal
                .val()
                .and_then(|value| value.numeric_value::<i32>())
                .and_then(|ordinal| usize::try_from(ordinal).ok()?.checked_sub(1))
                .and_then(|position| select.target_list().iter().nth(position))
                .and_then(|target| self.key_column(target.val())),
            item => self.key_column(item),
        })
    }

    /// Whether the table declares `column` as `NOT NULL`. A table missing
    /// from the schema, e.g. before the schema is loaded, doesn't.
    fn is_not_null(&self, column: &str, schema: &Schema) -> bool {
        let not_null = |relation: &Relation| {
            relation
                .columns
                .get(column)
                .is_some_and(|column| !column.is_nullable)
        };

        match self.table.schema {
            Some(name) => schema.get(name, self.table.name).is_some_and(not_null),
            // The search path isn't known here, so every table with this name
            // has to agree.
            None => {
                let mut relations = schema
                    .relations
                    .values()
                    .filter_map(|relations| relations.get(self.table.name))
                    .peekable();
                relations.peek().is_some() && relations.all(not_null)
            }
        }
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
    use crate::backend::schema::relation::Relation as SchemaRelation;
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

        Some((
            key.matches(first.val()),
            key.group_by_key(&select).is_some(),
        ))
    }

    #[test]
    fn finds_distinct_aggregates_on_key() {
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
            (
                "SELECT sum(DISTINCT user_id), avg(DISTINCT user_id)::float8 FROM orders",
                vec![0, 1],
            ),
            ("SELECT sum(DISTINCT product_id) FROM orders", vec![]),
            // Two keys can share a square, which the sum of squares would count once.
            (
                "SELECT variance(DISTINCT user_id), stddev(DISTINCT user_id) FROM orders",
                vec![],
            ),
            (
                "SELECT count(DISTINCT user_id) FROM orders JOIN users ON users.id = orders.user_id",
                vec![],
            ),
        ] {
            assert_eq!(
                disjoint_distinct(&select(sql), &tables),
                Positions::from_iter(expected),
                "{sql}"
            );
        }
    }

    /// Schema with an `orders` table in each of `schemas`, given as
    /// (schema name, whether `user_id` is nullable).
    fn orders_schema(schemas: &[(&str, bool)]) -> Schema {
        let relations = schemas
            .iter()
            .map(|&(schema, nullable)| {
                let columns = ["user_id", "product_id"]
                    .into_iter()
                    .map(|name| {
                        let column = pgdog_stats::Column {
                            column_name: name.into(),
                            is_nullable: nullable || name != "user_id",
                            ..Default::default()
                        };
                        (name.to_owned(), column.into())
                    })
                    .collect();
                (
                    (schema.to_owned(), "orders".to_owned()),
                    SchemaRelation::test_table(schema, "orders", columns),
                )
            })
            .collect();

        Schema::from_parts(vec!["public".into()], relations)
    }

    #[test]
    fn groups_stay_on_one_shard_when_grouped_by_not_null_key() {
        let tables = orders_by_user();
        let not_null = orders_schema(&[("public", false)]);
        let nullable = orders_schema(&[("public", true)]);
        // The search path could resolve `orders` to either table.
        let mixed = orders_schema(&[("public", false), ("archive", true)]);

        for (sql, schema, expected) in [
            (
                "SELECT user_id, count(DISTINCT product_id) FROM orders GROUP BY user_id",
                &not_null,
                true,
            ),
            (
                "SELECT user_id, status, string_agg(note, ',') FROM orders GROUP BY 1, 2",
                &not_null,
                true,
            ),
            (
                "SELECT count(*) FROM orders o GROUP BY status, o.user_id HAVING count(*) > 1",
                &not_null,
                true,
            ),
            (
                "SELECT count(*) FROM public.orders GROUP BY user_id",
                &mixed,
                true,
            ),
            // Other groups span shards.
            (
                "SELECT status, count(*) FROM orders GROUP BY status",
                &not_null,
                false,
            ),
            (
                "SELECT count(DISTINCT user_id) FROM orders",
                &not_null,
                false,
            ),
            (
                "SELECT count(*) FROM orders GROUP BY user_id % 2",
                &not_null,
                false,
            ),
            // The grand total group spans shards.
            (
                "SELECT count(*) FROM orders GROUP BY ROLLUP (user_id)",
                &not_null,
                false,
            ),
            (
                "SELECT count(*) FROM orders JOIN users ON users.id = orders.user_id \
                 GROUP BY orders.user_id",
                &not_null,
                false,
            ),
            // Rows with a NULL key are on every shard, and so is their group.
            (
                "SELECT count(*) FROM orders GROUP BY user_id",
                &nullable,
                false,
            ),
            (
                "SELECT count(*) FROM orders GROUP BY user_id",
                &mixed,
                false,
            ),
            (
                "SELECT count(*) FROM archive.orders GROUP BY user_id",
                &mixed,
                false,
            ),
            // The schema isn't loaded.
            (
                "SELECT count(*) FROM orders GROUP BY user_id",
                &Schema::default(),
                false,
            ),
        ] {
            assert_eq!(
                groups_stay_on_one_shard(&select(sql), schema, &tables),
                expected,
                "{sql}"
            );
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
            // `user_id` is now the name of the table's first column.
            "SELECT user_id FROM orders AS o(user_id)",
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
