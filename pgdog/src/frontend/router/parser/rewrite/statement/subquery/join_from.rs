use std::ops::ControlFlow;

use itertools::Itertools;
use pg_raw_parse::{
    Node,
    nodes::{JoinExpr, JoinType, RangeVar},
    walk::{self, Recurse},
};

use crate::frontend::router::parser::{
    StatementParser, Table,
    rewrite::statement::{
        insert::{JoinFetch, Subquery},
        subquery::SubqueryRewrite,
    },
};

/// The table after JOIN, e.g., orders y in SELECT .. FROM users x JOIN orders y ON x.status = y.status
/// We call orders y the right side of the JOIN and users x the first table
struct JoinRightSide<'a> {
    table: &'a RangeVar,
    /// The alias of the table (y in the example above), or the name of the table if the table doesn't have an alias (orders in .. JOIN orders ON orders.status = ..)
    name: &'a str,
}

impl<'a, 'mem> SubqueryRewrite<'a, 'mem> {
    pub(crate) fn handle_subquery_in_join_from(&self) -> Option<Subquery> {
        let (first_table, join, right_side) = parse_join(self.outer_stmt)?;

        // If we have JOINs where one of the two tables isn't sharded, all rows of that table are on all shards, so we send those JOINs as-is
        //
        // TODO(jkaczman): LEFT JOINs from tables that aren't sharded to sharded tables return extra rows
        // E.g., for SELECT s.state, u.id FROM states s LEFT JOIN users u ON u.state = s.state, if the only user in CA is user 3 on Shard B:
        // - Shard A: the state CA is on Shard A, but user 3 isn't, so Shard A doesn't find a user for CA and returns (CA, NULL)
        // - Shard B: the state CA and user 3 are on Shard B, so Shard B returns (CA, 3)
        // The client receives both rows, but Postgres only returns (CA, 3)
        // We should fetch users from all shards first and then send the statement to one shard, since all shards would then return the same rows
        if !self.is_table_sharded(first_table) || !self.is_table_sharded(right_side.table) {
            return None;
        }

        let parser = StatementParser::new(self.outer_stmt, None, self.statement_rewrite.schema);
        if parser.is_joined_on_sharding_keys(join, right_side.name) {
            return None;
        }

        Some(Subquery::JoinFrom(JoinFetch {
            location: right_side.table.location,
            name: right_side.name.to_string(),
        }))
    }

    fn is_table_sharded(&self, table: &RangeVar) -> bool {
        self.statement_rewrite.schema.tables.is_table_sharded(
            Table::from(table),
            self.statement_rewrite.db_schema,
            self.statement_rewrite.user,
            self.statement_rewrite.search_path,
        )
    }
}

/// Returns the first table, the JOIN and the right side of the JOIN, if `outer_stmt` is SELECT .. FROM <first table> [LEFT | CROSS | NATURAL] JOIN <right side> .. (without WITH or FOR UPDATE)
/// E.g., for SELECT .. FROM users x JOIN orders y ON x.status = y.status, we return users x, the JOIN and orders y
/// Returns None for all other statements, e.g., SELECT .. FROM users x RIGHT JOIN orders y ON ..
fn parse_join<'a>(outer_stmt: Node<'a>) -> Option<(&'a RangeVar, &'a JoinExpr, JoinRightSide<'a>)> {
    let Node::SelectStmt(select) = outer_stmt else {
        return None;
    };

    if !select.locking_clause().is_empty() {
        return None;
    }

    if select.with_clause().is_some() {
        return None;
    }

    let Ok(Node::JoinExpr(join)) = select.from_clause().into_iter().exactly_one() else {
        return None;
    };

    if !matches!(join.jointype, JoinType::JOIN_INNER | JoinType::JOIN_LEFT) {
        return None;
    }

    let Node::RangeVar(first_table) = join.larg() else {
        return None;
    };

    Some((first_table, join, parse_right_side(join)?))
}

/// Returns the right side of `join` if the right side is a table, e.g., orders y in .. JOIN orders y ON ..
/// Returns None for all other right sides, e.g., .. JOIN (SELECT ..) y ON ..
/// We also return None for tables whose alias has column names (e.g., .. JOIN orders y(a, b) ON ..), since we don't put those column names in the alias of the fetched rows yet
fn parse_right_side<'a>(join: &'a JoinExpr) -> Option<JoinRightSide<'a>> {
    match join.rarg() {
        Node::RangeVar(table) => match table.alias() {
            Some(alias) if alias.colnames().is_empty() => Some(JoinRightSide {
                table,
                name: alias.aliasname()?,
            }),

            // If the table doesn't have an alias, column references use the name of the table (e.g., orders.id), so we use the name of the table as the alias of the fetched rows
            // We skip tables with a schema (e.g., public.orders), since public.orders.id couldn't refer to the fetched rows (aliases can't have a schema)
            None if table.schemaname().is_none() => Some(JoinRightSide {
                table,
                name: table.relname()?,
            }),

            _ => None,
        },

        _ => None,
    }
}

/// Returns the table that starts at `location` in the SQL of `stmt`, e.g., orders y for the location 27 in SELECT * FROM users x JOIN orders y ON ..
pub(crate) fn table_at_location(stmt: Node<'_>, location: i32) -> Option<&RangeVar> {
    walk::walk_manual(stmt, |node| match node {
        Node::RangeVar(table) if table.location == location => ControlFlow::Break(table),
        _ => Recurse::yes(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Returns true if `parse_join` accepts `sql`
    /// E.g., we return true for SELECT * FROM a x JOIN b y ON x.c = y.c and false for SELECT * FROM a x RIGHT JOIN b y ON x.c = y.c
    ///
    /// `parse_join` doesn't read the sharding config, so a and b don't have to be sharded tables in these tests
    fn is_rewritten(sql: &str) -> bool {
        let raw = pg_raw_parse::parse(sql).unwrap();
        parse_join(raw.stmts().next().unwrap()).is_some()
    }

    // This tests that `parse_join` accepts SELECT * FROM a x JOIN b y ON x.c = y.c
    // If a and b are sharded tables and c isn't the sharding key of both tables, we fetch y for this JOIN
    // Fetching y means that we read all rows of b from all shards first and then send the statement to all shards with those rows in place of b y
    // The tests below change parts of this statement (e.g., RIGHT JOIN instead of JOIN)
    #[test]
    fn test_join_is_rewritten() {
        assert!(is_rewritten("SELECT * FROM a x JOIN b y ON x.c = y.c"));
    }

    // This tests that `parse_join` rejects the JOINs that we can't rewrite by fetching y
    // We send these JOINs to the shards as-is
    #[test]
    fn test_joins_that_we_cant_rewrite() {
        // RIGHT JOIN also returns the rows of y that don't match any rows of x
        // If we fetched y, Shard A would join all rows of y with only the rows of x on Shard A
        // E.g., if the row of y with c = 5 only matches a row of x on Shard B, Shard A wouldn't find a row of x for c = 5 and would return (NULL, 5) for x.c and y.c
        // Postgres doesn't return (NULL, 5), since the row of y with c = 5 matches a row of x
        assert!(!is_rewritten(
            "SELECT * FROM a x RIGHT JOIN b y ON x.c = y.c"
        ));

        // FULL JOIN also returns the rows of y that don't match any rows of x
        // So if we fetched y, Shard A would return the same extra row (NULL, 5) as for RIGHT JOIN above
        assert!(!is_rewritten(
            "SELECT * FROM a x FULL JOIN b y ON x.c = y.c"
        ));

        // FOR UPDATE locks the rows of a and b that the statement reads
        // If we fetched y, the rewritten statement would read the rows of b from unnest(..) arrays instead of the table b, so Postgres wouldn't lock the rows of b
        assert!(!is_rewritten(
            "SELECT * FROM a x JOIN b y ON x.c = y.c FOR UPDATE"
        ));

        // LATERAL (SELECT c FROM b WHERE b.c = x.c) y runs the subquery once for each row of x and uses the x.c of that row
        // E.g., for the row of x with c = 5, y is SELECT c FROM b WHERE b.c = 5
        // For the row of x with c = 6, y is SELECT c FROM b WHERE b.c = 6
        // So y isn't one set of rows that we could fetch from all shards before we run the statement
        assert!(!is_rewritten(
            "SELECT * FROM a x JOIN LATERAL (SELECT c FROM b WHERE b.c = x.c) y ON true"
        ));
    }

    // This tests that `parse_join` rejects the JOINs that we could rewrite by fetching y, but don't rewrite yet
    //
    // Why aren't these JOINs in the subquery suite (tests/select.sql)?
    // Because the suite compares the rows that the client receives with the rows that Postgres returns
    // We send these JOINs to the shards as-is, so the client can receive different rows than Postgres returns
    #[test]
    fn test_joins_that_we_dont_rewrite_yet() {
        // Aliases can't have a schema, so we'd use b as the alias of the fetched rows
        // public.b.c in ON would then still refer to the table public.b, which isn't in the FROM of the rewritten statement
        assert!(!is_rewritten(
            "SELECT * FROM a x JOIN public.b ON x.c = public.b.c"
        ));

        // y(c, d) renames the first two columns of b to c and d
        // We don't put those column names in the alias of the fetched rows yet
        assert!(!is_rewritten(
            "SELECT * FROM a x JOIN b y(c, d) ON x.c = y.c"
        ));

        // We fetch y with SELECT * FROM b, so y has to be a table
        // For the derived table (SELECT c FROM b) y, we'd have to send the SELECT of the derived table instead
        assert!(!is_rewritten(
            "SELECT * FROM a x JOIN (SELECT c FROM b) y ON x.c = y.c"
        ));

        // We only fetch y if x is a sharded table
        // The sharding config only lists tables, so we can't tell whether the derived table (SELECT c FROM a) x is sharded
        assert!(!is_rewritten(
            "SELECT * FROM (SELECT c FROM a) x JOIN b y ON x.c = y.c"
        ));

        // For JOINs between three tables, we'd have to pick which tables stay on the shards and which tables we fetch
        assert!(!is_rewritten(
            "SELECT * FROM a x JOIN b y ON x.c = y.c JOIN d z ON z.c = y.c"
        ));

        // FROM a x, b y joins the tables with a comma and a condition in WHERE, which we don't rewrite yet (only JOIN .. ON ..)
        assert!(!is_rewritten("SELECT * FROM a x, b y WHERE x.c = y.c"));

        // If we have WITH, names in FROM can refer to CTEs (here, b refers to the CTE b)
        // We don't tell CTEs apart from tables here yet, so we'd fetch the table b instead of using the CTE b
        assert!(!is_rewritten(
            "WITH b AS (SELECT 1 AS c) SELECT * FROM a x JOIN b y ON x.c = y.c"
        ));

        // We don't rewrite UPDATE .. FROM yet (only SELECTs)
        assert!(!is_rewritten(
            "UPDATE a SET c = y.c FROM b y WHERE a.c = y.c"
        ));
    }
}
