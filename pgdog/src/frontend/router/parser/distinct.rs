use pg_raw_parse::{Node, nodes};

#[derive(Debug, PartialEq, Clone)]
pub(crate) enum DistinctColumn {
    Name(String),
    Index(usize),
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) enum DistinctBy {
    Row,
    Columns(Vec<DistinctColumn>),
}

#[derive(Debug, Clone)]
pub(crate) struct Distinct<'a> {
    stmt: &'a nodes::SelectStmt,
}

impl<'a> Distinct<'a> {
    pub(crate) fn new(stmt: &'a nodes::SelectStmt) -> Self {
        Self { stmt }
    }

    pub(crate) fn distinct(&self) -> Option<DistinctBy> {
        match self.stmt.distinct_clause().first() {
            Some(Node::None) => return Some(DistinctBy::Row),
            None => return None,
            _ => (),
        }

        let columns = self
            .stmt
            .distinct_clause()
            .iter()
            .filter_map(|node| match node {
                // Postgres counts SELECT list positions from 1 and rejects
                // anything lower itself, so there is no column to record.
                Node::A_Const(c) => Some(DistinctColumn::Index(
                    usize::try_from(c.val()?.numeric_value::<i32>()?)
                        .ok()?
                        .checked_sub(1)?,
                )),
                // org_id -> [*org_id*]
                // t.org_id -> [t, *org_id*]
                // public.t.org_id -> [public, t, *org_id*]
                Node::ColumnRef(c) => Some(DistinctColumn::Name(
                    c.fields().into_iter().next_back()?.as_str()?.to_owned(),
                )),
                // Expressions have no name. Cross-shard, the projection
                // rewrite gives every key a column of its own.
                _ => None,
            })
            .collect();

        Some(DistinctBy::Columns(columns))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn distinct(sql: &str) -> Option<DistinctBy> {
        let ast = pg_raw_parse::parse(sql).expect("valid SQL");
        let Some(Node::SelectStmt(stmt)) = ast.stmts().next() else {
            panic!("not a select");
        };
        Distinct::new(stmt).distinct()
    }

    #[test]
    fn test_distinct_on_ordinal_below_one_is_left_to_postgres() {
        // Postgres rejects both: "DISTINCT ON position 0 is not in select list".
        for sql in [
            "SELECT DISTINCT ON (0) id FROM sharded",
            "SELECT DISTINCT ON (-1) id FROM sharded",
        ] {
            assert_eq!(distinct(sql), Some(DistinctBy::Columns(vec![])), "{sql}");
        }
    }
}
