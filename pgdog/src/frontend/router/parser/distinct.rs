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
                // FIXME: We should return an error to the client name if they
                // sent a form we don't support and the query is routed
                // cross-shard
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
    fn test_distinct_on_ordinals_start_at_one() {
        assert_eq!(
            distinct("SELECT DISTINCT ON (1, value) id, value FROM sharded"),
            Some(DistinctBy::Columns(vec![
                DistinctColumn::Index(0),
                DistinctColumn::Name("value".into()),
            ]))
        );
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

    #[test]
    fn test_distinct_row_and_none() {
        assert_eq!(
            distinct("SELECT DISTINCT id FROM sharded"),
            Some(DistinctBy::Row)
        );
        assert_eq!(distinct("SELECT id FROM sharded"), None);
    }
}
