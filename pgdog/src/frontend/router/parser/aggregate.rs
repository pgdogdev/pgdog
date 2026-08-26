use pg_raw_parse::{Node, nodes};
use std::fmt;

use super::Function;
use crate::backend::schema::Schema;

#[derive(Debug, Clone, PartialEq)]
pub struct AggregateTarget {
    column: usize,
    function: AggregateFunction,
    distinct: bool,
}

impl AggregateTarget {
    pub fn function(&self) -> &AggregateFunction {
        &self.function
    }

    pub fn column(&self) -> usize {
        self.column
    }

    pub fn is_distinct(&self) -> bool {
        self.distinct
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFunction {
    Count,
    Max,
    Min,
    Avg,
    Sum,
    StddevPop,
    StddevSamp,
    VarPop,
    VarSamp,
    Unrecognized(String),
}

impl fmt::Display for AggregateFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            AggregateFunction::Count => write!(f, "count"),
            AggregateFunction::Max => write!(f, "max"),
            AggregateFunction::Min => write!(f, "min"),
            AggregateFunction::Avg => write!(f, "avg"),
            AggregateFunction::Sum => write!(f, "sum"),
            AggregateFunction::StddevPop => write!(f, "stddev_pop"),
            AggregateFunction::StddevSamp => write!(f, "stddev_samp"),
            AggregateFunction::VarPop => write!(f, "var_pop"),
            AggregateFunction::VarSamp => write!(f, "var_samp"),
            AggregateFunction::Unrecognized(s) => f.write_str(s),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Aggregate {
    targets: Vec<AggregateTarget>,
    group_by: Vec<usize>,
}

fn index_of_column(stmt: &nodes::SelectStmt, qualified_column_name: &[&str]) -> Option<usize> {
    stmt.target_list().iter().position(|node| {
        let Node::ColumnRef(c) = node.val() else {
            return false;
        };
        let selected_column = c
            .fields()
            .iter()
            .filter_map(Node::as_str)
            .collect::<Vec<_>>();
        columns_match(&selected_column, qualified_column_name)
    })
}

fn columns_match(group_by_names: &[&str], select_names: &[&str]) -> bool {
    if group_by_names == select_names {
        return true;
    }

    if group_by_names.len() == 1 && select_names.len() == 2 {
        return select_names[1] == group_by_names[0];
    }

    if group_by_names.len() == 2 && select_names.len() == 1 {
        return group_by_names[1] == select_names[0];
    }

    false
}

impl Aggregate {
    /// Figure out what aggregates are present and which ones PgDog supports.
    pub(crate) fn parse(stmt: &nodes::SelectStmt, schema: &Schema) -> Self {
        let group_by = stmt
            .group_clause()
            .iter()
            .filter_map(|node| match node {
                // We use 0-indexed arrays, Postgres uses 1-indexed.
                Node::A_Const(c) => c
                    .val()
                    .and_then(|v| v.numeric_value::<i32>().map(|x| x as usize - 1)),
                Node::ColumnRef(c) => index_of_column(
                    stmt,
                    &c.fields()
                        .iter()
                        .filter_map(Node::as_str)
                        .collect::<Vec<_>>(),
                ),
                _ => None, // FIXME: We should error instead of silently skipping
            })
            .collect();

        let targets = stmt
            .target_list()
            .iter()
            .enumerate()
            .filter_map(|(idx, node)| {
                let func = Function::try_from(node.val()).ok()?;
                let function = match func.name {
                    "count" => AggregateFunction::Count,
                    "max" => AggregateFunction::Max,
                    "min" => AggregateFunction::Min,
                    "sum" => AggregateFunction::Sum,
                    "avg" => AggregateFunction::Avg,
                    "stddev" | "stddev_samp" => AggregateFunction::StddevSamp,
                    "stddev_pop" => AggregateFunction::StddevPop,
                    "variance" | "var_samp" => AggregateFunction::VarSamp,
                    "var_pop" => AggregateFunction::VarPop,
                    fname => {
                        if schema.aggregate_functions.contains(fname) {
                            AggregateFunction::Unrecognized(fname.to_owned())
                        } else {
                            return None;
                        }
                    }
                };

                // FIXME: This doesn't correctly handle distinct behind type
                // casts any other nodes that Function::try_from handles
                let distinct = if let Node::FuncCall(func) = node.val() {
                    func.agg_distinct
                } else {
                    false
                };

                Some(AggregateTarget {
                    column: idx,
                    function,
                    distinct,
                })
            })
            .collect();
        Self { group_by, targets }
    }

    pub fn targets(&self) -> &[AggregateTarget] {
        &self.targets
    }

    pub fn group_by(&self) -> &[usize] {
        &self.group_by
    }

    #[cfg(test)]
    pub fn new_count(column: usize) -> Self {
        Self {
            targets: vec![AggregateTarget {
                function: AggregateFunction::Count,
                column,
                distinct: false,
            }],
            group_by: vec![],
        }
    }

    #[cfg(test)]
    pub fn new_count_group_by(column: usize, group_by: &[usize]) -> Self {
        Self {
            targets: vec![AggregateTarget {
                function: AggregateFunction::Count,
                column,
                distinct: false,
            }],
            group_by: group_by.to_vec(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.targets.len()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use pg_raw_parse::{Owned, make};

    fn select(stmt: &str) -> Owned<nodes::SelectStmt> {
        match pg_raw_parse::parse(stmt).unwrap().stmts().next().unwrap() {
            Node::SelectStmt(stmt) => make::owned(|mem| mem.make_unique(stmt)),
            _ => panic!("not a select"),
        }
    }

    fn parse(stmt: &str) -> Aggregate {
        Aggregate::parse(&select(stmt), &Default::default())
    }

    #[test]
    fn test_parse_aggregate() {
        let aggr = parse("SELECT COUNT(*)::bigint FROM users");
        assert_eq!(
            aggr.targets().first().unwrap().function,
            AggregateFunction::Count
        );
    }

    #[test]
    fn test_parse_distinct_and_not_distinct() {
        let aggr = parse("SELECT COUNT(DISTINCT price), AVG(price) FROM menu");
        assert_eq!(aggr.targets().len(), 2);
        let count = &aggr.targets()[0];
        let avg = &aggr.targets()[1];
        assert!(matches!(count.function(), AggregateFunction::Count));
        assert!(matches!(avg.function(), AggregateFunction::Avg));
        assert!(count.is_distinct());
        assert!(!avg.is_distinct());
    }

    #[test]
    fn test_parse_stddev_variants() {
        let aggr = parse("SELECT STDDEV(price), STDDEV_SAMP(price), STDDEV_POP(price) FROM menu");
        assert_eq!(aggr.targets().len(), 3);
        assert!(matches!(
            aggr.targets()[0].function(),
            AggregateFunction::StddevSamp
        ));
        assert!(matches!(
            aggr.targets()[1].function(),
            AggregateFunction::StddevSamp
        ));
        assert!(matches!(
            aggr.targets()[2].function(),
            AggregateFunction::StddevPop
        ));
    }

    #[test]
    fn test_parse_variance_variants() {
        let aggr = parse("SELECT VARIANCE(price), VAR_SAMP(price), VAR_POP(price) FROM menu");
        assert_eq!(aggr.targets().len(), 3);
        assert!(matches!(
            aggr.targets()[0].function(),
            AggregateFunction::VarSamp
        ));
        assert!(matches!(
            aggr.targets()[1].function(),
            AggregateFunction::VarSamp
        ));
        assert!(matches!(
            aggr.targets()[2].function(),
            AggregateFunction::VarPop
        ));
    }

    #[test]
    fn test_parse_group_by_ordinals() {
        let aggr = parse("SELECT price, category_id, SUM(quantity) FROM menu GROUP BY 1, 2");
        assert_eq!(aggr.group_by(), &[0, 1]);
        assert_eq!(aggr.targets().len(), 1);
        let target = &aggr.targets()[0];
        assert!(matches!(target.function(), AggregateFunction::Sum));
        assert_eq!(target.column(), 2);
    }

    #[test]
    fn test_parse_sum_distinct_sets_flag() {
        let aggr = parse("SELECT SUM(DISTINCT price) FROM menu");
        assert_eq!(aggr.targets().len(), 1);
        let target = &aggr.targets()[0];
        assert!(matches!(target.function(), AggregateFunction::Sum));
        assert!(target.is_distinct());
    }

    #[test]
    fn test_parse_group_by_column_name_single() {
        let aggr = parse("SELECT user_id, COUNT(1) FROM example GROUP BY user_id");
        assert_eq!(aggr.group_by(), &[0]);
        assert_eq!(aggr.targets().len(), 1);
        let target = &aggr.targets()[0];
        assert!(matches!(target.function(), AggregateFunction::Count));
        assert_eq!(target.column(), 1);
    }

    #[test]
    fn test_parse_group_by_column_name_multiple() {
        let aggr = parse(
            "SELECT COUNT(*), user_id, category_id FROM example GROUP BY user_id, category_id",
        );
        assert_eq!(aggr.group_by(), &[1, 2]);
        assert_eq!(aggr.targets().len(), 1);
        let target = &aggr.targets()[0];
        assert!(matches!(target.function(), AggregateFunction::Count));
        assert_eq!(target.column(), 0);
    }

    #[test]
    fn test_parse_group_by_qualified_column_name() {
        let aggr = parse("SELECT COUNT(1), example.user_id FROM example GROUP BY example.user_id");
        assert_eq!(aggr.group_by(), &[1]);
        assert_eq!(aggr.targets().len(), 1);
        let target = &aggr.targets()[0];
        assert!(matches!(target.function(), AggregateFunction::Count));
        assert_eq!(target.column(), 0);
    }

    #[test]
    fn test_parse_group_by_mixed_ordinal_and_column_name() {
        let aggr =
            parse("SELECT user_id, category_id, SUM(quantity) FROM example GROUP BY user_id, 2");
        assert_eq!(aggr.group_by(), &[0, 1]);
        assert_eq!(aggr.targets().len(), 1);
        let target = &aggr.targets()[0];
        assert!(matches!(target.function(), AggregateFunction::Sum));
        assert_eq!(target.column(), 2);
    }

    #[test]
    fn test_parse_group_by_column_not_in_select() {
        let aggr = parse("SELECT COUNT(*) FROM example GROUP BY user_id");
        assert!(aggr.group_by().is_empty());
        assert_eq!(aggr.targets().len(), 1);
    }

    #[test]
    fn test_parse_group_by_with_multiple_aggregates() {
        let aggr =
            parse("SELECT COUNT(*), SUM(price), user_id, AVG(price) FROM example GROUP BY user_id");
        assert_eq!(aggr.group_by(), &[2]);
        assert_eq!(aggr.targets().len(), 3);
        assert!(matches!(
            aggr.targets()[0].function(),
            AggregateFunction::Count
        ));
        assert!(matches!(
            aggr.targets()[1].function(),
            AggregateFunction::Sum
        ));
        assert!(matches!(
            aggr.targets()[2].function(),
            AggregateFunction::Avg
        ));
    }

    #[test]
    fn test_parse_group_by_qualified_matches_select_unqualified() {
        let aggr = parse("SELECT user_id, COUNT(1) FROM example GROUP BY example.user_id");
        assert_eq!(aggr.group_by(), &[0]);
        assert_eq!(aggr.targets().len(), 1);
    }

    #[test]
    fn test_parse_group_by_unqualified_matches_select_qualified() {
        let aggr = parse("SELECT example.user_id, COUNT(1) FROM example GROUP BY user_id");
        assert_eq!(aggr.group_by(), &[0]);
        assert_eq!(aggr.targets().len(), 1);
    }

    #[test]
    fn test_parse_group_by_both_qualified_order_matters() {
        let aggr = parse("SELECT example.user_id, COUNT(1) FROM example GROUP BY example.user_id");
        assert_eq!(aggr.group_by(), &[0]);
        assert_eq!(aggr.targets().len(), 1);
    }

    #[test]
    fn test_unrecognized_aggregate_function_errors() {
        let schema_with_agg = Schema::from_parts_with_agg(
            Vec::new(),
            Default::default(),
            vec![String::from("mysum")],
        );
        let schema_without_agg = Default::default();
        let query = select("SELECT mysum(lol) FROM example");

        // A random function that isn't listed as aggregate in the schema
        // doesn't require special support on our end, so we should be fine.
        let aggregate = Aggregate::parse(&query, &schema_without_agg);
        assert_eq!(aggregate.targets, Vec::new());

        // If we see an aggregate function we don't recognize, we can't
        // process the query correctly, since we need to combine the
        // results from each shard.
        let aggregate = Aggregate::parse(&query, &schema_with_agg);
        let funcs = aggregate
            .targets
            .into_iter()
            .map(|t| t.function)
            .collect::<Vec<_>>();
        assert_eq!(
            funcs,
            vec![AggregateFunction::Unrecognized("mysum".to_owned())]
        );
    }
}
