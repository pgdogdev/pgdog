use pg_raw_parse::{Node, nodes, walk};
use std::fmt;

use pgdog_stats::Relation as StatsRelation;

use super::Function;
use crate::backend::schema::Schema;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AggregateTarget {
    column: usize,
    function: AggregateFunction,
    distinct: bool,
}

impl AggregateTarget {
    pub(crate) fn function(&self) -> &AggregateFunction {
        &self.function
    }

    pub(crate) fn column(&self) -> usize {
        self.column
    }

    pub(crate) fn is_distinct(&self) -> bool {
        self.distinct
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum AggregateFunction {
    Count,
    Max,
    Min,
    Avg,
    Sum,
    StddevPop,
    StddevSamp,
    VarPop,
    VarSamp,
    BoolAnd,
    BoolOr,
    BitAnd,
    BitOr,
    Unrecognized(String),
}

impl AggregateFunction {
    pub(crate) fn from_builtin_name(name: &str) -> Option<Self> {
        Some(match name {
            "count" => Self::Count,
            "max" => Self::Max,
            "min" => Self::Min,
            "sum" => Self::Sum,
            "avg" => Self::Avg,
            "stddev" | "stddev_samp" => Self::StddevSamp,
            "stddev_pop" => Self::StddevPop,
            "variance" | "var_samp" => Self::VarSamp,
            "var_pop" => Self::VarPop,
            "bool_and" | "every" => Self::BoolAnd,
            "bool_or" => Self::BoolOr,
            "bit_and" => Self::BitAnd,
            "bit_or" => Self::BitOr,
            _ => return None,
        })
    }
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
            AggregateFunction::BoolAnd => write!(f, "bool_and"),
            AggregateFunction::BoolOr => write!(f, "bool_or"),
            AggregateFunction::BitAnd => write!(f, "bit_and"),
            AggregateFunction::BitOr => write!(f, "bit_or"),
            AggregateFunction::Unrecognized(s) => f.write_str(s),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct Aggregate {
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

/// Columns every table has without listing them in its definition.
const SYSTEM_COLUMNS: [&str; 6] = ["tableoid", "xmin", "cmin", "xmax", "cmax", "ctid"];

/// What a bare `GROUP BY` name that matches a `SELECT` alias refers to.
///
/// Postgres looks for a column of the `FROM` tables first and falls back to
/// the `SELECT` alias, so the schema decides which one it is, unless the
/// `SELECT` list itself reads the column:
///
/// ```sql
/// SELECT lower(name) AS name, count(*) FROM users GROUP BY name -- users.name
/// SELECT lower(name) AS n, count(*) FROM users GROUP BY n -- lower(name)
/// ```
#[derive(Debug, Clone, Copy, PartialEq)]
enum GroupByAlias {
    /// A table in `FROM` has a column with this name.
    InputColumn,
    /// No table has one. The name is the `SELECT` list item at this position.
    Output(usize),
    /// `FROM` has something other than tables found in the schema.
    Unknown,
}

impl GroupByAlias {
    /// Returns `None` if the column isn't a bare name matching a `SELECT` alias.
    fn resolve(
        stmt: &nodes::SelectStmt,
        column: &nodes::ColumnRef,
        schema: &Schema,
    ) -> Option<Self> {
        let name = bare_name(column)?;
        let index = stmt
            .target_list()
            .iter()
            .position(|target| target.name() == Some(name))?;

        // `lower(name) AS name` only runs if a table has the column, so the
        // schema isn't needed to know it's there.
        if stmt
            .target_list()
            .iter()
            .any(|target| references_column(target.val(), name))
        {
            return Some(Self::InputColumn);
        }

        if SYSTEM_COLUMNS.contains(&name) {
            return Some(Self::Unknown);
        }

        let mut is_input_column = false;
        for from in stmt.from_clause().iter() {
            match from_has_column(from, name, schema, stmt) {
                Some(has_column) => is_input_column |= has_column,
                None => return Some(Self::Unknown),
            }
        }

        Some(if is_input_column {
            Self::InputColumn
        } else {
            Self::Output(index)
        })
    }
}

/// The name of an unqualified column reference: `name`, not `users.name`.
fn bare_name(column: &nodes::ColumnRef) -> Option<&str> {
    let mut fields = column.fields().into_iter();
    let name = fields.next().and_then(Node::as_str)?;
    fields.next().is_none().then_some(name)
}

/// Whether a SELECT list expression reads a column by this bare name.
/// Subqueries have columns of their own, so expressions with one don't count.
fn references_column(expression: Node<'_>, name: &str) -> bool {
    let mut referenced = false;
    let mut has_subquery = false;
    walk::walk(expression, |node| match node {
        Node::SubLink(_) => has_subquery = true,
        Node::ColumnRef(column) => referenced |= bare_name(column) == Some(name),
        _ => (),
    });
    referenced && !has_subquery
}

/// Whether a `FROM` item has a column with this name. `None` if it isn't made
/// of tables found in the schema, e.g., a subquery, a function or a CTE.
fn from_has_column(
    from: Node<'_>,
    name: &str,
    schema: &Schema,
    stmt: &nodes::SelectStmt,
) -> Option<bool> {
    // `AS t(a, b)` renames the columns.
    let renames_columns =
        |alias: Option<&nodes::Alias>| alias.is_some_and(|alias| !alias.colnames().is_empty());

    match from {
        Node::RangeVar(table) => {
            let relname = table.relname()?;
            if renames_columns(table.alias()) {
                return None;
            }
            let has_column = |relation: &StatsRelation| relation.columns.contains_key(name);
            match table.schemaname() {
                Some(schemaname) => schema.get(schemaname, relname).map(has_column),
                None if is_cte(stmt, relname) => None,
                // The search path isn't known here. A column in any table
                // with this name counts, which only ever picks the hidden
                // column the shards check themselves.
                None => schema
                    .relations
                    .values()
                    .filter_map(|relations| relations.get(relname))
                    .map(has_column)
                    .reduce(|a, b| a || b),
            }
        }
        Node::JoinExpr(join) => {
            if renames_columns(join.alias()) {
                return None;
            }
            let left = from_has_column(join.larg(), name, schema, stmt)?;
            let right = from_has_column(join.rarg(), name, schema, stmt)?;
            Some(left || right)
        }
        _ => None,
    }
}

fn is_cte(stmt: &nodes::SelectStmt, name: &str) -> bool {
    stmt.with_clause()
        .is_some_and(|with| with.ctes().iter().any(|cte| cte.ctename() == Some(name)))
}

/// How a cross-shard merge finds the value of a `GROUP BY` item.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum GroupByKey {
    /// `GROUP BY 1`: the `SELECT` list item at this position.
    Ordinal(usize),
    /// A column that's also in the `SELECT` list, at this position.
    SelectedColumn(usize),
    /// A `SELECT` alias, for the item at this position.
    Alias(usize),
    /// Not in the `SELECT` list. Shards have to return it as a hidden column.
    Unselected,
    /// A name PgDog can't tell is a `SELECT` alias or a table column. Shards
    /// can't resolve a `SELECT` alias inside a hidden column.
    UnresolvedAlias,
    /// `GROUPING SETS`, `ROLLUP` or `CUBE`: a merged row can't tell which
    /// grouping set it came from.
    GroupingSet,
}

impl GroupByKey {
    pub(super) const UNRESOLVED_ALIAS: &str = "GROUP BY on a SELECT alias is only supported when FROM has just tables \
         PgDog has loaded the schema for";
    pub(super) const GROUPING_SET: &str = "GROUPING SETS, ROLLUP and CUBE are unsupported";

    /// Classify every `GROUP BY` item, in order.
    pub(super) fn parse<'a>(
        stmt: &'a nodes::SelectStmt,
        schema: &'a Schema,
    ) -> impl Iterator<Item = Self> + 'a {
        stmt.group_clause()
            .iter()
            .map(move |node| Self::new(stmt, node, schema))
    }

    pub(super) fn new(stmt: &nodes::SelectStmt, node: Node<'_>, schema: &Schema) -> Self {
        match node {
            // We use 0-indexed arrays, Postgres uses 1-indexed.
            Node::A_Const(c) => c
                .val()
                .and_then(|v| v.numeric_value::<i32>())
                .and_then(|x| usize::try_from(x).ok()?.checked_sub(1))
                .map_or(Self::Unselected, Self::Ordinal),
            Node::ColumnRef(c) => {
                let selected = index_of_column(
                    stmt,
                    &c.fields()
                        .iter()
                        .filter_map(Node::as_str)
                        .collect::<Vec<_>>(),
                );
                match selected {
                    Some(index) => Self::SelectedColumn(index),
                    None => match GroupByAlias::resolve(stmt, c, schema) {
                        Some(GroupByAlias::Output(index)) => Self::Alias(index),
                        Some(GroupByAlias::Unknown) => Self::UnresolvedAlias,
                        Some(GroupByAlias::InputColumn) | None => Self::Unselected,
                    },
                }
            }
            Node::GroupingSet(_) => Self::GroupingSet,
            _ => Self::Unselected,
        }
    }

    /// Position of the key in the `SELECT` list, if it's there.
    fn position(self) -> Option<usize> {
        match self {
            Self::Ordinal(index) | Self::SelectedColumn(index) | Self::Alias(index) => Some(index),
            Self::Unselected | Self::UnresolvedAlias | Self::GroupingSet => None,
        }
    }
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

impl AggregateFunction {
    pub(crate) fn from_name(name: &str, schema: &Schema) -> Option<Self> {
        Self::from_builtin_name(name).or_else(|| {
            schema
                .aggregate_functions
                .contains(name)
                .then(|| Self::Unrecognized(name.to_owned()))
        })
    }
}

impl Aggregate {
    /// Figure out what aggregates are present and which ones PgDog supports.
    pub(crate) fn parse(stmt: &nodes::SelectStmt, schema: &Schema) -> Self {
        Self::with_keys(stmt, schema, GroupByKey::parse(stmt, schema))
    }

    /// Same as [`Self::parse`], for `GROUP BY` keys that are already classified.
    pub(super) fn with_keys(
        stmt: &nodes::SelectStmt,
        schema: &Schema,
        keys: impl IntoIterator<Item = GroupByKey>,
    ) -> Self {
        // Cross-shard queries add the skipped keys as hidden columns.
        let group_by = keys.into_iter().filter_map(GroupByKey::position).collect();

        let targets = stmt
            .target_list()
            .iter()
            .enumerate()
            .filter_map(|(idx, node)| {
                let func = Function::try_from(node.val()).ok()?;
                let function = AggregateFunction::from_name(func.name, schema)?;

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

    pub(crate) fn with_group_by(mut self, group_by: Vec<usize>) -> Self {
        self.group_by = group_by;
        self
    }

    pub(crate) fn targets(&self) -> &[AggregateTarget] {
        &self.targets
    }

    pub(crate) fn group_by(&self) -> &[usize] {
        &self.group_by
    }

    #[cfg(test)]
    pub(crate) fn new_count(column: usize) -> Self {
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
    pub(crate) fn new_count_group_by(column: usize, group_by: &[usize]) -> Self {
        Self {
            targets: vec![AggregateTarget {
                function: AggregateFunction::Count,
                column,
                distinct: false,
            }],
            group_by: group_by.to_vec(),
        }
    }

    /// Nothing to merge across shards. GROUP BY without aggregate
    /// functions still has to merge the groups each shard returns.
    pub(crate) fn is_empty(&self) -> bool {
        self.targets.is_empty() && self.group_by.is_empty()
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
    fn test_parse_bool_and_bit_aggregates() {
        let aggr = parse(
            "SELECT BOOL_AND(open), EVERY(open), BOOL_OR(open), BIT_AND(flags), BIT_OR(flags) FROM menu",
        );
        let functions = aggr
            .targets()
            .iter()
            .map(|target| target.function().clone())
            .collect::<Vec<_>>();
        assert_eq!(
            functions,
            vec![
                AggregateFunction::BoolAnd,
                AggregateFunction::BoolAnd,
                AggregateFunction::BoolOr,
                AggregateFunction::BitAnd,
                AggregateFunction::BitOr,
            ]
        );
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
    fn test_group_by_without_aggregate_is_not_empty() {
        let aggr = parse("SELECT category FROM menu GROUP BY category");
        assert!(aggr.targets().is_empty());
        assert_eq!(aggr.group_by(), &[0]);
        assert!(!aggr.is_empty());

        assert!(parse("SELECT category FROM menu").is_empty());
    }

    #[test]
    fn test_parse_group_by_column_not_in_select() {
        let aggr = parse("SELECT COUNT(*) FROM example GROUP BY user_id");
        assert!(aggr.group_by().is_empty());
        assert_eq!(aggr.targets().len(), 1);
    }

    #[test]
    fn test_parse_group_by_invalid_ordinal_is_skipped() {
        for sql in [
            "SELECT user_id, COUNT(*) FROM example GROUP BY 0",
            "SELECT user_id, COUNT(*) FROM example GROUP BY -1",
        ] {
            let aggr = parse(sql);
            assert!(aggr.group_by().is_empty(), "{sql}");
            assert_eq!(aggr.targets().len(), 1, "{sql}");
        }
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
