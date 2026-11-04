//! Route-dependent rewrite for cross-shard `HAVING`.

use std::collections::HashSet;

use itertools::Itertools;
use pg_raw_parse::{
    ConstValue, Node, NodeMut, make, nodes,
    transform::{self, Assignable},
    walk,
};

use crate::backend::schema::Schema;
use crate::frontend::router::parser::{AggregateFunction, HavingRewritePlan};

use super::{
    Error, offset,
    projection::{column_name, is_star, target_output_name},
};

const ALIAS_PREFIX: &str = "__pgdog_having_";
const ROW_COLUMN: &str = "__pgdog_row";
const TABLE_ALIAS: &str = "__pgdog_having";

/// Project every `HAVING` operand and disable shard-local filtering.
///
/// Aggregate calls and column references in `HAVING` become hidden columns.
/// The returned plan holds a query that evaluates the original predicate over
/// the merged values, so PostgreSQL applies its own semantics to the result.
///
/// ```sql
/// SELECT customer_id, count(*) FROM purchases
/// GROUP BY customer_id HAVING count(*) > 2 AND max(price) < 100
/// ```
///
/// is sent to every shard as
///
/// ```sql
/// SELECT customer_id, count(*),
///        count(*) AS __pgdog_having_0, max(price) AS __pgdog_having_1,
///        customer_id AS __pgdog_having_2
/// FROM purchases
/// GROUP BY customer_id HAVING (count(*) > 2 AND max(price) < 100) OR true
/// ```
///
/// and the merged groups are filtered with
///
/// ```sql
/// SELECT __pgdog_row FROM unnest($1, $2) WITH ORDINALITY
///     __pgdog_having(__pgdog_having_0, __pgdog_having_1, __pgdog_row)
/// WHERE __pgdog_having_0 > 2 AND __pgdog_having_1 < 100
/// ```
///
/// which returns the positions of the groups to keep. LIMIT and OFFSET move
/// from the shard query to the filter query, after `ORDER BY __pgdog_row`.
pub(super) fn rewrite_select<'a>(
    select: &mut nodes::SelectStmtMut<'a, '_>,
    mem: make::MemoryToken<'a>,
    schema: &Schema,
) -> Result<HavingRewritePlan, Error> {
    debug_assert!(
        !matches!(select.having_clause(), Node::None),
        "HAVING rewrite requires a HAVING clause"
    );
    let original = mem.make_unique(select.having_clause());
    let having = original.as_ref();

    let mut operands = Operands {
        schema,
        aliases: select
            .target_list()
            .iter()
            .filter_map(|target| target_output_name(target).map(str::to_owned))
            .collect(),
        projected: Vec::new(),
        params: Vec::new(),
        next_alias: 0,
        error: None,
    };

    let paginated =
        !matches!(select.limit_count(), Node::None) || !matches!(select.limit_offset(), Node::None);

    let filter = make::try_owned(|filter_mem| {
        let mut filter = filter_mem.make_unique(having);
        transform::transform(&mut filter, |node| operands.replace(mem, filter_mem, node));
        if let Some(error) = operands.error.take() {
            return Err(error);
        }
        if operands.projected.is_empty() {
            return Err(Error::UnsupportedHaving(
                "HAVING must reference an aggregate or a grouped column",
            ));
        }
        let mut limit = filter_mem.make_unique(select.limit_count());
        let mut limit_offset = filter_mem.make_unique(select.limit_offset());
        operands.record_params(limit.as_ref());
        operands.record_params(limit_offset.as_ref());

        // Client parameters follow the operand arrays in the filter query.
        let offset = operands.projected.len();
        for node in [&mut filter, &mut limit, &mut limit_offset] {
            renumber_params(node, filter_mem, offset, &operands.params);
        }

        // Ordering by position keeps the order PgDog sorted the groups in.
        let template = pg_raw_parse::parse(&format!(
            "SELECT {ROW_COLUMN} FROM unnest({}) WITH ORDINALITY AS {TABLE_ALIAS}({}, {ROW_COLUMN}){}",
            (1..=offset).map(|number| format!("${number}")).join(", "),
            operands.projected.iter().map(|(alias, _)| alias).join(", "),
            if paginated {
                format!(" ORDER BY {ROW_COLUMN}")
            } else {
                String::new()
            },
        ))?;
        let Some(Node::SelectStmt(template)) = template.stmts().next() else {
            unreachable!("HAVING filter template is a SELECT");
        };
        let mut query = filter_mem.make_unique(template);
        query.as_mut().set_where_clause(filter);
        query.as_mut().set_limit_count(limit);
        query.as_mut().set_limit_offset(limit_offset);
        query.as_mut().set_limit_option(select.limit_option);
        Ok::<_, Error>(query)
    })?;
    let filter_sql = pg_raw_parse::deparse(&*filter)?.as_str().to_owned();
    let operand_aliases = operands
        .projected
        .iter()
        .map(|(alias, _)| alias.clone())
        .collect::<Vec<_>>();

    let group_sources = select
        .group_clause()
        .iter()
        .map(|group| mem.make_unique(group))
        .collect::<Vec<_>>();
    for group_source in group_sources {
        match group_source.as_ref() {
            // Shards can't resolve a SELECT alias inside the hidden projection.
            Node::ColumnRef(column) if groups_by_output_alias(select, column) => {
                return Err(Error::UnsupportedHaving(
                    "GROUP BY on a SELECT alias is unsupported",
                ));
            }
            Node::ColumnRef(_) => {
                let alias = operands.next_alias();
                operands.projected.push((alias, group_source));
            }
            Node::A_Const(_) => {}
            _ => {
                return Err(Error::UnsupportedHaving(
                    "GROUP BY expressions other than columns and ordinals are unsupported",
                ));
            }
        }
    }

    let hidden_aliases = operands
        .projected
        .iter()
        .map(|(alias, _)| alias.clone())
        .collect::<Vec<_>>();
    let targets = operands
        .projected
        .into_iter()
        .map(|(alias, node)| mem.make_res_target(Some(&alias), mem.empty(), node))
        .collect::<Vec<_>>();
    select
        .target_list_mut()
        .extend(mem, mem.make_list(&targets));

    // Keep the predicate under `OR true` instead of removing it, so the shard
    // statement still references every `$n` and Postgres infers the same
    // parameter types. Postgres folds `x OR true` away, so shards don't evaluate it.
    let true_value = mem.make_a_const(ConstValue::Boolean(true)).uncast();
    let mut neutralized = mem.make_node::<nodes::BoolExpr>();
    neutralized
        .as_mut()
        .set_boolop(nodes::BoolExprType::OR_EXPR);
    neutralized
        .as_mut()
        .set_args(mem.make_list(&[original, true_value]));
    select.set_having_clause(neutralized.uncast());

    // Shards return every group; the filter query applies LIMIT and OFFSET.
    let limit = without_limit(select.limit_count(), mem);
    let offset = without_limit(select.limit_offset(), mem);
    select.set_limit_count(limit);
    select.set_limit_offset(offset);

    Ok(HavingRewritePlan::new(
        filter_sql,
        operand_aliases,
        hidden_aliases,
        operands.params,
    ))
}

/// Point each `$n` at its position after the `offset` operand arrays.
fn renumber_params<'f>(
    node: &mut make::Unique<'f, Node<'_>>,
    mem: make::MemoryToken<'f>,
    offset: usize,
    params: &[usize],
) {
    if matches!(node.as_ref(), Node::None) {
        return;
    }
    transform::transform(node, |node| match &*node {
        NodeMut::ParamRef(param) => {
            let position = params
                .iter()
                .position(|&number| number + 1 == param.number as usize)
                .expect("HAVING parameter recorded");
            node.replace(mem.make_param_ref((offset + position + 1) as i32).uncast());
            None
        }
        _ => Some(node),
    });
}

/// Remove LIMIT or OFFSET from the shard query. An expression with `$n`
/// stays as `CASE WHEN false THEN (expr)::bigint END`, so Postgres infers the
/// same parameter types without evaluating it.
fn without_limit<'a>(node: Node<'_>, mem: make::MemoryToken<'a>) -> make::Unique<'a, Node<'a>> {
    let mut has_param = false;
    if !matches!(node, Node::None) {
        walk::walk(node, |node| has_param |= matches!(node, Node::ParamRef(_)));
    }
    if !has_param {
        return mem.none();
    }
    let mut when = mem.make_node::<nodes::CaseWhen>();
    when.as_mut()
        .set_expr(mem.make_a_const(ConstValue::Boolean(false)).uncast());
    when.as_mut().set_result(offset::to_bigint(node, mem));
    let mut case = mem.make_node::<nodes::CaseExpr>();
    case.as_mut().set_args(mem.make_list(&[when.uncast()]));
    case.uncast()
}

fn groups_by_output_alias(
    select: &nodes::SelectStmtMut<'_, '_>,
    column: &nodes::ColumnRef,
) -> bool {
    column.fields().len() == 1
        && select.target_list().iter().any(|target| {
            target
                .name()
                .is_some_and(|name| Some(name) == column_name(column))
        })
}

struct Operands<'a, 's> {
    schema: &'s Schema,
    aliases: HashSet<String>,
    /// Hidden columns added to the shard query.
    projected: Vec<(String, make::Unique<'a, Node<'a>>)>,
    /// 0-based client parameters in the filter, in first-use order.
    params: Vec<usize>,
    next_alias: usize,
    error: Option<Error>,
}

impl<'a> Operands<'a, '_> {
    /// Replace an aggregate call or column with a reference to its hidden column.
    fn replace<'f, 'r>(
        &mut self,
        mem: make::MemoryToken<'a>,
        filter_mem: make::MemoryToken<'f>,
        node: Assignable<'f, 'r>,
    ) -> Option<Assignable<'f, 'r>> {
        if self.error.is_some() {
            return None;
        }
        match &*node {
            NodeMut::FuncCall(function) if is_aggregate(function, self.schema) => {
                if function.agg_distinct
                    || function.agg_within_group
                    || !function.agg_order().is_empty()
                    || !matches!(function.agg_filter(), Node::None)
                    || function.over().is_some()
                {
                    self.error = Some(Error::UnsupportedHaving(
                        "DISTINCT, FILTER, ORDER BY and OVER on aggregates are unsupported",
                    ));
                    return None;
                }
            }
            NodeMut::ColumnRef(column) if is_star(column) => {
                self.error = Some(Error::UnsupportedHaving("* in HAVING is unsupported"));
                return None;
            }
            NodeMut::ColumnRef(_) => {}
            NodeMut::SubLink(_) => {
                self.error = Some(Error::UnsupportedHaving("subqueries are unsupported"));
                return None;
            }
            NodeMut::ParamRef(param) => {
                self.record_param(param.number);
                return None;
            }
            _ => return Some(node),
        }

        let alias = self.next_alias();
        self.projected
            .push((alias.clone(), mem.make_unique(node.as_ref())));
        node.replace(
            filter_mem
                .make_column_ref(
                    filter_mem.make_list(&[filter_mem.make_string(Some(&alias)).uncast()]),
                )
                .uncast(),
        );
        None
    }

    /// Record the client parameters LIMIT and OFFSET pass to the filter query.
    fn record_params(&mut self, node: Node<'_>) {
        if matches!(node, Node::None) {
            return;
        }
        walk::walk(node, |node| {
            if let Node::ParamRef(param) = node {
                self.record_param(param.number);
            }
        });
    }

    fn record_param(&mut self, number: i32) {
        let param = number as usize - 1;
        if !self.params.contains(&param) {
            self.params.push(param);
        }
    }

    fn next_alias(&mut self) -> String {
        loop {
            let alias = format!("{ALIAS_PREFIX}{}", self.next_alias);
            self.next_alias += 1;
            if self.aliases.insert(alias.clone()) {
                return alias;
            }
        }
    }
}

/// Aggregates PgDog can't merge, such as `bool_or`, are still projected
/// whole, so the merge rejects them like it does in the SELECT list.
fn is_aggregate(function: &nodes::FuncCall, schema: &Schema) -> bool {
    function
        .funcname()
        .iter()
        .next_back()
        .and_then(Node::as_str)
        .is_some_and(|name| AggregateFunction::from_name(name, schema).is_some())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rewrite(sql: &str) -> Result<(String, HavingRewritePlan), Error> {
        let ast = pg_raw_parse::parse(sql).expect("parse HAVING query");
        let Node::SelectStmt(select) = ast.stmts().next().expect("one statement") else {
            panic!("expected SELECT")
        };
        let mut plan = None;
        let rewritten = make::try_owned(|mem| {
            let mut select = mem.make_unique(select);
            plan = Some(rewrite_select(
                &mut select.as_mut(),
                mem,
                &Schema::default(),
            )?);
            Ok::<_, Error>(select)
        })?;
        Ok((
            pg_raw_parse::deparse(&*rewritten)
                .expect("deparse rewritten query")
                .as_str()
                .to_owned(),
            plan.expect("rewrite plan"),
        ))
    }

    #[test]
    fn projects_operands_and_builds_filter_query() {
        let (sql, plan) = rewrite(
            "SELECT customer_id, COUNT(*) FROM purchases GROUP BY customer_id \
             HAVING COUNT(*)::numeric > $2 AND (customer_id <> $1 OR MAX(price) IS NULL)",
        )
        .unwrap();

        assert_eq!(
            sql,
            "SELECT customer_id, count(*), count(*) AS __pgdog_having_0, \
             customer_id AS __pgdog_having_1, max(price) AS __pgdog_having_2, \
             customer_id AS __pgdog_having_3 FROM purchases GROUP BY customer_id \
             HAVING (count(*)::numeric > $2 AND (customer_id <> $1 OR max(price) IS NULL)) OR true"
        );
        assert_eq!(
            plan.filter_sql(),
            "SELECT __pgdog_row FROM unnest($1, $2, $3) WITH ORDINALITY \
             __pgdog_having(__pgdog_having_0, __pgdog_having_1, __pgdog_having_2, __pgdog_row) \
             WHERE __pgdog_having_0::numeric > $4 \
             AND (__pgdog_having_1 <> $5 OR __pgdog_having_2 IS NULL)"
        );
        assert_eq!(plan.operand_aliases().len(), 3);
        assert_eq!(plan.hidden_aliases().len(), 4);
        assert_eq!(plan.params(), &[1, 0]);
    }

    #[test]
    fn moves_limit_and_offset_to_filter_query() {
        let (sql, plan) = rewrite(
            "SELECT customer_id, COUNT(*) FROM purchases GROUP BY customer_id \
             HAVING COUNT(*) > $2 ORDER BY customer_id LIMIT $1 OFFSET 1",
        )
        .unwrap();

        assert_eq!(
            sql,
            "SELECT customer_id, count(*), count(*) AS __pgdog_having_0, \
             customer_id AS __pgdog_having_1 FROM purchases GROUP BY customer_id \
             HAVING count(*) > $2 OR true ORDER BY customer_id \
             LIMIT CASE WHEN false THEN $1::bigint END"
        );
        assert_eq!(
            plan.filter_sql(),
            "SELECT __pgdog_row FROM unnest($1) WITH ORDINALITY \
             __pgdog_having(__pgdog_having_0, __pgdog_row) \
             WHERE __pgdog_having_0 > $2 ORDER BY __pgdog_row LIMIT $3 OFFSET 1"
        );
        assert_eq!(plan.params(), &[1, 0]);
    }

    #[test]
    fn rejects_operands_that_cannot_be_merged() {
        for sql in [
            // Can't be merged from per-shard partial results.
            "SELECT customer_id FROM purchases GROUP BY customer_id \
             HAVING COUNT(DISTINCT price) > 1",
            "SELECT customer_id FROM purchases GROUP BY customer_id \
             HAVING COUNT(*) FILTER (WHERE price > 1) > 1",
            // Not implemented yet.
            "SELECT customer_id FROM purchases GROUP BY customer_id \
             HAVING COUNT(*) > (SELECT 1)",
            // References neither an aggregate nor a grouped column.
            "SELECT customer_id FROM purchases GROUP BY customer_id HAVING $1",
        ] {
            assert!(
                matches!(rewrite(sql), Err(Error::UnsupportedHaving(_))),
                "{sql}"
            );
        }
    }
}
