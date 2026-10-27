//! Route-dependent SELECT rewrites.
//!
//! The cached AST stays in its base form so one statement can safely alternate
//! between direct and cross-shard execution. Cross-shard SQL is built from that
//! AST only after routing and cached independently from per-execution Bind values.

use super::Error;
use super::aggregate::{AggregatesRewrite, HelperKind};
use super::having;
use super::offset::{self, OffsetPlan};
use super::order_by;
use crate::backend::schema::Schema;
use crate::frontend::router::parser::{Aggregate, HavingExpr, HavingRewritePlan, OrderBy};
use crate::frontend::{ClientRequest, PreparedStatements};
use crate::net::{ProtocolMessage, RowDescription};
use pg_raw_parse::{Node, StmtList, make};
use std::collections::BTreeSet;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AggregateHelper {
    pub(crate) target_column: usize,
    pub(crate) distinct: bool,
    pub(crate) kind: HelperKind,
    pub(crate) alias: String,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OrderByHelper {
    pub(crate) sort_position: usize,
    pub(crate) source: OrderBySource,
    pub(crate) alias: String,
    /// False when the SELECT list already has this unique name and we only
    /// remap the route. Those columns must stay in the client result.
    pub(crate) injected: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum OrderBySource {
    Column(String),
    Vector(String),
}

impl OrderByHelper {
    fn matches(&self, order_by: &OrderBy) -> bool {
        match (&self.source, order_by) {
            (OrderBySource::Column(source), OrderBy::AscColumn(column))
            | (OrderBySource::Column(source), OrderBy::DescColumn(column))
            | (OrderBySource::Vector(source), OrderBy::AscVectorL2Column(column, _)) => {
                source == column
            }
            _ => false,
        }
    }
}

#[derive(Debug, Default, PartialEq)]
pub(crate) struct ProjectionRewritePlan {
    pub(crate) aggregate_helpers: Vec<AggregateHelper>,
    pub(crate) order_by_helpers: Vec<OrderByHelper>,
    pub(crate) having: Option<Box<HavingRewriteResult>>,
}

/// The filter and aggregate metadata produced by rewriting a HAVING query.
#[derive(Debug, PartialEq)]
pub(crate) struct HavingRewriteResult {
    rewrite: HavingRewritePlan,
    aggregate: Aggregate,
}

impl HavingRewriteResult {
    pub(crate) fn new(rewrite: HavingRewritePlan, aggregate: Aggregate) -> Self {
        Self { rewrite, aggregate }
    }

    pub(crate) fn expression(&self) -> &HavingExpr {
        self.rewrite.expression()
    }

    pub(crate) fn hidden_aliases(&self) -> &[String] {
        self.rewrite.hidden_aliases()
    }

    pub(crate) fn aggregate(&self) -> &Aggregate {
        &self.aggregate
    }
}

impl ProjectionRewritePlan {
    pub(crate) fn is_noop(&self) -> bool {
        self.aggregate_helpers.is_empty()
            && self.order_by_helpers.is_empty()
            && self.having.is_none()
    }

    pub(crate) fn drop_columns(&self, row_description: &RowDescription) -> BTreeSet<usize> {
        self.aggregate_helpers
            .iter()
            .map(|helper| helper.alias.as_str())
            .chain(
                self.order_by_helpers
                    .iter()
                    .filter(|helper| helper.injected)
                    .map(|helper| helper.alias.as_str()),
            )
            .chain(
                self.having
                    .iter()
                    .flat_map(|plan| plan.hidden_aliases().iter().map(String::as_str)),
            )
            .filter_map(|alias| row_description.field_index(alias))
            .collect()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PostRouteRewrite {
    sql: Arc<str>,
    plan: Option<Arc<ProjectionRewritePlan>>,
}

pub(crate) fn finalize_after_route(
    request: &mut ClientRequest,
    schema: &Schema,
    offset_plan: Option<&OffsetPlan>,
) -> Result<(), Error> {
    if !request.route().is_cross_shard() {
        return Ok(());
    }

    let Some(ast) = request.ast.as_ref() else {
        return Ok(());
    };
    let rewrite_offset = offset_plan.is_some_and(|plan| !plan.prepare_execute);
    let Some(rewrite) = ast
        .post_route_rewrite
        .get_or_try_init(|| build(&ast.ast, schema, rewrite_offset))?
    else {
        return Ok(());
    };
    let base_name = request.messages.iter().find_map(|message| match message {
        ProtocolMessage::Parse(parse) if !parse.anonymous() => Some(parse.name()),
        ProtocolMessage::Bind(bind) if !bind.anonymous() => Some(bind.statement()),
        ProtocolMessage::Describe(describe) if describe.is_statement() && !describe.anonymous() => {
            Some(describe.statement())
        }
        _ => None,
    });
    let variant =
        base_name.and_then(|name| PreparedStatements::cross_shard_variant(name, &rewrite.sql));

    for message in &mut request.messages {
        match message {
            ProtocolMessage::Query(query) => query.set_query(&rewrite.sql),
            ProtocolMessage::Parse(parse) => {
                parse.set_query(&rewrite.sql);
                if let Some(variant) = &variant {
                    parse.rename(variant);
                }
            }
            ProtocolMessage::Bind(bind) => {
                if let Some(variant) = &variant {
                    bind.rename(variant);
                }
            }
            ProtocolMessage::Describe(describe) if describe.is_statement() => {
                if let Some(variant) = &variant {
                    describe.rename(variant);
                }
            }
            _ => {}
        }
    }
    // Parse/Describe-only requests must keep the saved anonymous Parse in its
    // base form; the later Bind/Execute request will finalize its injected copy.
    if request.is_executable()
        && let Some(parse) = request.last_parse.as_mut()
    {
        parse.set_query(&rewrite.sql);
    }
    if let Some(plan) = &rewrite.plan
        && let Some(route) = request.route.as_mut()
    {
        route.projection_rewrite_plan = Some(Arc::clone(plan));
        if let Some(having) = &plan.having {
            route.set_aggregate(having.aggregate().clone());
        }
        let mut order_by = route.order_by().to_vec();
        for helper in &plan.order_by_helpers {
            // Prefer the structural position. The source fallback handles a
            // bind-dependent vector sort omitted from this execution's route.
            let position = order_by
                .get(helper.sort_position)
                .filter(|sort| helper.matches(sort))
                .map(|_| helper.sort_position)
                .or_else(|| order_by.iter().position(|sort| helper.matches(sort)));
            let Some(sort) = position.and_then(|position| order_by.get_mut(position)) else {
                continue;
            };
            *sort = if sort.asc() {
                OrderBy::AscColumn(helper.alias.clone())
            } else {
                OrderBy::DescColumn(helper.alias.clone())
            };
        }
        route.set_order_by(order_by);
    }

    Ok(())
}

fn build(
    ast: &StmtList,
    schema: &Schema,
    rewrite_offset: bool,
) -> Result<Option<PostRouteRewrite>, Error> {
    let Some(Node::SelectStmt(select)) = ast.stmts().next() else {
        return Ok(None);
    };
    let has_having = !matches!(select.having_clause(), Node::None);
    let aggregate = if has_having {
        None
    } else {
        let aggregate = Aggregate::parse(select, schema);
        if aggregate.is_empty() && select.sort_clause().is_empty() && !rewrite_offset {
            return Ok(None);
        }
        Some(aggregate)
    };

    // We can't sort by an expression with an aggregate in it, e.g., `ORDER BY count(*)`.
    // Each shard runs the aggregate ONLY on the rows stored directly on it, e.g., Shard A counts 2 'free' rows and Shard B counts 3.
    // The merge adds up the aggregates in the SELECT list, but not the helper column, so we would sort by Shard A's count (2) instead of the total (5).
    // We can't sort by an expression with DISTINCT either, as the helper column would change which rows are distinct.
    let has_unsupported_sort_key = select.sort_clause().iter().any(|sort_by| {
        order_by::is_computed_sort_key(sort_by.node())
            && (order_by::contains_aggregate(sort_by.node(), schema)
                || !select.distinct_clause().is_empty())
    });

    if has_unsupported_sort_key {
        return Err(Error::OrderByExpressionUnsupported);
    }

    let mut plan = ProjectionRewritePlan::default();
    let rewritten = make::try_owned(|mem| {
        let mut select = mem.make_unique(select);
        if has_having {
            let having = having::rewrite_select(&mut select.as_mut(), mem)?;
            let rewritten_aggregate = Aggregate::parse(&select, schema);
            if rewritten_aggregate.is_empty() {
                return Err(Error::UnsupportedHaving(
                    "at least one aggregate function is required",
                ));
            }
            // Aggregate::parse skips GROUP BY items it cannot resolve. Require
            // every item here so unrelated groups are never merged together.
            if rewritten_aggregate.group_by().len() != select.group_clause().len() {
                return Err(Error::UnsupportedHaving(
                    "every GROUP BY expression must be a selected column or ordinal",
                ));
            }
            plan.having = Some(Box::new(HavingRewriteResult::new(
                having,
                rewritten_aggregate,
            )));
        }
        let aggregate = plan
            .having
            .as_deref()
            .map(HavingRewriteResult::aggregate)
            .or(aggregate.as_ref())
            .expect("non-HAVING aggregate parsed before projection rewrite");
        if !aggregate.is_empty() {
            let aggregate_plan =
                AggregatesRewrite::rewrite_select(&mut select.as_mut(), mem, aggregate);
            plan.aggregate_helpers = aggregate_plan.aggregate_helpers;
        }
        order_by::rewrite_select(&mut select.as_mut(), mem, &mut plan);
        if rewrite_offset {
            offset::rewrite_select(&mut select.as_mut(), mem);
        }
        Ok::<_, Error>(select)
    })?;
    if plan.is_noop() && !rewrite_offset {
        return Ok(None);
    }
    let sql: Arc<str> = pg_raw_parse::deparse(&*rewritten)?.as_str().into();
    let plan = (!plan.is_noop()).then(|| Arc::new(plan));

    Ok(Some(PostRouteRewrite { sql, plan }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_count_sort_key() {
        let ast = pg_raw_parse::parse("SELECT value FROM items GROUP BY value ORDER BY count(*)")
            .unwrap();

        let rewrite_result = build(&ast, &Schema::default(), false);

        assert!(matches!(
            rewrite_result,
            Err(Error::OrderByExpressionUnsupported)
        ));
    }

    #[test]
    fn projection_plan_tracks_helpers() {
        let mut plan = ProjectionRewritePlan::default();
        plan.aggregate_helpers.push(AggregateHelper {
            target_column: 0,
            distinct: false,
            kind: HelperKind::Count,
            alias: "__pgdog_count_col0".into(),
        });
        plan.order_by_helpers.push(OrderByHelper {
            sort_position: 0,
            source: OrderBySource::Column("created_at".into()),
            alias: "__pgdog_order_col0".into(),
            injected: true,
        });

        assert!(!plan.is_noop());
        let row_description = RowDescription::new(&[
            crate::net::Field::double("avg"),
            crate::net::Field::bigint("__pgdog_count_col0"),
            crate::net::Field::timestamp("__pgdog_order_col0"),
        ]);
        assert_eq!(plan.drop_columns(&row_description), BTreeSet::from([1, 2]));
        assert_eq!(plan.aggregate_helpers.len(), 1);
        assert_eq!(plan.order_by_helpers.len(), 1);
    }

    #[test]
    fn remapped_order_by_alias_is_not_dropped() {
        let mut plan = ProjectionRewritePlan::default();
        plan.order_by_helpers.push(OrderByHelper {
            sort_position: 0,
            source: OrderBySource::Column("price".into()),
            alias: "item_price".into(),
            injected: false,
        });

        let row_description = RowDescription::new(&[
            crate::net::Field::bigint("id"),
            crate::net::Field::numeric("item_price"),
        ]);
        assert!(plan.drop_columns(&row_description).is_empty());
    }

    #[test]
    fn having_internal_aliases_do_not_collide_with_client_aliases() {
        let ast = pg_raw_parse::parse(
            "SELECT __pgdog_having_0, COUNT(*) \
             FROM purchases GROUP BY __pgdog_having_0 HAVING COUNT(*) > 2",
        )
        .unwrap();
        let rewrite = build(&ast, &Schema::default(), false)
            .unwrap()
            .expect("HAVING requires a rewrite");

        assert_eq!(
            rewrite
                .plan
                .as_deref()
                .expect("projection plan")
                .having
                .as_ref()
                .expect("HAVING plan")
                .hidden_aliases(),
            &["__pgdog_having_1", "__pgdog_having_2", "__pgdog_having_3"]
        );
    }

    #[test]
    fn having_projects_unselected_grouping_column() {
        let ast = pg_raw_parse::parse(
            "SELECT COUNT(*) FROM purchases \
             GROUP BY customer_id HAVING COUNT(*) > 2",
        )
        .unwrap();
        let rewrite = build(&ast, &Schema::default(), false)
            .unwrap()
            .expect("HAVING requires a rewrite");
        let plan = rewrite.plan.expect("projection plan");
        let aggregate = plan.having.as_ref().expect("HAVING plan");

        assert_eq!(aggregate.aggregate().group_by().len(), 1);
        assert_eq!(aggregate.aggregate().targets().len(), 2);
    }

    #[test]
    fn having_avg_receives_independent_count_helper() {
        let ast = pg_raw_parse::parse(
            "SELECT customer_id, AVG(price) FROM purchases \
             GROUP BY customer_id HAVING AVG(price) > 10",
        )
        .unwrap();
        let rewrite = build(&ast, &Schema::default(), false)
            .unwrap()
            .expect("HAVING requires a rewrite");

        assert_eq!(
            rewrite
                .plan
                .as_deref()
                .expect("projection plan")
                .having
                .as_ref()
                .expect("HAVING plan")
                .aggregate()
                .targets()
                .len(),
            2
        );
        assert_eq!(
            rewrite
                .plan
                .as_deref()
                .expect("projection plan")
                .aggregate_helpers
                .len(),
            2,
            "selected and hidden AVG each require their own COUNT"
        );
    }

    #[test]
    fn aggregate_used_only_by_having_is_merged_and_hidden() {
        let ast = pg_raw_parse::parse(
            "SELECT customer_id FROM purchases \
             GROUP BY customer_id HAVING COUNT(*) > 2",
        )
        .unwrap();
        let rewrite = build(&ast, &Schema::default(), false)
            .unwrap()
            .expect("HAVING requires a rewrite");
        let plan = rewrite.plan.expect("projection plan");
        let aggregate = plan.having.as_ref().expect("HAVING plan");

        assert_eq!(aggregate.aggregate().targets().len(), 1);
        assert_eq!(aggregate.aggregate().group_by().len(), 1);
    }

    #[test]
    fn unsupported_having_queries_fail_closed() {
        for sql in [
            "SELECT customer_id, SUM(price) FROM purchases \
             GROUP BY customer_id HAVING SUM(price) / COUNT(*) > 2",
            "SELECT customer_id FROM purchases \
             GROUP BY customer_id HAVING customer_id > 2",
            "SELECT lower(customer_id), COUNT(*) FROM purchases \
             GROUP BY lower(customer_id) HAVING COUNT(*) > 2",
        ] {
            let ast = pg_raw_parse::parse(sql).unwrap();
            let error = build(&ast, &Schema::default(), false).unwrap_err();
            assert!(matches!(error, Error::UnsupportedHaving(_)), "{sql}");
        }
    }
}
