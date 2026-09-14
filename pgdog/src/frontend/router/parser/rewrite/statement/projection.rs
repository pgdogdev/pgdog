use super::Error;
use super::aggregate::{AggregatesRewrite, HelperKind};
use super::offset::{self, OffsetPlan};
use super::order_by;
use crate::backend::schema::Schema;
use crate::frontend::router::parser::{Aggregate, OrderBy};
use crate::frontend::{ClientRequest, PreparedStatements};
use crate::net::ProtocolMessage;
use crate::net::messages::RowDescription;
use pg_raw_parse::{Node, StmtList, make};
use std::collections::BTreeSet;
use std::sync::Arc;

/// Aggregate function projected temporarily for cross-shard merging.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AggregateHelper {
    pub(crate) target_column: usize,
    pub(crate) distinct: bool,
    pub(crate) kind: HelperKind,
    pub(crate) alias: String,
}

/// Column projected temporarily for cross-shard ordering.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OrderByHelper {
    pub(crate) sort_position: usize,
    pub(crate) alias: String,
}

/// Temporary result columns required while merging cross-shard results.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ProjectionRewritePlan {
    aggregate_helpers: Vec<AggregateHelper>,
    order_by_helpers: Vec<OrderByHelper>,
}

impl ProjectionRewritePlan {
    pub(crate) fn is_noop(&self) -> bool {
        self.aggregate_helpers.is_empty() && self.order_by_helpers.is_empty()
    }

    /// Aliases of every column we added to the statement.
    pub(crate) fn aliases(&self) -> impl Iterator<Item = &str> {
        self.aggregate_helpers
            .iter()
            .map(|helper| helper.alias.as_str())
            .chain(
                self.order_by_helpers
                    .iter()
                    .map(|helper| helper.alias.as_str()),
            )
    }

    /// Positions of our columns in the result set.
    ///
    /// Resolved against the `RowDescription` Postgres sent back: the position
    /// in the statement is not the position in the result, because `*` expands
    /// to however many columns the table has.
    pub(crate) fn drop_columns(&self, row_description: &RowDescription) -> BTreeSet<usize> {
        self.aliases()
            .filter_map(|alias| row_description.field_index(alias))
            .collect()
    }

    pub(crate) fn aggregate_helpers(&self) -> &[AggregateHelper] {
        &self.aggregate_helpers
    }

    pub(crate) fn order_by_helpers(&self) -> &[OrderByHelper] {
        &self.order_by_helpers
    }

    pub(crate) fn add_aggregate_helper(&mut self, helper: AggregateHelper) {
        self.aggregate_helpers.push(helper);
    }

    pub(crate) fn add_order_by_helper(&mut self, helper: OrderByHelper) {
        self.order_by_helpers.push(helper);
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct RewriteOutput {
    pub(crate) plan: ProjectionRewritePlan,
}

#[derive(Debug, Clone)]
pub(crate) struct PostRouteRewrite {
    sql: Arc<str>,
    plan: ProjectionRewritePlan,
}

impl RewriteOutput {
    pub(crate) fn new(plan: ProjectionRewritePlan) -> Self {
        Self { plan }
    }
}

/// Add temporary columns needed to merge a cross-shard SELECT.
///
/// This deliberately operates on a copy of the cached AST. The cached AST is
/// the route-independent representation and must remain suitable for direct
/// execution.
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
    let order_by = request.route().order_by();
    let Some(rewrite) = ast
        .post_route_rewrite
        .get_or_try_init(|| build(&ast.ast, schema, order_by, rewrite_offset))?
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
    let variant = base_name.and_then(|name| {
        PreparedStatements::global()
            .write()
            .cross_shard_variant(name, &rewrite.sql)
    });

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
    if let Some(parse) = request.last_parse.as_mut() {
        parse.set_query(&rewrite.sql);
    }
    if !rewrite.plan.is_noop()
        && let Some(route) = request.route.as_mut()
    {
        route.set_projection_rewrite_plan(rewrite.plan.clone());
        let mut order_by = route.order_by().to_vec();
        for helper in rewrite.plan.order_by_helpers() {
            let Some(sort) = order_by.get_mut(helper.sort_position) else {
                continue;
            };
            // Sort by name: the alias is resolved against the RowDescription,
            // so it survives `*` expanding to any number of columns.
            *sort = match &*sort {
                OrderBy::AscVectorL2Column(_, vector) => {
                    OrderBy::AscVectorL2Column(helper.alias.clone(), vector.clone())
                }
                sort if sort.asc() => OrderBy::AscColumn(helper.alias.clone()),
                _ => OrderBy::DescColumn(helper.alias.clone()),
            };
        }
        route.set_order_by(order_by);
    }

    Ok(())
}

fn build(
    ast: &StmtList,
    schema: &Schema,
    order_by: &[OrderBy],
    rewrite_offset: bool,
) -> Result<Option<PostRouteRewrite>, Error> {
    let Some(Node::SelectStmt(select)) = ast.stmts().next() else {
        return Ok(None);
    };

    let aggregate = Aggregate::parse(select, schema);
    if aggregate.is_empty() && order_by.is_empty() && !rewrite_offset {
        return Ok(None);
    }

    let mut plan = ProjectionRewritePlan::default();
    let rewritten = make::owned(|mem| {
        let mut select = mem.make_unique(select);
        if !aggregate.is_empty() {
            plan = AggregatesRewrite::rewrite_select(&mut select.as_mut(), mem, &aggregate).plan;
        }
        order_by::rewrite_select(&mut select.as_mut(), mem, order_by, &mut plan);
        if rewrite_offset {
            offset::rewrite_select(&mut select.as_mut(), mem);
        }
        select
    });
    if plan.is_noop() && !rewrite_offset {
        return Ok(None);
    }
    let sql: Arc<str> = pg_raw_parse::deparse(&*rewritten)?.as_str().into();

    Ok(Some(PostRouteRewrite { sql, plan }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projection_plan_tracks_helpers() {
        let mut plan = ProjectionRewritePlan::default();
        plan.add_aggregate_helper(AggregateHelper {
            target_column: 0,
            distinct: false,
            kind: HelperKind::Count,
            alias: "__pgdog_count_col0".into(),
        });
        plan.add_order_by_helper(OrderByHelper {
            sort_position: 0,
            alias: "__pgdog_order_col0".into(),
        });

        assert!(!plan.is_noop());
        assert_eq!(
            plan.aliases().collect::<Vec<_>>(),
            ["__pgdog_count_col0", "__pgdog_order_col0"]
        );
        assert_eq!(plan.aggregate_helpers().len(), 1);
        assert_eq!(plan.order_by_helpers().len(), 1);
    }

    /// `SELECT *` expands to N columns, so our helpers are not where the
    /// statement says they are. Resolve them by name instead.
    #[test]
    fn drop_columns_resolves_positions_from_row_description() {
        use crate::net::messages::Field;

        let mut plan = ProjectionRewritePlan::default();
        plan.add_order_by_helper(OrderByHelper {
            sort_position: 0,
            alias: "__pgdog_order_col0".into(),
        });

        let row_description = RowDescription::new(&[
            Field::bigint("id"),
            Field::text("value"),
            Field::bigint("__pgdog_order_col0"),
        ]);

        assert_eq!(
            plan.drop_columns(&row_description),
            BTreeSet::from([2]),
            "helper sits after the expanded columns"
        );
    }
}
