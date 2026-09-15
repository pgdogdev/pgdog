use super::Error;
use super::aggregate::{AggregatesRewrite, HelperKind};
use super::offset::{self, OffsetPlan};
use super::order_by;
use crate::backend::schema::Schema;
use crate::frontend::router::parser::{Aggregate, OrderBy};
use crate::frontend::{ClientRequest, PreparedStatements};
use crate::net::ProtocolMessage;
use pg_raw_parse::{Node, StmtList, make};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AggregateHelper {
    pub(crate) target_column: usize,
    pub(crate) projected_column: usize,
    pub(crate) distinct: bool,
    pub(crate) kind: HelperKind,
    pub(crate) alias: String,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OrderByHelper {
    pub(crate) sort_position: usize,
    pub(crate) source: OrderBySource,
    pub(crate) projected_column: usize,
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

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ProjectionRewritePlan {
    aggregate_helpers: Vec<AggregateHelper>,
    order_by_helpers: Vec<OrderByHelper>,
}

impl ProjectionRewritePlan {
    pub(crate) fn is_noop(&self) -> bool {
        self.aggregate_helpers.is_empty() && self.order_by_helpers.is_empty()
    }

    pub(crate) fn drop_columns(&self) -> impl Iterator<Item = usize> + '_ {
        self.aggregate_helpers
            .iter()
            .map(|helper| helper.projected_column)
            .chain(
                self.order_by_helpers
                    .iter()
                    .map(|helper| helper.projected_column),
            )
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
    if request.is_executable()
        && let Some(parse) = request.last_parse.as_mut()
    {
        parse.set_query(&rewrite.sql);
    }
    if !rewrite.plan.is_noop()
        && let Some(route) = request.route.as_mut()
    {
        route.set_projection_rewrite_plan(rewrite.plan.clone());
        let mut order_by = route.order_by().to_vec();
        for helper in rewrite.plan.order_by_helpers() {
            let position = order_by
                .get(helper.sort_position)
                .filter(|sort| helper.matches(sort))
                .map(|_| helper.sort_position)
                .or_else(|| order_by.iter().position(|sort| helper.matches(sort)));
            let Some(sort) = position.and_then(|position| order_by.get_mut(position)) else {
                continue;
            };
            *sort = if sort.asc() {
                OrderBy::Asc(helper.projected_column + 1)
            } else {
                OrderBy::Desc(helper.projected_column + 1)
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

    let aggregate = Aggregate::parse(select, schema);
    if aggregate.is_empty() && select.sort_clause().is_empty() && !rewrite_offset {
        return Ok(None);
    }

    let mut plan = ProjectionRewritePlan::default();
    let rewritten = make::owned(|mem| {
        let mut select = mem.make_unique(select);
        if !aggregate.is_empty() {
            plan = AggregatesRewrite::rewrite_select(&mut select.as_mut(), mem, &aggregate).plan;
        }
        order_by::rewrite_select(&mut select.as_mut(), mem, &mut plan);
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
            projected_column: 1,
            distinct: false,
            kind: HelperKind::Count,
            alias: "__pgdog_count_col0".into(),
        });
        plan.add_order_by_helper(OrderByHelper {
            sort_position: 0,
            source: OrderBySource::Column("created_at".into()),
            projected_column: 2,
        });

        assert!(!plan.is_noop());
        assert_eq!(plan.drop_columns().collect::<Vec<_>>(), [1, 2]);
        assert_eq!(plan.aggregate_helpers().len(), 1);
        assert_eq!(plan.order_by_helpers().len(), 1);
    }
}
