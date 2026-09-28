//! Route-dependent SELECT rewrites.
//!
//! The cached AST stays in its base form so one statement can safely alternate
//! between direct and cross-shard execution. Cross-shard SQL is built from that
//! AST only after routing and cached independently from per-execution Bind values.

use super::Error;
use super::aggregate::{AggregatesRewrite, HelperKind};
use super::offset::{self, OffsetPlan};
use super::order_by;
use crate::backend::schema::Schema;
use crate::frontend::router::parser::{Aggregate, OrderBy};
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

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ProjectionRewritePlan {
    pub(crate) aggregate_helpers: Vec<AggregateHelper>,
    pub(crate) order_by_helpers: Vec<OrderByHelper>,
}

impl ProjectionRewritePlan {
    pub(crate) fn is_noop(&self) -> bool {
        self.aggregate_helpers.is_empty() && self.order_by_helpers.is_empty()
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
            .filter_map(|alias| row_description.field_index(alias))
            .collect()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PostRouteRewrite {
    sql: Arc<str>,
    plan: ProjectionRewritePlan,
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
    if !rewrite.plan.is_noop()
        && let Some(route) = request.route.as_mut()
    {
        route.projection_rewrite_plan = rewrite.plan.clone();
        let mut order_by = route.order_by().to_vec();
        for helper in &rewrite.plan.order_by_helpers {
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

    let aggregate = Aggregate::parse(select, schema);
    if aggregate.is_empty() && select.sort_clause().is_empty() && !rewrite_offset {
        return Ok(None);
    }

    let mut plan = ProjectionRewritePlan::default();
    let rewritten = make::owned(|mem| {
        let mut select = mem.make_unique(select);
        if !aggregate.is_empty() {
            plan = AggregatesRewrite::rewrite_select(&mut select.as_mut(), mem, &aggregate);
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
}
