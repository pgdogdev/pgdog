use pg_query::{
    NodeEnum,
    protobuf::{
        AConst, BoolExpr, BoolExprType, Boolean, Node, ParseResult as ProtobufParseResult,
        ResTarget, a_const::Val,
    },
};

use crate::{
    backend::schema::Schema,
    frontend::ClientRequest,
    frontend::router::parser::{
        Aggregate, HavingExprTemplate, HavingLiteral, HavingRewritePlan,
        having::HavingValueTemplate,
    },
    net::{ProtocolMessage, messages::bind::Bind},
};

use super::Error;

pub(super) fn apply_having_after_parser(request: &mut ClientRequest) -> Result<(), Error> {
    let aggregate = match request.route.as_ref() {
        Some(route) if route.is_cross_shard() => route.aggregate().clone(),
        _ => return Ok(()),
    };

    let ast = match request.ast.as_ref() {
        Some(ast) => ast,
        None => return Ok(()),
    };

    let mut source_protobuf = ast.ast.protobuf.clone();
    let Some(select) = select_stmt_mut(&mut source_protobuf) else {
        return Ok(());
    };

    let mut template = match HavingExprTemplate::parse(select, &aggregate) {
        Ok(Some(template)) => template,
        Ok(None) | Err(_) => return Ok(()),
    };

    let bind = first_bind(request);
    if bind.is_none() && !template.param_indices().is_empty() {
        return Ok(());
    }

    let Some(current_sql) = current_sql(request) else {
        return Ok(());
    };

    let mut rewritten = pg_query::parse(current_sql)?.protobuf;
    let Some(select) = select_stmt_mut(&mut rewritten) else {
        return Ok(());
    };
    let having_plan = materialize_hidden_expressions(select, &mut template);
    let resolved = template.resolve_with(&|idx| {
        let bind = bind?;
        decode_bind_param(bind, idx)
    });
    let Some(resolved) = resolved else {
        return Ok(());
    };
    if !rewrite_having_clause_true(select) {
        return Ok(());
    }

    let rewritten_aggregate = if having_plan.is_noop() {
        None
    } else {
        Some(Aggregate::parse(select, &Schema::default()))
    };
    let new_sql = pg_query::ParseResult::new(rewritten, String::new()).deparse()?;
    for message in request.messages.iter_mut() {
        match message {
            ProtocolMessage::Query(query) => query.set_query(&new_sql),
            ProtocolMessage::Parse(parse) => parse.set_query(&new_sql),
            _ => {}
        }
    }

    if let Some(route) = request.route.as_mut() {
        route.set_having(Some(resolved));
        route.set_having_rewrite_plan(having_plan);
        if let Some(rewritten_aggregate) = rewritten_aggregate {
            *route.aggregate_mut() = rewritten_aggregate;
        }
    }
    Ok(())
}

fn first_bind(request: &ClientRequest) -> Option<&Bind> {
    request.messages.iter().find_map(|message| match message {
        ProtocolMessage::Bind(bind) => Some(bind),
        _ => None,
    })
}

fn current_sql(request: &ClientRequest) -> Option<&str> {
    request.messages.iter().find_map(|message| match message {
        ProtocolMessage::Query(query) => Some(query.query()),
        ProtocolMessage::Parse(parse) => Some(parse.query()),
        _ => None,
    })
}

fn select_stmt_mut(ast: &mut ProtobufParseResult) -> Option<&mut pg_query::protobuf::SelectStmt> {
    let stmt = ast.stmts.first_mut()?.stmt.as_mut()?;
    match stmt.node.as_mut()? {
        NodeEnum::SelectStmt(select) => Some(select.as_mut()),
        _ => None,
    }
}

fn decode_bind_param(bind: &Bind, index: usize) -> Option<HavingLiteral> {
    let parameter = bind.parameter(index).ok()??;
    if parameter.is_null() {
        return Some(HavingLiteral::Null);
    }

    if let Some(text) = parameter.text() {
        if let Ok(boolean) = text.parse::<bool>() {
            return Some(HavingLiteral::Boolean(boolean));
        }
        if let Ok(integer) = text.parse::<i64>() {
            return Some(HavingLiteral::Integer(integer));
        }
        if let Ok(float) = text.parse::<f64>() {
            return Some(HavingLiteral::Float(float));
        }
        return Some(HavingLiteral::String(text.to_owned()));
    }

    if let Some(integer) = parameter.decode::<i64>() {
        return Some(HavingLiteral::Integer(integer));
    }
    if let Some(boolean) = parameter.decode::<bool>() {
        return Some(HavingLiteral::Boolean(boolean));
    }
    if let Some(text) = parameter.decode::<String>() {
        return Some(HavingLiteral::String(text));
    }

    Some(HavingLiteral::String(parameter.text_debug()))
}

fn materialize_hidden_expressions(
    select: &mut pg_query::protobuf::SelectStmt,
    template: &mut HavingExprTemplate,
) -> HavingRewritePlan {
    let mut plan = HavingRewritePlan::default();
    let mut next = 0usize;
    rewrite_template_values(template, &mut |value| {
        let HavingValueTemplate::Expression(expr) = value else {
            return;
        };
        let alias = format!("__pgdog_having_expr{}", next);
        next += 1;

        let target = Node {
            node: Some(NodeEnum::ResTarget(Box::new(ResTarget {
                name: alias.clone(),
                val: Some(Box::new(expr.clone())),
                ..Default::default()
            }))),
        };
        select.target_list.push(target);
        plan.add_hidden_alias(alias.clone());
        *value = HavingValueTemplate::ColumnName(alias);
    });
    plan
}

fn rewrite_template_values(
    expr: &mut HavingExprTemplate,
    callback: &mut impl FnMut(&mut HavingValueTemplate),
) {
    match expr {
        HavingExprTemplate::Compare { left, right, .. } => {
            callback(left);
            callback(right);
        }
        HavingExprTemplate::And(children) | HavingExprTemplate::Or(children) => {
            for child in children {
                rewrite_template_values(child, callback);
            }
        }
        HavingExprTemplate::IsNull { value, .. } => callback(value),
    }
}

fn rewrite_having_clause_true(select: &mut pg_query::protobuf::SelectStmt) -> bool {
    let Some(existing) = select.having_clause.take() else {
        return false;
    };

    let true_node = Node {
        node: Some(NodeEnum::AConst(AConst {
            val: Some(Val::Boolval(Boolean { boolval: true })),
            isnull: false,
            location: -1,
        })),
    };

    select.having_clause = Some(Box::new(Node {
        node: Some(NodeEnum::BoolExpr(Box::new(BoolExpr {
            boolop: BoolExprType::OrExpr.into(),
            args: vec![*existing, true_node],
            location: -1,
            ..Default::default()
        }))),
    }));

    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frontend::router::parser::cache::ast::Ast;
    use crate::frontend::router::parser::{Aggregate, DistinctBy};
    use crate::frontend::router::parser::{Limit, Route, Shard, ShardWithPriority};
    use crate::net::{Parse, messages::Query};

    fn cross_shard_route(aggregate: Aggregate) -> Route {
        Route::select(
            ShardWithPriority::new_table(Shard::All),
            vec![],
            aggregate,
            Limit::default(),
            Option::<DistinctBy>::None,
        )
    }

    #[test]
    fn apply_having_rewrites_query_and_sets_route_having() {
        let sql = "SELECT user_id, COUNT(*) AS c FROM t GROUP BY user_id HAVING COUNT(*) > 2";
        let ast = Ast::new_record(sql, pgdog_config::QueryParserEngine::PgQueryProtobuf).unwrap();
        let aggregate = crate::frontend::router::parser::Aggregate::parse(
            match ast
                .ast
                .protobuf
                .stmts
                .first()
                .and_then(|stmt| stmt.stmt.as_ref())
                .and_then(|stmt| stmt.node.as_ref())
            {
                Some(NodeEnum::SelectStmt(select)) => select,
                _ => panic!("not a select"),
            },
            &Default::default(),
        );

        let mut request = ClientRequest::from(vec![
            ProtocolMessage::Query(Query::new(sql)),
            ProtocolMessage::Parse(Parse::named("s", sql)),
        ]);
        request.ast = Some(ast);
        request.route = Some(cross_shard_route(aggregate));

        apply_having_after_parser(&mut request).unwrap();

        let query = match &request.messages[0] {
            ProtocolMessage::Query(query) => query.query(),
            _ => panic!("expected query"),
        };
        assert!(query.contains("HAVING"));
        assert!(query.contains("true"));
        let route = request.route.unwrap();
        assert!(route.having().is_some());
        assert!(route.having_rewrite_plan().is_noop());
    }

    #[test]
    fn apply_having_injects_hidden_target_for_missing_aggregate() {
        let sql = "SELECT user_id FROM t GROUP BY user_id HAVING COUNT(*) > 1";
        let ast = Ast::new_record(sql, pgdog_config::QueryParserEngine::PgQueryProtobuf).unwrap();
        let aggregate = crate::frontend::router::parser::Aggregate::parse(
            match ast
                .ast
                .protobuf
                .stmts
                .first()
                .and_then(|stmt| stmt.stmt.as_ref())
                .and_then(|stmt| stmt.node.as_ref())
            {
                Some(NodeEnum::SelectStmt(select)) => select,
                _ => panic!("not a select"),
            },
            &Default::default(),
        );

        let mut request = ClientRequest::from(vec![
            ProtocolMessage::Query(Query::new(sql)),
            ProtocolMessage::Parse(Parse::named("s", sql)),
        ]);
        request.ast = Some(ast);
        request.route = Some(cross_shard_route(aggregate));

        apply_having_after_parser(&mut request).unwrap();

        let query = match &request.messages[0] {
            ProtocolMessage::Query(query) => query.query(),
            _ => panic!("expected query"),
        };
        assert!(query.contains("__pgdog_having_expr0"));
        assert!(query.contains("HAVING"));
        assert!(query.contains("true"));

        let route = request.route.unwrap();
        assert!(route.having().is_some());
        assert!(!route.having_rewrite_plan().is_noop());
        assert_eq!(route.having_rewrite_plan().hidden_aliases().len(), 1);
        assert!(!route.aggregate().targets().is_empty());
    }
}
