use std::ops::Deref;

use pg_raw_parse::{ConstValue, Node, deparse, make, nodes};

use crate::frontend::ClientRequest;
use crate::frontend::router::parser::Limit;
use crate::net::ProtocolMessage;

use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct OffsetPlan {
    pub(crate) limit: Limit,
    pub(crate) limit_param: usize,
    pub(crate) offset_param: usize,
    pub(crate) prepare_execute: bool,
}

impl OffsetPlan {
    pub(super) fn apply_after_route(&self, request: &mut ClientRequest) -> Result<(), Error> {
        let route = match request.route.as_mut() {
            Some(route) => route,
            None => return Ok(()),
        };

        if !route.is_cross_shard() {
            return Ok(());
        }

        if self.prepare_execute {
            return self.handle_prepare_execute(request);
        }

        // Resolve actual values: use literal if known, otherwise read from Bind.
        let mut limit_val = self.limit.limit;
        let mut offset_val = self.limit.offset;

        for message in request.messages.iter_mut() {
            if let ProtocolMessage::Bind(bind) = message {
                if limit_val.is_none() && self.limit_param > 0 {
                    let idx = self.limit_param - 1;
                    limit_val = Some(
                        bind.parameter(idx)?
                            .ok_or(Error::MissingParameter(self.limit_param as u16))?
                            .bigint()
                            .ok_or(Error::MissingParameter(self.limit_param as u16))?
                            as usize,
                    );
                }
                if offset_val.is_none() {
                    let idx = self.offset_param - 1;
                    offset_val = Some(
                        bind.parameter(idx)?
                            .ok_or(Error::MissingParameter(self.offset_param as u16))?
                            .bigint()
                            .ok_or(Error::MissingParameter(self.offset_param as u16))?
                            as usize,
                    );
                }

                break;
            }
        }

        route.set_limit(Limit {
            limit: limit_val,
            offset: offset_val,
        });

        Ok(())
    }

    /// `apply_after_route` helper method for handling Prepare + Execute cases, where
    /// we need to re-write limit / offset for multi-shard queries upon execution.
    fn handle_prepare_execute(&self, request: &mut ClientRequest) -> Result<(), Error> {
        // Assert expectations of what should've happened before this method was called
        // in case something beforehand is changed in the future.
        assert!(
            self.prepare_execute,
            "self.prepare_execute was checked before method call"
        );

        let route = request
            .route
            .as_mut()
            .expect("route.is_some() was checked before method call");

        assert!(
            route.is_cross_shard(),
            "route.is_cross_shard() was checked before method call"
        );

        let node = &mut request.ast;
        let node = node.as_mut().ok_or(Error::MissingAst)?;
        let node = node.ast.first().ok_or(Error::MissingAst)?;

        let pg_raw_parse::Node::ExecuteStmt(execute) = node.stmt() else {
            unreachable!("The query must be ExecuteStmt to have reached here.");
        };

        let new_execute = pg_raw_parse::make::owned(|mem| {
            let mut execute_unique = mem.make_unique(execute);

            let mut mutable_execute_unique = execute_unique.as_mut();
            let mut params = mutable_execute_unique.params_mut();

            let new_limit = self.limit.limit.unwrap_or(0) + self.limit.offset.unwrap_or(0);

            // These are guarenteed to be `ParamRefs` because of our
            // re-write for all `A_Const` nodes for the original `PreparedStmt` that we cached.
            params.set(
                self.limit_param - 1,
                mem.make_a_const(ConstValue::Integer(new_limit as i32))
                    .uncast(),
            );
            params.set(
                self.offset_param - 1,
                mem.make_a_const(ConstValue::Integer(0i32)).uncast(),
            );

            execute_unique
        });

        let new_execute = new_execute.deref();
        let new_execute_sql = deparse(new_execute)?;

        // `ExecuteStmt` will be a Query, because this is simple-protocol.
        // Replace with our re-written `ExecuteStmt`
        // (replacing limit/offset with proper multi-shard vlaues)
        for message in request.messages.iter_mut() {
            if let ProtocolMessage::Query(query) = message {
                query.set_query(new_execute_sql.as_str());
            }
        }

        route.set_limit(Limit {
            limit: self.limit.limit,
            offset: self.limit.offset,
        });

        Ok(())
    }
}

#[derive(Debug)]
enum LimitValueInfo {
    Literal(usize),
    Param(usize),
}

impl LimitValueInfo {
    fn literal(&self) -> Option<usize> {
        match self {
            LimitValueInfo::Literal(v) => Some(*v),
            LimitValueInfo::Param(_) => None,
        }
    }

    fn param_index(&self) -> usize {
        match self {
            LimitValueInfo::Param(i) => *i,
            LimitValueInfo::Literal(_) => 0,
        }
    }
}

fn extract_limit_value(node: Node<'_>) -> Option<LimitValueInfo> {
    match node {
        Node::A_Const(c) if let Some(i) = c.val().and_then(|c| c.numeric_value::<i32>()) => {
            Some(LimitValueInfo::Literal(i as usize))
        }
        Node::ParamRef(nodes::ParamRef { number, .. }) => {
            Some(LimitValueInfo::Param(*number as usize))
        }
        _ => None,
    }
}

/// `$1 + $2` is ambiguous to Postgres when both sides are untyped parameters,
/// so spell out the type both operands would have had as LIMIT/OFFSET.
fn to_bigint<'a>(
    node: make::Unique<'a, Node<'a>>,
    mem: make::MemoryToken<'a>,
) -> make::Unique<'a, Node<'a>> {
    mem.make_type_cast(
        node,
        mem.make_list(&[
            mem.make_string(Some("pg_catalog")),
            mem.make_string(Some("int8")),
        ]),
    )
    .uncast()
}

/// Keep LIMIT and OFFSET as an expression instead of folding them into an
/// `A_Const`. One cached SQL form then works for literals and placeholders
/// without changing client Bind values or their text/binary encoding. Postgres
/// evaluates the per-shard fetch bound; the route keeps the original values for
/// final proxy-side pagination.
pub(super) fn rewrite_select<'a>(
    select: &mut nodes::SelectStmtMut<'a, '_>,
    mem: make::MemoryToken<'a>,
) {
    // Without LIMIT, NULL + OFFSET is NULL, which Postgres treats as no limit.
    let limit = match select.limit_count() {
        Node::None => mem.make_null().uncast(),
        limit => mem.make_unique(limit).uncast(),
    };
    let limit = to_bigint(limit, mem);
    let offset = to_bigint(mem.make_unique(select.limit_offset()).uncast(), mem);
    let combined = mem.make_a_expr(
        nodes::A_Expr_Kind::AEXPR_OP,
        mem.make_list(&[mem.make_string(Some("+")).uncast()]),
        limit,
        offset,
    );
    select.set_limit_count(combined.uncast());
    select.set_limit_offset(mem.none());
}

impl StatementRewrite<'_> {
    pub(super) fn limit_offset(&self, select: &nodes::SelectStmt, plan: &mut RewritePlan) {
        if self.schema.shards <= 1 {
            return;
        }

        let Some(offset_info) = extract_limit_value(select.limit_offset()) else {
            return;
        };

        let limit_info = match select.limit_count() {
            // No LIMIT, or LIMIT ALL. The OFFSET still has to move to the proxy,
            // or every shard skips those rows and the proxy skips them again.
            Node::None => None,
            Node::A_Const(c) if c.val().is_none() => None,
            limit => {
                let Some(limit_info) = extract_limit_value(limit) else {
                    return;
                };
                Some(limit_info)
            }
        };

        plan.offset = Some(OffsetPlan {
            limit: Limit {
                limit: limit_info.as_ref().and_then(LimitValueInfo::literal),
                offset: offset_info.literal(),
            },
            limit_param: limit_info.as_ref().map_or(0, LimitValueInfo::param_index),
            offset_param: offset_info.param_index(),
            prepare_execute: false,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::ShardingSchema;
    use crate::backend::schema::Schema;
    use crate::frontend::PreparedStatements;
    use crate::frontend::router::parser::StatementRewriteContext;
    use crate::frontend::router::parser::route::{Route, Shard, ShardWithPriority};
    use crate::net::Parse;
    use crate::net::messages::Query;
    use crate::net::messages::bind::{Bind, Parameter};
    use pgdog_config::Rewrite;

    fn sharded_schema() -> ShardingSchema {
        ShardingSchema {
            shards: 2,
            rewrite: Rewrite {
                enabled: true,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn single_shard_schema() -> ShardingSchema {
        ShardingSchema {
            shards: 1,
            ..Default::default()
        }
    }

    fn cross_shard_route() -> Route {
        Route::select(
            ShardWithPriority::new_table(Shard::All),
            vec![],
            Default::default(),
            Limit::default(),
            None,
        )
    }

    fn single_shard_route() -> Route {
        Route::select(
            ShardWithPriority::new_table(Shard::Direct(0)),
            vec![],
            Default::default(),
            Limit::default(),
            None,
        )
    }

    fn run_limit_offset(sql: &str, schema: &ShardingSchema) -> RewritePlan {
        let stmt = pg_raw_parse::parse(sql).unwrap();
        let db_schema = Schema::default();
        let mut ps = PreparedStatements::default();
        let rewrite = StatementRewrite::new(StatementRewriteContext {
            extended: false,
            prepared: false,
            prepared_statements: &mut ps,
            schema,
            db_schema: &db_schema,
            user: "test",
            search_path: None,
            timezone: None,
            query_timestamps: QueryTimestamps::default(),
        });
        let mut plan = RewritePlan::default();
        rewrite.limit_offset(
            if let Node::SelectStmt(stmt) = stmt.stmts().next().unwrap() {
                stmt
            } else {
                unreachable!("not a select")
            },
            &mut plan,
        );
        plan
    }

    #[test]
    fn test_limit_offset_detection_literals() {
        let plan = run_limit_offset("SELECT * FROM t LIMIT 10 OFFSET 5", &sharded_schema());
        let offset = plan.offset.unwrap();
        assert_eq!(offset.limit.limit, Some(10));
        assert_eq!(offset.limit.offset, Some(5));
    }

    #[test]
    fn test_limit_offset_detection_params() {
        let plan = run_limit_offset("SELECT * FROM t LIMIT $1 OFFSET $2", &sharded_schema());
        let offset = plan.offset.unwrap();
        assert_eq!(offset.limit.limit, None);
        assert_eq!(offset.limit.offset, None);
        assert_eq!(offset.limit_param, 1);
        assert_eq!(offset.offset_param, 2);
    }

    #[test]
    fn test_limit_offset_detection_mixed_limit_literal_offset_param() {
        let plan = run_limit_offset("SELECT * FROM t LIMIT 10 OFFSET $1", &sharded_schema());
        let offset = plan.offset.unwrap();
        assert_eq!(offset.limit.limit, Some(10));
        assert_eq!(offset.limit.offset, None);
        assert_eq!(offset.offset_param, 1);
    }

    #[test]
    fn test_limit_offset_detection_mixed_limit_param_offset_literal() {
        let plan = run_limit_offset("SELECT * FROM t LIMIT $1 OFFSET 5", &sharded_schema());
        let offset = plan.offset.unwrap();
        assert_eq!(offset.limit.limit, None);
        assert_eq!(offset.limit.offset, Some(5));
        assert_eq!(offset.limit_param, 1);
    }

    #[test]
    fn test_limit_offset_skipped_single_shard() {
        let plan = run_limit_offset("SELECT * FROM t LIMIT 10 OFFSET 5", &single_shard_schema());
        assert!(plan.offset.is_none());
    }

    #[test]
    fn test_limit_offset_skipped_no_offset() {
        let plan = run_limit_offset("SELECT * FROM t LIMIT 10", &sharded_schema());
        assert!(plan.offset.is_none());
    }

    #[test]
    fn test_limit_offset_detection_no_limit() {
        for sql in [
            "SELECT * FROM t OFFSET 5",
            "SELECT * FROM t LIMIT ALL OFFSET 5",
            "SELECT * FROM t LIMIT NULL OFFSET 5",
        ] {
            let offset = run_limit_offset(sql, &sharded_schema()).offset.unwrap();
            assert_eq!(offset.limit.limit, None, "{sql}");
            assert_eq!(offset.limit.offset, Some(5), "{sql}");
            assert_eq!(offset.limit_param, 0, "{sql}");
        }

        let offset = run_limit_offset("SELECT * FROM t OFFSET $1", &sharded_schema())
            .offset
            .unwrap();
        assert_eq!(offset.limit.limit, None);
        assert_eq!(offset.limit.offset, None);
        assert_eq!(offset.limit_param, 0);
        assert_eq!(offset.offset_param, 1);
    }

    #[test]
    fn test_rewrite_select_no_limit() {
        for (sql, expected) in [
            (
                "SELECT * FROM t OFFSET 5",
                "SELECT * FROM t LIMIT NULL::bigint + 5::bigint",
            ),
            (
                "SELECT * FROM t LIMIT ALL OFFSET $1",
                "SELECT * FROM t LIMIT NULL::bigint + $1::bigint",
            ),
        ] {
            let stmt = pg_raw_parse::parse(sql).unwrap();
            let Node::SelectStmt(select) = stmt.stmts().next().unwrap() else {
                unreachable!("not a select")
            };
            let rewritten = make::owned(|mem| {
                let mut select = mem.make_unique(select);
                rewrite_select(&mut select.as_mut(), mem);
                select
            });
            assert_eq!(deparse(&*rewritten).unwrap().as_str(), expected);
        }
    }

    #[test]
    fn test_apply_after_route_literals_cross_shard() {
        let plan = OffsetPlan {
            limit: Limit {
                limit: Some(10),
                offset: Some(5),
            },
            limit_param: 0,
            offset_param: 0,
            prepare_execute: false,
        };
        let mut request = ClientRequest::from(vec![ProtocolMessage::Query(Query::new(
            "SELECT * FROM t LIMIT 10 OFFSET 5",
        ))]);
        request.route = Some(cross_shard_route());
        plan.apply_after_route(&mut request).unwrap();

        let query = match &request.messages[0] {
            ProtocolMessage::Query(q) => q.query().to_owned(),
            _ => panic!("expected Query"),
        };
        assert_eq!(query, "SELECT * FROM t LIMIT 10 OFFSET 5");

        let route = request.route.unwrap();
        assert_eq!(route.limit().limit, Some(10));
        assert_eq!(route.limit().offset, Some(5));
    }

    #[test]
    fn test_apply_after_route_params_cross_shard() {
        let plan = OffsetPlan {
            limit: Limit {
                limit: None,
                offset: None,
            },
            limit_param: 1,
            offset_param: 2,
            prepare_execute: false,
        };
        let mut request = ClientRequest::from(vec![ProtocolMessage::Bind(Bind::new_params(
            "",
            &[Parameter::new(b"10"), Parameter::new(b"5")],
        ))]);
        request.route = Some(cross_shard_route());

        plan.apply_after_route(&mut request).unwrap();

        if let ProtocolMessage::Bind(bind) = &request.messages[0] {
            assert_eq!(bind.params_raw()[0].data.as_ref(), b"10");
            assert_eq!(bind.params_raw()[1].data.as_ref(), b"5");
        } else {
            panic!("expected Bind");
        }

        let route = request.route.unwrap();
        assert_eq!(route.limit().limit, Some(10));
        assert_eq!(route.limit().offset, Some(5));
    }

    #[test]
    fn test_apply_after_route_no_limit_offset_param() {
        let plan = OffsetPlan {
            limit: Limit {
                limit: None,
                offset: None,
            },
            limit_param: 0,
            offset_param: 1,
            prepare_execute: false,
        };
        let mut request = ClientRequest::from(vec![ProtocolMessage::Bind(Bind::new_params(
            "",
            &[Parameter::new(b"5")],
        ))]);
        request.route = Some(cross_shard_route());

        plan.apply_after_route(&mut request).unwrap();

        let route = request.route.unwrap();
        assert_eq!(route.limit().limit, None);
        assert_eq!(route.limit().offset, Some(5));
    }

    #[test]
    fn test_apply_after_route_single_shard_noop() {
        let plan = OffsetPlan {
            limit: Limit {
                limit: Some(10),
                offset: Some(5),
            },
            limit_param: 0,
            offset_param: 0,
            prepare_execute: false,
        };
        let mut request = ClientRequest::from(vec![ProtocolMessage::Query(Query::new(
            "SELECT * FROM t LIMIT 10 OFFSET 5",
        ))]);
        request.route = Some(single_shard_route());

        plan.apply_after_route(&mut request).unwrap();

        let query = match &request.messages[0] {
            ProtocolMessage::Query(q) => q.query().to_owned(),
            _ => panic!("expected Query"),
        };
        assert_eq!(query, "SELECT * FROM t LIMIT 10 OFFSET 5");
    }

    #[test]
    fn test_apply_after_route_mixed_limit_literal_offset_param() {
        let plan = OffsetPlan {
            limit: Limit {
                limit: Some(10),
                offset: None,
            },
            limit_param: 0,
            offset_param: 1,
            prepare_execute: false,
        };
        let mut request = ClientRequest::from(vec![
            ProtocolMessage::Parse(Parse::named("s", "SELECT * FROM t LIMIT 10 OFFSET $1")),
            ProtocolMessage::Bind(Bind::new_params("s", &[Parameter::new(b"5")])),
        ]);
        request.route = Some(cross_shard_route());
        plan.apply_after_route(&mut request).unwrap();

        if let ProtocolMessage::Bind(bind) = &request.messages[1] {
            assert_eq!(bind.params_raw()[0].data.as_ref(), b"5");
        } else {
            panic!("expected Bind");
        }

        let sql = match &request.messages[0] {
            ProtocolMessage::Parse(p) => p.query().to_owned(),
            _ => panic!("expected Parse"),
        };
        assert_eq!(sql, "SELECT * FROM t LIMIT 10 OFFSET $1");

        let route = request.route.unwrap();
        assert_eq!(route.limit().limit, Some(10));
        assert_eq!(route.limit().offset, Some(5));
    }
}
