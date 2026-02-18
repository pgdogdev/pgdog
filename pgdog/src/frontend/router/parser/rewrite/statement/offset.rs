use pg_query::protobuf::{a_const::Val, AConst, Integer, ParamRef, ParseResult};
use pg_query::NodeEnum;

use crate::frontend::router::parser::Limit;
use crate::frontend::ClientRequest;
use crate::net::messages::bind::{Format, Parameter};
use crate::net::ProtocolMessage;

use super::*;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OffsetPlan {
    pub(crate) limit: Limit,
    pub(crate) limit_param: usize,
    pub(crate) offset_param: usize,
}

impl OffsetPlan {
    pub(super) fn apply_after_parser(&self, request: &mut ClientRequest) -> Result<(), Error> {
        let route = match request.route.as_mut() {
            Some(route) => route,
            None => return Ok(()),
        };

        if !route.is_cross_shard() {
            return Ok(());
        }

        // Resolve actual values: use literal if known, otherwise read from Bind.
        let mut limit_val = self.limit.limit;
        let mut offset_val = self.limit.offset;

        for message in request.messages.iter_mut() {
            if let ProtocolMessage::Bind(bind) = message {
                if limit_val.is_none() {
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

                let new_limit = limit_val.unwrap_or(0) + offset_val.unwrap_or(0);

                // Overwrite parameterized limit.
                if self.limit.limit.is_none() {
                    let idx = self.limit_param - 1;
                    let fmt = bind.parameter_format(idx)?;
                    let param = match fmt {
                        Format::Binary => Parameter::new(&(new_limit as i64).to_be_bytes()),
                        Format::Text => Parameter::new(new_limit.to_string().as_bytes()),
                    };
                    bind.set_param(idx, param);
                }

                // Overwrite parameterized offset.
                if self.limit.offset.is_none() {
                    let idx = self.offset_param - 1;
                    let fmt = bind.parameter_format(idx)?;
                    let param = match fmt {
                        Format::Binary => Parameter::new(&0i64.to_be_bytes()),
                        Format::Text => Parameter::new(b"0"),
                    };
                    bind.set_param(idx, param);
                }
                break;
            }
        }

        // Rewrite SQL if any value was a literal.
        if self.limit.limit.is_some() || self.limit.offset.is_some() {
            let new_limit = (limit_val.unwrap_or(0) + offset_val.unwrap_or(0)) as i32;
            let ast = request.ast.as_ref().ok_or(Error::MissingAst)?;
            let mut protobuf = ast.ast.protobuf.clone();
            if rewrite_ast_limit_offset(&mut protobuf, new_limit) {
                let result = pg_query::ParseResult::new(protobuf, "".into());
                let new_sql = result.deparse()?;
                for message in request.messages.iter_mut() {
                    match message {
                        ProtocolMessage::Query(q) => q.set_query(&new_sql),
                        ProtocolMessage::Parse(p) => p.set_query(&new_sql),
                        _ => {}
                    }
                }
            }
        }

        route.set_limit(Limit {
            limit: limit_val,
            offset: offset_val,
        });

        Ok(())
    }
}

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

fn extract_limit_value(node: &Option<pg_query::NodeEnum>) -> Option<LimitValueInfo> {
    match node {
        Some(NodeEnum::AConst(AConst {
            val: Some(Val::Ival(Integer { ival })),
            ..
        })) => Some(LimitValueInfo::Literal(*ival as usize)),
        Some(NodeEnum::ParamRef(ParamRef { number, .. })) => {
            Some(LimitValueInfo::Param(*number as usize))
        }
        _ => None,
    }
}

fn rewrite_ast_limit_offset(ast: &mut ParseResult, new_limit: i32) -> bool {
    let raw_stmt = match ast.stmts.first_mut() {
        Some(s) => s,
        None => return false,
    };
    let stmt = match raw_stmt.stmt.as_mut() {
        Some(s) => s,
        None => return false,
    };
    let select = match &mut stmt.node {
        Some(NodeEnum::SelectStmt(s)) => s,
        _ => return false,
    };

    select.limit_count = Some(Box::new(pg_query::Node {
        node: Some(NodeEnum::AConst(AConst {
            val: Some(Val::Ival(Integer { ival: new_limit })),
            isnull: false,
            location: -1i32,
        })),
    }));

    select.limit_offset = Some(Box::new(pg_query::Node {
        node: Some(NodeEnum::AConst(AConst {
            val: Some(Val::Ival(Integer { ival: 0 })),
            isnull: false,
            location: -1i32,
        })),
    }));

    true
}

impl StatementRewrite<'_> {
    pub(super) fn limit_offset(&mut self, plan: &mut RewritePlan) -> Result<(), Error> {
        if self.schema.shards <= 1 {
            return Ok(());
        }

        let raw_stmt = match self.stmt.stmts.first() {
            Some(s) => s,
            None => return Ok(()),
        };
        let stmt = match raw_stmt.stmt.as_ref() {
            Some(s) => s,
            None => return Ok(()),
        };
        let select = match &stmt.node {
            Some(NodeEnum::SelectStmt(s)) => s,
            _ => return Ok(()),
        };

        let offset_node = match &select.limit_offset {
            Some(node) => node,
            None => return Ok(()),
        };
        let limit_node = match &select.limit_count {
            Some(node) => node,
            None => return Ok(()),
        };

        let limit_info = extract_limit_value(&limit_node.node);
        let offset_info = extract_limit_value(&offset_node.node);

        let (limit_info, offset_info) = match (limit_info, offset_info) {
            (Some(l), Some(o)) => (l, o),
            _ => return Ok(()),
        };

        plan.offset = Some(OffsetPlan {
            limit: Limit {
                limit: limit_info.literal(),
                offset: offset_info.literal(),
            },
            limit_param: limit_info.param_index(),
            offset_param: offset_info.param_index(),
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::schema::Schema;
    use crate::backend::ShardingSchema;
    use crate::frontend::router::parser::cache::ast::Ast;
    use crate::frontend::router::parser::route::{Route, Shard, ShardWithPriority};
    use crate::frontend::router::parser::StatementRewriteContext;
    use crate::frontend::PreparedStatements;
    use crate::net::messages::bind::{Bind, Parameter};
    use crate::net::messages::Query;
    use crate::net::Parse;
    use pgdog_config::{QueryParserEngine, Rewrite};

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

    fn make_ast(sql: &str) -> Ast {
        Ast::new_record(sql, QueryParserEngine::PgQueryProtobuf).unwrap()
    }

    fn run_limit_offset(sql: &str, schema: &ShardingSchema) -> RewritePlan {
        let mut ast = pg_query::parse(sql).unwrap();
        let db_schema = Schema::default();
        let mut ps = PreparedStatements::default();
        let mut rewrite = StatementRewrite::new(StatementRewriteContext {
            stmt: &mut ast.protobuf,
            extended: false,
            prepared: false,
            prepared_statements: &mut ps,
            schema,
            db_schema: &db_schema,
            user: "test",
            search_path: None,
        });
        let mut plan = RewritePlan::default();
        rewrite.limit_offset(&mut plan).unwrap();
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
    fn test_limit_offset_skipped_no_limit() {
        let plan = run_limit_offset("SELECT * FROM t OFFSET 5", &sharded_schema());
        assert!(plan.offset.is_none());
    }

    #[test]
    fn test_apply_after_parser_literals_cross_shard() {
        let plan = OffsetPlan {
            limit: Limit {
                limit: Some(10),
                offset: Some(5),
            },
            limit_param: 0,
            offset_param: 0,
        };
        let mut request = ClientRequest::from(vec![ProtocolMessage::Query(Query::new(
            "SELECT * FROM t LIMIT 10 OFFSET 5",
        ))]);
        request.route = Some(cross_shard_route());
        request.ast = Some(make_ast("SELECT * FROM t LIMIT 10 OFFSET 5"));

        plan.apply_after_parser(&mut request).unwrap();

        let query = match &request.messages[0] {
            ProtocolMessage::Query(q) => q.query().to_owned(),
            _ => panic!("expected Query"),
        };
        assert_eq!(query, "SELECT * FROM t LIMIT 15 OFFSET 0");

        let route = request.route.unwrap();
        assert_eq!(route.limit().limit, Some(10));
        assert_eq!(route.limit().offset, Some(5));
    }

    #[test]
    fn test_apply_after_parser_params_cross_shard() {
        let plan = OffsetPlan {
            limit: Limit {
                limit: None,
                offset: None,
            },
            limit_param: 1,
            offset_param: 2,
        };
        let mut request = ClientRequest::from(vec![ProtocolMessage::Bind(Bind::new_params(
            "",
            &[Parameter::new(b"10"), Parameter::new(b"5")],
        ))]);
        request.route = Some(cross_shard_route());

        plan.apply_after_parser(&mut request).unwrap();

        if let ProtocolMessage::Bind(bind) = &request.messages[0] {
            assert_eq!(bind.params_raw()[0].data.as_ref(), b"15");
            assert_eq!(bind.params_raw()[1].data.as_ref(), b"0");
        } else {
            panic!("expected Bind");
        }

        let route = request.route.unwrap();
        assert_eq!(route.limit().limit, Some(10));
        assert_eq!(route.limit().offset, Some(5));
    }

    #[test]
    fn test_apply_after_parser_single_shard_noop() {
        let plan = OffsetPlan {
            limit: Limit {
                limit: Some(10),
                offset: Some(5),
            },
            limit_param: 0,
            offset_param: 0,
        };
        let mut request = ClientRequest::from(vec![ProtocolMessage::Query(Query::new(
            "SELECT * FROM t LIMIT 10 OFFSET 5",
        ))]);
        request.route = Some(single_shard_route());

        plan.apply_after_parser(&mut request).unwrap();

        let query = match &request.messages[0] {
            ProtocolMessage::Query(q) => q.query().to_owned(),
            _ => panic!("expected Query"),
        };
        assert_eq!(query, "SELECT * FROM t LIMIT 10 OFFSET 5");
    }

    #[test]
    fn test_apply_after_parser_mixed_limit_literal_offset_param() {
        let plan = OffsetPlan {
            limit: Limit {
                limit: Some(10),
                offset: None,
            },
            limit_param: 0,
            offset_param: 1,
        };
        let mut request = ClientRequest::from(vec![
            ProtocolMessage::Parse(Parse::named("s", "SELECT * FROM t LIMIT 10 OFFSET $1")),
            ProtocolMessage::Bind(Bind::new_params("s", &[Parameter::new(b"5")])),
        ]);
        request.route = Some(cross_shard_route());
        request.ast = Some(make_ast("SELECT * FROM t LIMIT 10 OFFSET $1"));

        plan.apply_after_parser(&mut request).unwrap();

        if let ProtocolMessage::Bind(bind) = &request.messages[1] {
            assert_eq!(bind.params_raw()[0].data.as_ref(), b"0");
        } else {
            panic!("expected Bind");
        }

        let sql = match &request.messages[0] {
            ProtocolMessage::Parse(p) => p.query().to_owned(),
            _ => panic!("expected Parse"),
        };
        assert_eq!(sql, "SELECT * FROM t LIMIT 15 OFFSET 0");

        let route = request.route.unwrap();
        assert_eq!(route.limit().limit, Some(10));
        assert_eq!(route.limit().offset, Some(5));
    }
}
