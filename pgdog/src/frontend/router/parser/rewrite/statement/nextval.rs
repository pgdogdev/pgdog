use std::collections::HashMap;

use pg_raw_parse::{ConstValue, Node, make, transform, walk};

use crate::frontend::router::parser::rewrite::ee;

use super::plan::GeneratedId;
use super::{Error, RewritePlan, StatementRewrite};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SequenceCall {
    Nextval(String),
    Currval(String),
    Setval {
        name: String,
        value: i64,
        is_called: bool,
    },
}

impl SequenceCall {
    pub(super) async fn execute(&self) -> Result<i64, ee::Error> {
        match self {
            Self::Nextval(name) => ee::nextval(name).await,
            Self::Currval(name) => ee::currval(name).await,
            Self::Setval {
                name,
                value,
                is_called,
            } => ee::setval(name, *value, *is_called).await,
        }
    }
}

impl RewritePlan {
    /// Replace simple-protocol sequence calls in the current statement with
    /// freshly fetched bigint literals. The cached plan remains unchanged.
    /// Returns `None` when the plan has no statement.
    pub(crate) async fn rewrite_sequence_simple(&self) -> Result<Option<String>, Error> {
        self.rewrite_sequence_simple_with(SequenceCall::execute)
            .await
    }

    pub(super) async fn rewrite_sequence_simple_with(
        &self,
        mut execute: impl AsyncFnMut(&SequenceCall) -> Result<i64, ee::Error>,
    ) -> Result<Option<String>, Error> {
        let Some(stmt) = &self.stmt else {
            return Ok(None);
        };

        // Reparse the current SQL to find calls after any earlier rewrites.
        let ast = pg_raw_parse::parse(stmt)?;
        let mut calls = Vec::new();
        for stmt in ast.stmts() {
            walk::walk(stmt, |node| {
                if let Node::FuncCall(func) = node
                    && let Some(call) = sequence_call(node)
                {
                    calls.push((func.location, call));
                }
            });
        }
        if calls.is_empty() {
            return Ok(Some(stmt.clone()));
        }
        calls.sort_unstable_by_key(|(location, _)| *location);

        // AST mutation callbacks are synchronous; resolve values before
        // entering the parser's memory context to replace the calls.
        let mut values = HashMap::with_capacity(calls.len());
        for (location, call) in calls {
            values.insert(location, execute(&call).await?);
        }
        let rewritten = make::owned(|mem| {
            let mut copy = mem.make_unique(&**ast);
            for mut stmt in copy.as_mut() {
                transform::transform_node(
                    stmt.stmt_mut(),
                    &mut transform::TransformClosure::new(|node| {
                        if let Node::FuncCall(func) = node.as_ref()
                            && let Some(value) = values.get(&func.location)
                        {
                            node.replace(
                                mem.make_type_cast(
                                    mem.make_a_const(ConstValue::Float(&value.to_string()))
                                        .uncast(),
                                    mem.make_list(&[
                                        mem.make_string(Some("pg_catalog")),
                                        mem.make_string(Some("int8")),
                                    ]),
                                )
                                .uncast(),
                            );
                            None
                        } else {
                            Some(node)
                        }
                    }),
                );
            }
            copy
        });
        Ok(Some(pg_raw_parse::deparse_stmts(&*rewritten)?))
    }
}

impl StatementRewrite<'_> {
    /// Record a sequence call and return its replacement in extended protocol.
    /// Simple protocol retains the call for asynchronous rewriting.
    pub(super) fn rewrite_sequence<'mem>(
        &mut self,
        node: Node<'_>,
        mem: make::MemoryToken<'mem>,
        next_param: &mut i32,
        plan: &mut RewritePlan,
    ) -> Option<make::Unique<'mem, Node<'mem>>> {
        let sequence = sequence_call(node)?;
        let param = *next_param;
        *next_param += 1;
        plan.generated_ids
            .push((param as u16, GeneratedId::Sequence(sequence)));
        // Retain simple-protocol SQL even when a sequence call is the only rewrite.
        self.rewritten = true;
        self.extended.then(|| {
            mem.make_type_cast(
                mem.make_param_ref(param).uncast(),
                mem.make_list(&[
                    mem.make_string(Some("pg_catalog")),
                    mem.make_string(Some("int8")),
                ]),
            )
            .uncast()
        })
    }
}

/// Recognize pgdog sequence calls with literal arguments.
fn sequence_call(node: Node<'_>) -> Option<SequenceCall> {
    let Node::FuncCall(func) = node else {
        return None;
    };
    let mut names = func.funcname().iter();
    if names.next()?.as_str()? != "pgdog" {
        return None;
    }
    let function = names.next()?.as_str()?;
    if names.next().is_some() {
        return None;
    }

    let mut args = func.args().iter();
    let name = sequence_name(args.next()?)?;
    let call = match function {
        "nextval" => SequenceCall::Nextval(name),
        "currval" => SequenceCall::Currval(name),
        "setval" => {
            let Node::A_Const(value) = args.next()? else {
                return None;
            };
            let is_called = match args.next() {
                None => true,
                Some(Node::A_Const(is_called)) => is_called.val()?.bool_value()?,
                _ => return None,
            };
            SequenceCall::Setval {
                name,
                value: value.val()?.numeric_value::<i64>()?,
                is_called,
            }
        }
        _ => return None,
    };
    if args.next().is_some() {
        return None;
    }
    Some(call)
}

/// Extract a literal sequence name, optionally cast to regclass.
fn sequence_name(mut arg: Node<'_>) -> Option<String> {
    if let Node::TypeCast(cast) = arg {
        let type_name = cast.type_name()?;
        let names = type_name.names();
        if !names.iter().map(|name| name.sval()).eq([Some("regclass")])
            && !names
                .iter()
                .map(|name| name.sval())
                .eq([Some("pg_catalog"), Some("regclass")])
        {
            return None;
        }
        arg = cast.arg();
    }

    let Node::A_Const(value) = arg else {
        return None;
    };
    value.val()?.string_value().map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use crate::backend::{ShardingSchema, schema::Schema};
    use crate::frontend::ClientRequest;
    use crate::frontend::PreparedStatements;
    use crate::frontend::router::parser::StatementRewriteContext;
    use crate::net::messages::bind::{Format, Parameter};
    use crate::net::{Bind, Parse, ProtocolMessage, Query};
    use pgdog_config::Rewrite;

    use super::*;

    fn rewrite(sql: &str, extended: bool) -> (String, RewritePlan) {
        let schema = ShardingSchema {
            rewrite: Rewrite {
                enabled: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let db_schema = Schema::default();
        let mut prepared_statements = PreparedStatements::default();
        let mut rewriter = StatementRewrite::new(StatementRewriteContext {
            extended,
            prepared: false,
            prepared_statements: &mut prepared_statements,
            schema: &schema,
            db_schema: &db_schema,
            user: "test",
            search_path: None,
        });
        let mut plan = RewritePlan::default();
        let ast = make::owned(|mem| {
            let mut ast = mem.parse(sql).expect("valid SQL");
            plan = rewriter
                .maybe_rewrite(ast.as_mut().into_iter().next().expect("statement"), mem)
                .expect("rewrite succeeds");
            ast
        });
        (pg_raw_parse::deparse_stmts(&*ast).expect("valid AST"), plan)
    }

    #[test]
    fn test_sequence_extended_parameter_indexes() {
        let (sql, plan) = rewrite(
            "SELECT pgdog.nextval('sequence.name'), $1, pgdog.unique_id(), \
             pgdog.currval('other.seq'::regclass), pgdog.setval('sequence.name', 42, false)",
            true,
        );
        assert_eq!(
            sql,
            "SELECT $2::bigint, $1, $3::bigint, $4::bigint, $5::bigint"
        );
        assert_eq!(plan.params, 1);
        assert_eq!(plan.unique_ids, 1);
        assert_eq!(
            plan.generated_ids,
            vec![
                (
                    2,
                    GeneratedId::Sequence(SequenceCall::Nextval("sequence.name".to_owned()))
                ),
                (3, GeneratedId::UniqueId),
                (
                    4,
                    GeneratedId::Sequence(SequenceCall::Currval("other.seq".to_owned()))
                ),
                (
                    5,
                    GeneratedId::Sequence(SequenceCall::Setval {
                        name: "sequence.name".to_owned(),
                        value: 42,
                        is_called: false,
                    })
                ),
            ]
        );
        assert_eq!(plan.stmt.as_deref(), Some(sql.as_str()));
    }

    #[test]
    fn test_nextval_simple_records_calls_without_placeholders() {
        let original = "SELECT pgdog.nextval('sequence.name'), \
                        pgdog.nextval('sequence.name'::regclass)";
        let (sql, plan) = rewrite(original, false);
        assert_eq!(sql, original);
        assert_eq!(
            plan.generated_ids,
            vec![
                (
                    1,
                    GeneratedId::Sequence(SequenceCall::Nextval("sequence.name".to_owned()))
                ),
                (
                    2,
                    GeneratedId::Sequence(SequenceCall::Nextval("sequence.name".to_owned()))
                ),
            ]
        );
        assert_eq!(plan.unique_ids, 0);
        assert_eq!(plan.stmt.as_deref(), Some(original));
        assert!(!plan.is_empty());
    }

    #[test]
    fn test_nextval_insert_preserves_quoted_sequence_name() {
        let (sql, plan) = rewrite(
            "INSERT INTO t (id) VALUES \
             (pgdog.nextval('\"My Schema\".\"My Sequence\"'::pg_catalog.regclass)), \
             (pgdog.nextval('other.seq'))",
            true,
        );
        assert_eq!(sql, "INSERT INTO t (id) VALUES ($1::bigint), ($2::bigint)");
        assert_eq!(
            plan.generated_ids,
            vec![
                (
                    1,
                    GeneratedId::Sequence(SequenceCall::Nextval(
                        "\"My Schema\".\"My Sequence\"".to_owned()
                    ))
                ),
                (
                    2,
                    GeneratedId::Sequence(SequenceCall::Nextval("other.seq".to_owned()))
                ),
            ]
        );
    }

    #[tokio::test]
    async fn test_nextval_simple_rewrites_current_statement() {
        let (_, mut plan) = rewrite("SELECT pgdog.nextval('old')", false);
        // The current statement has different locations, names, and call count
        // than the original SQL.
        plan.stmt = Some(
            "SELECT 123::bigint, pgdog.nextval('a'::regclass), \
             (SELECT pgdog.nextval('b')), pgdog.nextval('a'::pg_catalog.regclass), \
             nextval('local'), 'pgdog.nextval(''literal'')'"
                .to_owned(),
        );
        let before = plan.stmt.clone();
        let mut names = Vec::new();
        let mut values = [i64::MIN, 0, i64::MAX].into_iter();
        let sql = plan
            .rewrite_sequence_simple_with(async |call: &SequenceCall| {
                let SequenceCall::Nextval(name) = call else {
                    panic!("expected nextval");
                };
                names.push(name.to_owned());
                Ok(values.next().expect("one value per call"))
            })
            .await
            .expect("rewrite succeeds")
            .expect("statement");
        assert_eq!(names, ["a", "b", "a"]);
        assert_eq!(
            sql,
            "SELECT 123::bigint, (-9223372036854775808)::bigint, \
                         (SELECT (0)::bigint), (9223372036854775807)::bigint, \
                         nextval('local'), 'pgdog.nextval(''literal'')'"
        );
        assert_eq!(plan.stmt, before);
        pg_raw_parse::parse(&sql).expect("rewritten SQL parses");
    }

    #[tokio::test]
    async fn test_nextval_simple_fetches_fresh_values() {
        let (_, plan) = rewrite(
            "INSERT INTO t (id) VALUES (pgdog.nextval('a')), (pgdog.nextval('a'))",
            false,
        );
        let mut value = 0i64;
        let mut nextval = async |_: &SequenceCall| {
            value += 1;
            Ok(value)
        };
        for (first, second) in [(1, 2), (3, 4)] {
            let sql = plan
                .rewrite_sequence_simple_with(&mut nextval)
                .await
                .expect("rewrite succeeds")
                .expect("statement");
            assert_eq!(
                sql,
                format!("INSERT INTO t (id) VALUES (({first})::bigint), (({second})::bigint)")
            );
        }
    }

    #[tokio::test]
    async fn test_nextval_simple_error_preserves_statement() {
        let (_, plan) = rewrite("SELECT pgdog.nextval('a'), pgdog.nextval('b')", false);
        let before = plan.stmt.clone();
        let mut calls = 0;
        let error = plan
            .rewrite_sequence_simple_with(async |_: &SequenceCall| {
                calls += 1;
                if calls == 1 {
                    Ok(42)
                } else {
                    Err(ee::Error::EERequired)
                }
            })
            .await
            .expect_err("second fetch fails");
        assert!(matches!(error, Error::Enterprise(ee::Error::EERequired)));
        assert_eq!(plan.stmt, before);
        assert!(matches!(
            plan.rewrite_sequence_simple().await,
            Err(Error::Enterprise(ee::Error::EERequired))
        ));
    }

    #[tokio::test]
    async fn test_nextval_simple_without_calls() {
        assert_eq!(
            RewritePlan::default()
                .rewrite_sequence_simple()
                .await
                .expect("no SQL"),
            None
        );
        let sql = "SELECT  $1::bigint, nextval('local') /* keep formatting */";
        let plan = RewritePlan {
            stmt: Some(sql.to_owned()),
            ..Default::default()
        };
        assert_eq!(
            plan.rewrite_sequence_simple()
                .await
                .expect("no global calls"),
            Some(sql.to_owned())
        );
        let invalid = RewritePlan {
            stmt: Some("SELECT (".to_owned()),
            ..Default::default()
        };
        assert!(matches!(
            invalid.rewrite_sequence_simple().await,
            Err(Error::Parser(_))
        ));
    }

    #[tokio::test]
    async fn test_nextval_bind_values_and_formats() {
        let _guard = crate::test_utils::set_env_var("NODE_ID", "pgdog-1");
        let (_, plan) = rewrite(
            "SELECT $1, $2, pgdog.nextval('a'), pgdog.unique_id(), \
             pgdog.nextval('b'), pgdog.unique_id(), pgdog.nextval('a')",
            true,
        );
        for codes in [
            vec![],
            vec![Format::Binary],
            vec![Format::Text, Format::Binary],
        ] {
            let original_params = [
                Parameter::new(b"client"),
                Parameter::new(&7i64.to_be_bytes()),
            ];
            let mut bind = Bind::new_params_codes("stmt", &original_params, &codes);
            let mut calls = Vec::new();
            let mut value = -2i64;
            plan.apply_generated_ids(&mut bind, async |call: &SequenceCall| {
                let SequenceCall::Nextval(name) = call else {
                    panic!("expected nextval");
                };
                calls.push(name.to_owned());
                value += 1;
                Ok(value)
            })
            .await
            .expect("sequence values appended");

            assert_eq!(calls, ["a", "b", "a"]);
            assert_eq!(&bind.params_raw()[..2], &original_params);
            assert_eq!(bind.params_raw().len(), 7);
            for (index, value) in [(2, -1), (4, 0), (6, 1)] {
                let param = bind.parameter(index).expect("format").expect("parameter");
                assert_eq!(param.bigint(), Some(value));
                assert_eq!(
                    param.format(),
                    if codes.len() == 1 {
                        Format::Binary
                    } else {
                        Format::Text
                    }
                );
            }
            let first_id = bind
                .parameter(3)
                .expect("format")
                .expect("parameter")
                .bigint()
                .expect("bigint");
            let second_id = bind
                .parameter(5)
                .expect("format")
                .expect("parameter")
                .bigint()
                .expect("bigint");
            assert!(first_id > 0);
            assert!(second_id > first_id);
            if codes.len() <= 1 {
                assert_eq!(bind.format_codes_raw(), codes);
            } else {
                assert_eq!(
                    bind.format_codes_raw(),
                    [
                        Format::Text,
                        Format::Binary,
                        Format::Text,
                        Format::Text,
                        Format::Text,
                        Format::Text,
                        Format::Text
                    ]
                );
            }
        }
    }

    #[tokio::test]
    async fn test_nextval_bind_reexecution_fetches_new_values() {
        let (_, plan) = rewrite("SELECT pgdog.nextval('a'), pgdog.nextval('a')", true);
        let mut value = 0i64;
        let mut nextval = async |_: &SequenceCall| {
            value += 1;
            Ok(value)
        };
        for expected in [2, 4] {
            let mut bind = Bind::default();
            plan.apply_generated_ids(&mut bind, &mut nextval)
                .await
                .expect("values");
            assert_eq!(
                bind.parameter(1)
                    .expect("format")
                    .expect("parameter")
                    .bigint(),
                Some(expected)
            );
        }
    }

    #[tokio::test]
    async fn test_sequence_apply_propagates_enterprise_error() {
        for call in [
            "nextval('a')",
            "currval('a')",
            "setval('a', 42)",
            "setval('a', 42, true)",
        ] {
            let (_, plan) = rewrite(&format!("SELECT pgdog.{call}"), true);
            let mut request = ClientRequest::from(vec![ProtocolMessage::Bind(Bind::default())]);
            let error = plan
                .apply(&mut request)
                .await
                .expect_err("EE hook rejects sequence");
            assert!(matches!(error, Error::Enterprise(ee::Error::EERequired)));
        }
    }

    #[tokio::test]
    async fn test_sequence_apply_fetches_for_query_but_not_parse() {
        for call in [
            "nextval('a')",
            "currval('a')",
            "setval('a', 42)",
            "setval('a', 42, true)",
        ] {
            let original = format!("SELECT pgdog.{call}");
            let (_, extended_plan) = rewrite(&original, true);
            let mut request = ClientRequest::from(vec![ProtocolMessage::Parse(
                Parse::new_anonymous(&original),
            )]);
            extended_plan
                .apply(&mut request)
                .await
                .expect("prepare does not fetch");

            let (_, simple_plan) = rewrite(&original, false);
            let mut request =
                ClientRequest::from(vec![ProtocolMessage::Query(Query::new(&original))]);
            let error = simple_plan
                .apply(&mut request)
                .await
                .expect_err("simple query calls the EE hook");
            assert!(matches!(error, Error::Enterprise(ee::Error::EERequired)));
            let ProtocolMessage::Query(query) = &request.messages[0] else {
                panic!("expected Query");
            };
            assert_eq!(query.query(), original);
        }
    }

    #[test]
    fn test_sequence_literal_arguments() {
        let name = "\"My Schema\".\"My Sequence\"";
        for argument in [
            format!("'{name}'"),
            format!("'{name}'::regclass"),
            format!("'{name}'::pg_catalog.regclass"),
            format!("CAST('{name}' AS regclass)"),
        ] {
            let mut cases = vec![(
                format!("pgdog.currval({argument})"),
                SequenceCall::Currval(name.to_owned()),
            )];
            for value in [i64::MIN, -2147483649, -1, 0, 2147483648, i64::MAX] {
                for is_called in [None, Some(false), Some(true)] {
                    cases.push((
                        match is_called {
                            Some(is_called) => {
                                format!("pgdog.setval({argument}, {value}, {is_called})")
                            }
                            None => format!("pgdog.setval({argument}, {value})"),
                        },
                        SequenceCall::Setval {
                            name: name.to_owned(),
                            value,
                            is_called: is_called.unwrap_or(true),
                        },
                    ));
                }
            }
            for (call, expected) in cases {
                for extended in [false, true] {
                    let original = format!("SELECT {call}");
                    let (sql, plan) = rewrite(&original, extended);
                    assert_eq!(
                        plan.generated_ids,
                        [(1, GeneratedId::Sequence(expected.clone()))],
                        "{call}"
                    );
                    let canonical = original.replace(
                        &format!("CAST('{name}' AS regclass)"),
                        &format!("'{name}'::regclass"),
                    );
                    assert_eq!(
                        sql,
                        if extended {
                            "SELECT $1::bigint"
                        } else {
                            &canonical
                        },
                        "{call}"
                    );
                    assert_eq!(plan.stmt.as_deref(), Some(sql.as_str()));
                }
            }
        }
    }

    #[tokio::test]
    async fn test_sequence_mixed_calls_reexecute_in_order() {
        let original = "SELECT pgdog.nextval('a'), (SELECT pgdog.currval('a')), \
                        pgdog.setval('a', -42, false), pgdog.nextval('a'), \
                        pgdog.setval('a', 42), pgdog.nextval('a')";
        let expected_calls = [
            SequenceCall::Nextval("a".to_owned()),
            SequenceCall::Currval("a".to_owned()),
            SequenceCall::Setval {
                name: "a".to_owned(),
                value: -42,
                is_called: false,
            },
            SequenceCall::Nextval("a".to_owned()),
            SequenceCall::Setval {
                name: "a".to_owned(),
                value: 42,
                is_called: true,
            },
            SequenceCall::Nextval("a".to_owned()),
        ];
        for extended in [false, true] {
            let (sql, plan) = rewrite(original, extended);
            if extended {
                assert_eq!(
                    sql,
                    "SELECT $1::bigint, (SELECT $2::bigint), $3::bigint, \
                                 $4::bigint, $5::bigint, $6::bigint"
                );
            }
            let before = plan.stmt.clone();
            let mut calls = Vec::new();
            let mut current = 0i64;
            let mut called = true;
            let mut execute = async |call: &SequenceCall| {
                calls.push(call.clone());
                match call {
                    SequenceCall::Nextval(_) => {
                        if called {
                            current += 1;
                        }
                        called = true;
                        Ok(current)
                    }
                    SequenceCall::Currval(_) => Ok(current),
                    SequenceCall::Setval {
                        value, is_called, ..
                    } => {
                        current = *value;
                        called = *is_called;
                        Ok(*value)
                    }
                }
            };
            for first in [1, 44] {
                if extended {
                    let mut bind = Bind::default();
                    plan.apply_generated_ids(&mut bind, &mut execute)
                        .await
                        .expect("values appended");
                    assert_eq!(bind.params_raw().len(), 6);
                    for (index, value) in [first, first, -42, -42, 42, 43].into_iter().enumerate() {
                        assert_eq!(
                            bind.parameter(index)
                                .expect("format")
                                .expect("parameter")
                                .bigint(),
                            Some(value)
                        );
                    }
                } else {
                    let sql = plan
                        .rewrite_sequence_simple_with(&mut execute)
                        .await
                        .expect("rewrite succeeds")
                        .expect("statement");
                    assert_eq!(
                        sql,
                        format!(
                            "SELECT ({first})::bigint, (SELECT ({first})::bigint), \
                                            (-42)::bigint, (-42)::bigint, (42)::bigint, (43)::bigint"
                        )
                    );
                }
                assert_eq!(plan.stmt, before);
            }
            assert_eq!(calls.len(), expected_calls.len() * 2);
            for execution in calls.chunks(expected_calls.len()) {
                assert_eq!(execution, expected_calls);
            }
        }
    }

    #[test]
    fn test_sequence_unsupported_arguments() {
        for call in [
            "currval('seq')",
            "setval('seq', 42, true)",
            "other.currval('seq')",
            "other.setval('seq', 42, true)",
            "pgdog.currval()",
            "pgdog.currval('seq', 42)",
            "pgdog.currval($1)",
            "pgdog.currval(123)",
            "pgdog.currval(NULL)",
            "pgdog.currval('seq'::text)",
            "pgdog.currval('seq'::other.regclass)",
            "pgdog.setval()",
            "pgdog.setval('seq')",
            "pgdog.setval('seq', 42, true, false)",
            "pgdog.setval($1, 42, true)",
            "pgdog.setval(NULL, 42, true)",
            "pgdog.setval('seq'::text, 42, true)",
            "pgdog.setval('seq', $1, true)",
            "pgdog.setval('seq', 1 + 2, true)",
            "pgdog.setval('seq', NULL, true)",
            "pgdog.setval('seq', '42', true)",
            "pgdog.setval('seq', 1.5, true)",
            "pgdog.setval('seq', 1e2, true)",
            "pgdog.setval('seq', 9223372036854775808, true)",
            "pgdog.setval('seq', -9223372036854775809, true)",
            "pgdog.setval('seq', 42, $1)",
            "pgdog.setval('seq', 42, NULL)",
            "pgdog.setval('seq', 42, 'true')",
            "pgdog.setval('seq', 42, 1)",
            "pgdog.setval('seq', 42, 1 = 1)",
        ] {
            for extended in [false, true] {
                let (_, plan) = rewrite(&format!("SELECT {call}"), extended);
                assert!(plan.generated_ids.is_empty(), "{call}");
                assert!(plan.is_empty(), "{call}");
            }
        }
    }

    #[test]
    fn test_nextval_sequence_name() {
        for argument in [
            "'sequence.name'",
            "'sequence.name'::regclass",
            "'sequence.name'::pg_catalog.regclass",
            "CAST('sequence.name' AS regclass)",
        ] {
            let ast = pg_raw_parse::parse(&format!("SELECT pgdog.nextval({argument})"))
                .expect("valid SQL");
            let Node::SelectStmt(select) = ast.stmts().next().expect("statement") else {
                panic!("expected SELECT");
            };
            assert_eq!(
                sequence_call(select.target_list().first().expect("target").val()),
                Some(SequenceCall::Nextval("sequence.name".to_owned())),
                "{argument}"
            );
        }
    }

    #[test]
    fn test_nextval_unsupported_calls() {
        for call in [
            "nextval('seq')",
            "other.nextval('seq')",
            "pgdog.other('seq')",
            "pgdog.nextval()",
            "pgdog.nextval('seq', 'other')",
            "pgdog.nextval($1)",
            "pgdog.nextval(123)",
            "pgdog.nextval(NULL)",
            "pgdog.nextval('seq'::text)",
            "pgdog.nextval('seq'::other.regclass)",
        ] {
            let ast = pg_raw_parse::parse(&format!("SELECT {call}")).expect("valid SQL");
            let Node::SelectStmt(select) = ast.stmts().next().expect("statement") else {
                panic!("expected SELECT");
            };
            assert_eq!(
                sequence_call(select.target_list().first().expect("target").val()),
                None,
                "{call}"
            );
            let (_, plan) = rewrite(&format!("SELECT {call}"), true);
            assert!(plan.generated_ids.is_empty(), "{call}");
            assert!(plan.is_empty(), "{call}");
        }
    }
}
