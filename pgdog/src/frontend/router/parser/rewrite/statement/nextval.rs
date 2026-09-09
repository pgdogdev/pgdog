use std::collections::HashMap;

use pg_raw_parse::{ConstValue, Node, make, transform, walk};

use crate::frontend::router::parser::rewrite::ee;

use super::plan::GeneratedId;
use super::{Error, RewritePlan, StatementRewrite};

impl RewritePlan {
    /// Replace simple-protocol sequence calls in the current statement with
    /// freshly fetched bigint literals. The cached plan remains unchanged.
    /// Returns `None` when the plan has no statement.
    pub(crate) async fn rewrite_nextval_simple(&self) -> Result<Option<String>, Error> {
        self.rewrite_nextval_simple_with(ee::nextval).await
    }

    async fn rewrite_nextval_simple_with(
        &self,
        mut nextval: impl AsyncFnMut(&str) -> Result<i64, ee::Error>,
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
                    && let Some(name) = sequence_name(node)
                {
                    calls.push((func.location, name));
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
        for (location, name) in calls {
            values.insert(location, nextval(&name).await?);
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
    pub(super) fn rewrite_nextval<'mem>(
        &mut self,
        node: Node<'_>,
        mem: make::MemoryToken<'mem>,
        next_param: &mut i32,
        plan: &mut RewritePlan,
    ) -> Option<make::Unique<'mem, Node<'mem>>> {
        let sequence = sequence_name(node)?;
        let param = *next_param;
        *next_param += 1;
        plan.generated_ids
            .push((param as u16, GeneratedId::Sequence(sequence)));
        // Retain simple-protocol SQL even when nextval is the only rewrite.
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

/// Extract the literal sequence name from a pgdog.nextval() call.
fn sequence_name(node: Node<'_>) -> Option<String> {
    let Node::FuncCall(func) = node else {
        return None;
    };
    if !func
        .funcname()
        .iter()
        .map(Node::as_str)
        .eq([Some("pgdog"), Some("nextval")])
    {
        return None;
    }

    let mut args = func.args().iter();
    let mut arg = args.next()?;
    if args.next().is_some() {
        return None;
    }

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
    fn test_nextval_extended_parameter_indexes() {
        let (sql, plan) = rewrite(
            "SELECT pgdog.nextval('sequence.name'), $1, pgdog.unique_id(), \
             pgdog.nextval('other.seq'::regclass), pgdog.nextval('sequence.name')",
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
                (2, GeneratedId::Sequence("sequence.name".to_owned())),
                (3, GeneratedId::UniqueId),
                (4, GeneratedId::Sequence("other.seq".to_owned())),
                (5, GeneratedId::Sequence("sequence.name".to_owned())),
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
                (1, GeneratedId::Sequence("sequence.name".to_owned())),
                (2, GeneratedId::Sequence("sequence.name".to_owned())),
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
                    GeneratedId::Sequence("\"My Schema\".\"My Sequence\"".to_owned())
                ),
                (2, GeneratedId::Sequence("other.seq".to_owned())),
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
            .rewrite_nextval_simple_with(async |name: &str| {
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
        let mut nextval = async |_: &str| {
            value += 1;
            Ok(value)
        };
        for (first, second) in [(1, 2), (3, 4)] {
            let sql = plan
                .rewrite_nextval_simple_with(&mut nextval)
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
            .rewrite_nextval_simple_with(async |_: &str| {
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
            plan.rewrite_nextval_simple().await,
            Err(Error::Enterprise(ee::Error::EERequired))
        ));
    }

    #[tokio::test]
    async fn test_nextval_simple_without_calls() {
        assert_eq!(
            RewritePlan::default()
                .rewrite_nextval_simple()
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
            plan.rewrite_nextval_simple()
                .await
                .expect("no global calls"),
            Some(sql.to_owned())
        );
        let invalid = RewritePlan {
            stmt: Some("SELECT (".to_owned()),
            ..Default::default()
        };
        assert!(matches!(
            invalid.rewrite_nextval_simple().await,
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
            plan.apply_generated_ids(&mut bind, async |name: &str| {
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
        let mut nextval = async |_: &str| {
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
    async fn test_nextval_apply_propagates_enterprise_error() {
        let (_, plan) = rewrite("SELECT pgdog.nextval('a')", true);
        let mut request = ClientRequest::from(vec![ProtocolMessage::Bind(Bind::default())]);
        let error = plan
            .apply(&mut request)
            .await
            .expect_err("EE hook rejects sequence");
        assert!(matches!(error, Error::Enterprise(ee::Error::EERequired)));
    }

    #[tokio::test]
    async fn test_nextval_apply_fetches_for_query_but_not_parse() {
        let original = "SELECT pgdog.nextval('a')";
        let (_, extended_plan) = rewrite(original, true);
        let mut request =
            ClientRequest::from(vec![ProtocolMessage::Parse(Parse::new_anonymous(original))]);
        extended_plan
            .apply(&mut request)
            .await
            .expect("prepare does not fetch");

        let (_, simple_plan) = rewrite(original, false);
        let mut request = ClientRequest::from(vec![ProtocolMessage::Query(Query::new(original))]);
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
                sequence_name(select.target_list().first().expect("target").val()),
                Some("sequence.name".to_owned()),
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
                sequence_name(select.target_list().first().expect("target").val()),
                None,
                "{call}"
            );
            let (_, plan) = rewrite(&format!("SELECT {call}"), true);
            assert!(plan.generated_ids.is_empty(), "{call}");
            assert!(plan.is_empty(), "{call}");
        }
    }
}
