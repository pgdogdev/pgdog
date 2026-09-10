use super::super::nextval::SequenceCall;
use super::super::plan::RewriteResult;
use super::tests::make_schema_with_bigint_pk;
use super::*;
use crate::backend::ShardingSchema;
use crate::frontend::router::parser::StatementRewriteContext;
use crate::frontend::{ClientRequest, PreparedStatements};
use crate::net::messages::bind::{Format, Parameter};
use crate::net::{Bind, Parse, ProtocolMessage, Query};
use pgdog_config::Rewrite;

fn split_plan(sql: &str, extended: bool, prepared: bool) -> RewritePlan {
    let schema = ShardingSchema {
        shards: 3,
        rewrite: Rewrite {
            enabled: true,
            primary_key: RewriteMode::RewriteOmniGlobal,
            split_inserts: RewriteMode::Rewrite,
            ..Default::default()
        },
        ..Default::default()
    };
    let db_schema = make_schema_with_bigint_pk();
    let mut prepared_statements = PreparedStatements::new();
    let mut rewriter = StatementRewrite::new(StatementRewriteContext {
        extended,
        prepared,
        prepared_statements: &mut prepared_statements,
        schema: &schema,
        db_schema: &db_schema,
        user: "",
        search_path: None,
    });
    let mut plan = RewritePlan::default();
    make::owned(|mem| {
        let mut ast = mem.parse(sql).expect("valid SQL");
        plan = rewriter
            .maybe_rewrite(ast.as_mut().into_iter().next().expect("statement"), mem)
            .expect("rewrite succeeds");
        ast
    });
    assert_eq!(plan.insert_split.len(), 2);
    plan
}

#[tokio::test]
async fn test_nextval_auto_id_simple_splits_use_resolved_values() {
    for (columns, values) in [
        ("name", "('a'), ('b')"),
        ("name, id", "('a', DEFAULT), ('b', DEFAULT)"),
        (
            "name, id",
            "('a', pgdog.nextval('users_id_seq')), ('b', pgdog.nextval('users_id_seq'))",
        ),
    ] {
        let original = format!(
            "INSERT INTO users ({columns}) VALUES {values} ON CONFLICT (id) DO NOTHING RETURNING id"
        );
        let plan = split_plan(&original, false, false);
        let before = plan.stmt.clone();
        let mut calls = Vec::new();
        for first in [101, 103] {
            let mut value = first;
            let sql = plan
                .rewrite_sequence_simple_with(async |call: &SequenceCall| {
                    calls.push(call.clone());
                    let result = value;
                    value += 1;
                    Ok(result)
                })
                .await
                .expect("sequence rewrite succeeds")
                .expect("rewritten SQL");
            let request = ClientRequest::from(vec![ProtocolMessage::Query(Query::new(&sql))]);
            let result = plan.apply_after_messages(&request).expect("split succeeds");
            let RewriteResult::InsertSplit(splits) = result else {
                panic!("expected insert splits");
            };
            assert_eq!(value, first + 2, "allocate exactly once per row");
            let ProtocolMessage::Query(parent) = &request.messages[0] else {
                panic!("expected query");
            };
            assert!(!parent.query().contains("pgdog.nextval"));
            assert_eq!(splits.len(), 2);
            for (split, (name, id)) in splits.iter().zip([("a", first), ("b", first + 1)]) {
                let expected = format!(
                    "INSERT INTO users (name, id) VALUES ('{name}', {id}::bigint) \
                     ON CONFLICT (id) DO NOTHING RETURNING id"
                );
                let ProtocolMessage::Query(query) = &split.messages[0] else {
                    panic!("expected query");
                };
                assert_eq!(query.query(), expected);
                assert!(
                    parent
                        .query()
                        .contains(&format!("('{name}', ({id})::bigint)"))
                );
                let ast = split.ast.as_ref().expect("routing AST");
                assert_eq!(
                    pg_raw_parse::deparse_stmts(&*ast.ast).expect("SQL"),
                    expected
                );
                assert!(ast.rewrite_plan.is_empty(), "IDs already resolved");
            }
            assert_eq!(plan.stmt, before, "cached plan stays reusable");
        }
        assert_eq!(calls, vec![SequenceCall::Nextval("users_id_seq".into()); 4]);
    }
}

#[tokio::test]
async fn test_nextval_auto_id_extended_splits_keep_generated_parameters() {
    for prepared in [false, true] {
        for format in [Format::Text, Format::Binary] {
            for (columns, values) in [
                ("name", "($1), ($2)"),
                ("name, id", "($1, DEFAULT), ($2, DEFAULT)"),
                (
                    "name, id",
                    "($1, pgdog.nextval('users_id_seq')), ($2, pgdog.nextval('users_id_seq'))",
                ),
            ] {
                let original =
                    format!("INSERT INTO users ({columns}) VALUES {values} RETURNING id");
                let plan = split_plan(&original, true, prepared);
                let parse = Parse::named(if prepared { "auto_id_split" } else { "" }, &original);
                let mut prepare_request =
                    ClientRequest::from(vec![ProtocolMessage::Parse(parse.clone())]);
                let result = plan
                    .apply(&mut prepare_request)
                    .await
                    .expect("prepare succeeds");
                assert!(matches!(result, RewriteResult::InPlace { .. }));

                for separate_parse in [false, true] {
                    let mut bind = Bind::new_params_codes(
                        parse.name(),
                        &[Parameter::new(b"a"), Parameter::new(b"b")],
                        &[format],
                    );
                    let mut value = 200;
                    plan.apply_generated_ids(&mut bind, async |call: &SequenceCall| {
                        assert_eq!(call, &SequenceCall::Nextval("users_id_seq".into()));
                        value += 1;
                        Ok(value)
                    })
                    .await
                    .expect("sequence values appended");
                    let request = if separate_parse {
                        let mut request = ClientRequest::from(vec![ProtocolMessage::Bind(bind)]);
                        request.last_parse = Some(parse.clone());
                        request
                    } else {
                        ClientRequest::from(vec![
                            ProtocolMessage::Parse(parse.clone()),
                            ProtocolMessage::Bind(bind),
                        ])
                    };
                    let result = plan.apply_after_messages(&request).expect("split succeeds");
                    assert_eq!(value, 202);
                    let RewriteResult::InsertSplit(splits) = result else {
                        panic!("expected insert splits");
                    };
                    for (split, (name, id)) in splits.iter().zip([(b"a", 201), (b"b", 202)]) {
                        let query = split.query().expect("query lookup").expect("query");
                        assert_eq!(
                            query.query(),
                            "INSERT INTO users (name, id) VALUES ($1, $2::bigint) RETURNING id"
                        );
                        let bind = split
                            .messages
                            .iter()
                            .find_map(|message| match message {
                                ProtocolMessage::Bind(bind) => Some(bind),
                                _ => None,
                            })
                            .expect("bind");
                        assert_eq!(bind.params_raw().len(), 2);
                        assert_eq!(bind.params_raw()[0].data.as_ref(), name);
                        let generated = bind.parameter(1).expect("format").expect("ID");
                        assert_eq!(generated.bigint(), Some(id));
                        assert_eq!(generated.format(), format);
                        assert_eq!(bind.anonymous(), !prepared);
                        assert!(
                            split
                                .ast
                                .as_ref()
                                .expect("AST")
                                .rewrite_plan
                                .generated_ids
                                .is_empty()
                        );
                    }
                }
            }
        }
    }
}
