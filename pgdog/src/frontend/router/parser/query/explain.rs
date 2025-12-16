use super::*;

impl QueryParser {
    pub(super) fn explain(
        &mut self,
        cached_ast: &Ast,
        stmt: &ExplainStmt,
        context: &mut QueryParserContext,
    ) -> Result<Command, Error> {
        let query = stmt.query.as_ref().ok_or(Error::EmptyQuery)?;
        let node = query.node.as_ref().ok_or(Error::EmptyQuery)?;

        if context.expanded_explain() {
            if self.explain_recorder.is_none() {
                self.explain_recorder = Some(ExplainRecorder::new());
            }
        } else {
            self.explain_recorder = None;
        }

        let result = match node {
            NodeEnum::SelectStmt(ref stmt) => self.select(cached_ast, stmt, context),
            NodeEnum::InsertStmt(ref stmt) => self.insert(stmt, context),
            NodeEnum::UpdateStmt(ref stmt) => self.update(stmt, context),
            NodeEnum::DeleteStmt(ref stmt) => self.delete(stmt, context),

            _ => {
                // For other statement types, route to all shards
                Ok(Command::Query(Route::write(None)))
            }
        };

        match result {
            Ok(command) => Ok(command),
            Err(err) => {
                self.explain_recorder = None;
                Err(err)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::backend::Cluster;
    use crate::config::{self, config};
    use crate::frontend::client::Sticky;
    use crate::frontend::router::Ast;
    use crate::frontend::{BufferedQuery, ClientRequest, PreparedStatements, RouterContext};
    use crate::net::messages::{Bind, Parameter, Parse, Query};
    use bytes::Bytes;
    use std::sync::Once;

    fn enable_expanded_explain() {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            let mut cfg = config().as_ref().clone();
            cfg.config.general.expanded_explain = true;
            config::set(cfg).unwrap();
        });
    }

    // Helper function to route a plain SQL statement and return its `Route`.
    fn route(sql: &str) -> Route {
        enable_expanded_explain();
        let cluster = Cluster::new_test();
        let mut stmts = PreparedStatements::default();

        let ast = Ast::new(
            &BufferedQuery::Query(Query::new(sql)),
            &cluster.sharding_schema(),
            &mut stmts,
        )
        .unwrap();
        let mut buffer = ClientRequest::from(vec![Query::new(sql).into()]);
        buffer.ast = Some(ast);

        let ctx = RouterContext::new(&buffer, &cluster, None, None, Sticky::new()).unwrap();

        match QueryParser::default().parse(ctx).unwrap().clone() {
            Command::Query(route) => route,
            _ => panic!("expected Query command"),
        }
    }

    // Helper function to route a parameterized SQL statement and return its `Route`.
    fn route_parameterized(sql: &str, values: &[&[u8]]) -> Route {
        enable_expanded_explain();
        let parse_msg = Parse::new_anonymous(sql);
        let parameters = values
            .iter()
            .map(|v| Parameter {
                len: v.len() as i32,
                data: Bytes::copy_from_slice(v),
            })
            .collect::<Vec<_>>();

        let bind = Bind::new_params("", &parameters);

        let cluster = Cluster::new_test();
        let mut stmts = PreparedStatements::default();

        let ast = Ast::new(
            &BufferedQuery::Prepared(Parse::new_anonymous(sql)),
            &cluster.sharding_schema(),
            &mut stmts,
        )
        .unwrap();
        let mut buffer: ClientRequest = vec![parse_msg.into(), bind.into()].into();
        buffer.ast = Some(ast);

        let ctx = RouterContext::new(&buffer, &cluster, None, None, Sticky::new()).unwrap();

        match QueryParser::default().parse(ctx).unwrap().clone() {
            Command::Query(route) => route,
            _ => panic!("expected Query command"),
        }
    }

    #[test]
    #[should_panic(expected = "called `Result::unwrap()`")]
    fn test_explain_empty_query() {
        // explain() returns an EmptyQuery error
        // route() panics on error unwraps.
        let _ = route("EXPLAIN");
    }

    #[test]
    fn test_explain_select_no_tables() {
        let r = route("EXPLAIN SELECT NOW()");
        assert!(matches!(r.shard(), Shard::Direct(_)));
        assert!(r.is_read());
    }

    #[test]
    fn test_explain_select_with_sharding_key() {
        let r = route("EXPLAIN SELECT * FROM sharded WHERE id = 1");
        assert!(matches!(r.shard(), Shard::Direct(_)));
        assert!(r.is_read());
        let lines = r.explain().unwrap().render_lines();
        assert!(lines
            .iter()
            .any(|line| line.contains("matched sharding key")));

        let r = route_parameterized("EXPLAIN SELECT * FROM sharded WHERE id = $1", &[b"11"]);
        assert!(matches!(r.shard(), Shard::Direct(_)));
        assert!(r.is_read());
        let lines = r.explain().unwrap().render_lines();
        assert!(lines.iter().any(|line| line.contains("parameter")));
    }

    #[test]
    fn test_explain_select_all_shards() {
        let r = route("EXPLAIN SELECT * FROM sharded");
        assert_eq!(r.shard(), &Shard::All);
        assert!(r.is_read());
        let lines = r.explain().unwrap().render_lines();
        assert!(lines.iter().any(|line| line.contains("broadcast")));
    }

    #[test]
    fn test_explain_insert() {
        let r = route_parameterized(
            "EXPLAIN INSERT INTO sharded (id, email) VALUES ($1, $2)",
            &[b"11", b"test@test.com"],
        );
        assert!(matches!(r.shard(), Shard::Direct(_)));
        assert!(r.is_write());
        let lines = r.explain().unwrap().render_lines();
        assert!(lines
            .iter()
            .any(|line| line.contains("INSERT matched sharding key")));
    }

    #[test]
    fn test_explain_update() {
        let r = route_parameterized(
            "EXPLAIN UPDATE sharded SET email = $2 WHERE id = $1",
            &[b"11", b"new@test.com"],
        );
        assert!(matches!(r.shard(), Shard::Direct(_)));
        assert!(r.is_write());
        let lines = r.explain().unwrap().render_lines();
        assert!(lines
            .iter()
            .any(|line| line.contains("UPDATE matched WHERE clause")));

        let r = route("EXPLAIN UPDATE sharded SET active = true");
        assert_eq!(r.shard(), &Shard::All);
        assert!(r.is_write());
        let lines = r.explain().unwrap().render_lines();
        assert!(lines
            .iter()
            .any(|line| line.contains("UPDATE fell back to broadcast")));
    }

    #[test]
    fn test_explain_delete() {
        let r = route_parameterized("EXPLAIN DELETE FROM sharded WHERE id = $1", &[b"11"]);
        assert!(matches!(r.shard(), Shard::Direct(_)));
        assert!(r.is_write());
        let lines = r.explain().unwrap().render_lines();
        assert!(lines
            .iter()
            .any(|line| line.contains("DELETE matched WHERE clause")));

        let r = route("EXPLAIN DELETE FROM sharded");
        assert_eq!(r.shard(), &Shard::All);
        assert!(r.is_write());
        let lines = r.explain().unwrap().render_lines();
        assert!(lines
            .iter()
            .any(|line| line.contains("DELETE fell back to broadcast")));
    }

    #[test]
    fn test_explain_with_options() {
        let r = route("EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM sharded WHERE id = 1");
        assert!(matches!(r.shard(), Shard::Direct(_)));
        assert!(r.is_read());

        let r = route("EXPLAIN (FORMAT JSON) SELECT * FROM sharded WHERE id = 1");
        assert!(matches!(r.shard(), Shard::Direct(_)));
        assert!(r.is_read());
    }

    #[test]
    fn test_explain_with_comment_override() {
        let r = route("/* pgdog_shard: 5 */ EXPLAIN SELECT * FROM sharded");
        assert_eq!(r.shard(), &Shard::Direct(5));
        let lines = r.explain().unwrap().render_lines();
        assert_eq!(lines[3], "  Shard 5: manual override to shard=5");
    }

    #[test]
    fn test_explain_select_broadcast_entry() {
        let lines = route("EXPLAIN SELECT * FROM sharded")
            .explain()
            .unwrap()
            .render_lines();
        assert_eq!(lines[3], "  Note: no sharding key matched; broadcasting");
    }

    #[test]
    fn test_explain_select_parameter_entry() {
        let lines = route_parameterized("EXPLAIN SELECT * FROM sharded WHERE id = $1", &[b"22"])
            .explain()
            .unwrap()
            .render_lines();

        assert!(lines.iter().any(|line| line.contains("parameter")));
    }

    #[test]
    fn test_explain_analyze_insert() {
        let r = route("EXPLAIN ANALYZE INSERT INTO sharded (id, email) VALUES (1, 'a@a.com')");
        assert!(matches!(r.shard(), Shard::Direct(_)));
        assert!(r.is_write());

        let r = route_parameterized(
            "EXPLAIN ANALYZE INSERT INTO sharded (id, email) VALUES ($1, $2)",
            &[b"1", b"test@test.com"],
        );
        assert!(matches!(r.shard(), Shard::Direct(_)));
        assert!(r.is_write());
    }

    #[test]
    fn test_explain_analyze_update() {
        let r = route("EXPLAIN ANALYZE UPDATE sharded SET active = true");
        assert_eq!(r.shard(), &Shard::All);
        assert!(r.is_write());

        let r = route_parameterized(
            "EXPLAIN ANALYZE UPDATE sharded SET email = $2",
            &[b"everyone@same.com"],
        );
        assert_eq!(r.shard(), &Shard::All);
        assert!(r.is_write());

        // Test with sharding key
        let r = route_parameterized(
            "EXPLAIN ANALYZE UPDATE sharded SET email = $2 WHERE id = $1",
            &[b"1", b"new@test.com"],
        );
        assert!(matches!(r.shard(), Shard::Direct(_)));
        assert!(r.is_write());
    }

    #[test]
    fn test_explain_analyze_delete() {
        let r = route("EXPLAIN ANALYZE DELETE FROM sharded");
        assert_eq!(r.shard(), &Shard::All);
        assert!(r.is_write());

        // Test with non-sharding key
        let r = route_parameterized(
            "EXPLAIN ANALYZE DELETE FROM sharded WHERE active = $1",
            &[b"false"],
        );
        assert_eq!(r.shard(), &Shard::All);
        assert!(r.is_write());

        // Test with sharding key
        let r = route_parameterized("EXPLAIN ANALYZE DELETE FROM sharded WHERE id = $1", &[b"1"]);
        assert!(matches!(r.shard(), Shard::Direct(_)));
        assert!(r.is_write());
    }
}
