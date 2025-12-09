use super::*;
use crate::frontend::router::{parser::Shard, round_robin};

impl QueryParser {
    /// Handle SHOW command.
    pub(super) fn show(
        &mut self,
        stmt: &VariableShowStmt,
        context: &QueryParserContext,
    ) -> Result<Command, Error> {
        match stmt.name.as_str() {
            "pgdog.shards" => Ok(Command::InternalField {
                name: "shards".into(),
                value: context.shards.to_string(),
            }),
            "pgdog.unique_id" => Ok(Command::UniqueId),
            _ => {
                let shard = Shard::Direct(round_robin::next() % context.shards);
                let route = Route::write(shard).set_read(context.read_only);
                Ok(Command::Query(route))
            }
        }
    }
}

#[cfg(test)]
mod test_show {
    use crate::backend::Cluster;
    use crate::frontend::client::Sticky;
    use crate::frontend::router::parser::Shard;
    use crate::frontend::router::QueryParser;
    use crate::frontend::{ClientRequest, PreparedStatements, RouterContext};
    use crate::net::messages::Query;
    use crate::net::Parameters;

    #[test]
    fn show_runs_on_a_direct_shard_round_robin() {
        let mut ps = PreparedStatements::default();
        let c = Cluster::new_test();
        let p = Parameters::default();
        let mut parser = QueryParser::default();

        // First call
        let query = "SHOW TRANSACTION ISOLATION LEVEL";
        let buffer = ClientRequest::from(vec![Query::new(query).into()]);
        let context = RouterContext::new(&buffer, &c, &mut ps, &p, None, Sticky::new()).unwrap();

        let first = parser.parse(context).unwrap().clone();
        let first_shard = first.route().shard();
        assert!(matches!(first_shard, Shard::Direct(_)));

        // Second call
        let query = "SHOW TRANSACTION ISOLATION LEVEL";
        let buffer = ClientRequest::from(vec![Query::new(query).into()]);
        let context = RouterContext::new(&buffer, &c, &mut ps, &p, None, Sticky::new()).unwrap();

        let second = parser.parse(context).unwrap().clone();
        let second_shard = second.route().shard();
        assert!(matches!(second_shard, Shard::Direct(_)));

        // Round robin shard routing
        assert!(second_shard != first_shard);
    }
}
