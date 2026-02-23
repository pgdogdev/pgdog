use lazy_static::lazy_static;

use super::super::Error;
use crate::{
    backend::Cluster,
    frontend::{
        client::Sticky,
        router::parser::{AstContext, Cache, Shard},
        BufferedQuery, ClientRequest, Command, PreparedStatements, Router, RouterContext,
    },
    net::{replication::TupleData, Bind, Parameters, Parse},
};

#[derive(Debug)]
pub struct StreamContext<'a> {
    request: ClientRequest,
    cluster: &'a Cluster,
    bind: Bind,
    parse: Parse,
}

impl<'a> StreamContext<'a> {
    /// Construct new stream context.
    pub fn new(cluster: &'a Cluster, tuple: &TupleData, stmt: &Parse) -> Self {
        let bind = tuple.to_bind(stmt.name());
        let request = ClientRequest::from(vec![stmt.clone().into(), bind.clone().into()]);

        Self {
            request,
            cluster,
            bind,
            parse: stmt.clone(),
        }
    }

    pub fn shard(&'a mut self) -> Result<Shard, Error> {
        let router_context = self.router_context()?;
        let mut router = Router::new();
        let route = router.query(router_context)?;

        if let Command::Query(route) = route {
            Ok(route.shard().clone())
        } else {
            Err(Error::IncorrectCommand)
        }
    }

    /// Get Bind message.
    pub fn bind(&self) -> &Bind {
        &self.bind
    }

    /// Construct router context.
    pub fn router_context(&'a mut self) -> Result<RouterContext<'a>, Error> {
        lazy_static! {
            static ref PARAMS: Parameters = Parameters::default();
        }

        let ast_context = AstContext::from_cluster(self.cluster, &PARAMS);

        let ast = Cache::get().query(
            &BufferedQuery::Prepared(self.parse.clone()),
            &ast_context,
            &mut PreparedStatements::default(),
        )?;
        self.request.ast = Some(ast);

        Ok(RouterContext::new(
            &self.request,
            self.cluster,
            &PARAMS,
            None,
            Sticky::new(),
        )?)
    }
}

#[cfg(test)]
mod test {
    use bytes::Bytes;

    use crate::{
        config::config,
        net::replication::logical::tuple_data::{Column, Identifier},
    };

    use super::*;

    #[test]
    fn test_stream_context() {
        let cluster = Cluster::new_test(&config());
        let tuple = TupleData {
            columns: vec![Column {
                identifier: Identifier::Format(pgdog_postgres_types::Format::Text),
                len: 1,
                data: Bytes::from("1"),
            }],
        };
        let parse = Parse::new_anonymous("INSERT INTO sharded (customer_id) VALUES ($1)");

        let shard = StreamContext::new(&cluster, &tuple, &parse)
            .shard()
            .unwrap();
        assert!(matches!(shard, Shard::Direct(_)));
    }
}
