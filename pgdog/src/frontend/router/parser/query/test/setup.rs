use crate::{
    backend::Cluster,
    frontend::{
        client::{Sticky, TransactionType},
        router::{parser::Cache, QueryParser},
        ClientRequest, Command, PreparedStatements, RouterContext,
    },
    net::{Parameters, ProtocolMessage},
};

pub(crate) struct QueryParserTest {
    cluster: Cluster,
    params: Parameters,
    transaction: Option<TransactionType>,
    sticky: Sticky,
    prepared: PreparedStatements,
    pub(crate) parser: QueryParser,
    last_parse: Option<String>,
}

impl QueryParserTest {
    pub(crate) fn new() -> Self {
        let cluster = Cluster::new_test();

        Self {
            cluster,
            params: Parameters::default(),
            transaction: None,
            sticky: Sticky::default(),
            parser: QueryParser::default(),
            prepared: PreparedStatements::new(),
            last_parse: None,
        }
    }

    pub(crate) fn execute(&mut self, request: Vec<ProtocolMessage>) -> Command {
        let mut request: ClientRequest = request.into();

        for message in request.iter_mut() {
            if let ProtocolMessage::Parse(parse) = message {
                // Make sure it's available.
                let (_, name) = PreparedStatements::global().write().insert(parse);
                self.last_parse = Some(name);
            }

            if let ProtocolMessage::Bind(bind) = message {
                if let Some(ref name) = self.last_parse {
                    bind.rename(name);
                }
            }

            if let ProtocolMessage::Describe(desc) = message {
                if let Some(ref name) = self.last_parse {
                    desc.rename(name);
                }
            }
        }

        let ast = Cache::get()
            .query(
                &request.query().unwrap().expect("empty query in input"),
                &self.cluster.sharding_schema(),
                &mut self.prepared,
            )
            .unwrap();
        request.ast = Some(ast);

        let router_ctx = RouterContext::new(
            &request,
            &self.cluster,
            &self.params,
            self.transaction,
            self.sticky,
        )
        .unwrap();

        let command = self.parser.parse(router_ctx).unwrap();
        command.clone()
    }
}
