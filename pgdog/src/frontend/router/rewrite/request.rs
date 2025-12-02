use pg_query::ParseResult;
use tracing::debug;

use super::{Context, Error, Rewrite, StepOutput};
use crate::{
    backend::Cluster,
    frontend::{
        router::{
            parser::{cache::CachedAst, Cache},
            rewrite::RewriteModule,
        },
        ClientRequest, PreparedStatements,
    },
    net::ProtocolMessage,
};

pub struct RewriteRequest<'a> {
    request: &'a mut ClientRequest,
    cluster: &'a Cluster,
    prepared_statements: &'a mut PreparedStatements,
}

impl<'a> RewriteRequest<'a> {
    /// Perform new rewrite request.
    pub fn new(
        request: &'a mut ClientRequest,
        cluster: &'a Cluster,
        prepared_statements: &'a mut PreparedStatements,
    ) -> Self {
        Self {
            request,
            cluster,
            prepared_statements,
        }
    }

    /// Execute rewrite and return the query AST.
    pub fn execute(&'a mut self) -> Result<CachedAst, Error> {
        let schema = self.cluster.sharding_schema();

        let (result, ast, extended) = {
            let mut parse = None;
            let mut bind = None;
            let mut ast = None;

            let schema = self.cluster.sharding_schema();

            for message in self.request.iter() {
                match message {
                    ProtocolMessage::Parse(p) => {
                        ast = Some(Cache::get().parse(p.query(), &schema)?);
                        self.prepared_statements
                            .save_original_ast(p.name(), ast.as_ref().unwrap());
                        parse = Some(p);
                    }

                    ProtocolMessage::Query(query) => {
                        ast = Some(Cache::get().parse_uncached(query.query(), &schema)?);
                    }

                    ProtocolMessage::Bind(b) => {
                        let existing = self.prepared_statements.get_original_ast(b.statement());
                        if let Some(existing) = existing {
                            ast = Some(existing.clone());
                            bind = Some(b);
                        }
                    }

                    _ => (),
                }
            }

            let ast = ast.ok_or(Error::EmptyQuery)?;

            let mut context = Context::new(&ast.ast().protobuf, bind, parse);
            let mut rewrite = Rewrite::new(self.prepared_statements);

            let result = match rewrite.rewrite(&mut context) {
                Ok(_) => context.build()?,
                Err(Error::EmptyQuery) => StepOutput::NoOp,
                Err(err) => return Err(err),
            };

            (result, ast, parse.is_some())
        };

        let ast = match result {
            StepOutput::NoOp => {
                debug!("rewrite was a no-op");
                ast
            }
            StepOutput::RewriteInPlace { stmt, ast, actions } => {
                debug!("rewrite in-place: {}", stmt);
                for action in actions {
                    action.execute(self.request);
                }
                let ast = ParseResult::new(ast, "".into());
                // Cache new rewritten prepared statement.
                if extended {
                    Cache::get().save(&stmt, ast, &schema).unwrap()
                } else {
                    CachedAst::new_parsed(&stmt, ast, &schema).unwrap()
                }
            }
        };
        Ok(ast)
    }
}
