use pg_query::ParseResult;
use tracing::debug;

use super::{Context, Error, Rewrite, RewriteModule, StepOutput};
use crate::{
    backend::Cluster,
    frontend::{
        router::{
            parser::{cache::CachedAst, Cache},
            rewrite::ImmutableRewritePlan,
        },
        ClientRequest, PreparedStatements,
    },
    net::{Protocol, ProtocolMessage},
};

pub struct RewriteRequest<'a> {
    request: &'a mut ClientRequest,
    cluster: &'a Cluster,
    prepared_statements: &'a mut PreparedStatements,
    plan: Option<ImmutableRewritePlan>,
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
            plan: None,
        }
    }

    fn handle_parse(&mut self) -> Result<CachedAst, Error> {
        let parse = self.request.iter().find(|p| p.code() == 'P');
        let parse = if let Some(ProtocolMessage::Parse(parse)) = parse {
            parse
        } else {
            return Err(Error::EmptyQuery);
        };

        let schema = self.cluster.sharding_schema();
        let ast = Cache::get().parse(parse.query(), &schema)?;

        let mut context = Context::new(&ast.ast().protobuf, Some(parse));
        Rewrite::new(self.prepared_statements).rewrite(&mut context)?;
        let output = context.build()?;

        let ast = match output {
            StepOutput::NoOp => {
                debug!("rewrite (extended) is a no-op");
                ast
            }
            StepOutput::RewriteInPlace {
                actions,
                ast,
                stmt,
                stats,
                plan,
            } => {
                debug!("rewrite (extended): {}", stmt);

                for action in actions {
                    action.execute(self.request);

                    // Update the rewritten parse in the global cache
                    // and save its rewrite plan.
                    if let ProtocolMessage::Parse(parse) = action.message {
                        if !parse.anonymous() {
                            self.prepared_statements
                                .global
                                .write()
                                .set_rewrite(&parse, plan.clone());
                        }
                    }
                }

                self.plan = Some(plan);

                // Update stats.
                {
                    let cluster_stats = self.cluster.stats();
                    let mut lock = cluster_stats.lock();
                    lock.rewrite = lock.rewrite + stats;
                }

                let ast = ParseResult::new(ast, "".into());
                Cache::get().save(&stmt, ast, &schema)?
            }
        };

        Ok(ast)
    }

    fn handle_query(&mut self) -> Result<CachedAst, Error> {
        let query = self.request.iter().find(|p| p.code() == 'Q');
        let query = if let Some(ProtocolMessage::Query(query)) = query {
            query
        } else {
            return Err(Error::EmptyQuery);
        };

        let schema = self.cluster.sharding_schema();
        let ast = Cache::get().parse_uncached(query.query(), &schema)?;

        let mut context = Context::new(&ast.ast().protobuf, None);
        Rewrite::new(self.prepared_statements).rewrite(&mut context)?;
        let output = context.build()?;

        let ast = match output {
            StepOutput::NoOp => {
                debug!("rewrite (simple) is a no-op");
                ast
            }
            StepOutput::RewriteInPlace {
                actions,
                ast,
                stmt,
                stats,
                plan: _,
            } => {
                debug!("rewrite (simple): {}", stmt);

                for action in actions {
                    action.execute(self.request);
                }

                // Update stats.
                {
                    let cluster_stats = self.cluster.stats();
                    let mut lock = cluster_stats.lock();
                    lock.rewrite = lock.rewrite + stats;
                }

                let ast = ParseResult::new(ast, "".into());
                CachedAst::new_parsed(&stmt, ast, &schema)?
            }
        };

        Ok(ast)
    }

    /// Execute rewrite and return the query AST.
    pub fn execute(&mut self) -> Result<Option<CachedAst>, Error> {
        let mut ast: Option<CachedAst> = None;

        for result in [self.handle_parse(), self.handle_query()] {
            match result {
                Ok(a) => ast = Some(a),
                Err(Error::EmptyQuery) => continue,
                Err(err) => return Err(err),
            }
        }
        let parameters = self.request.parameters_mut()?;
        if let Some(parameters) = parameters {
            if self.plan.is_none() {
                self.plan = self
                    .prepared_statements
                    .global
                    .read()
                    .rewrite_engine_plan(parameters.statement());
            }

            if let Some(plan) = self.plan.take() {
                plan.apply_bind(parameters)?;
            }
        }

        Ok(ast)
    }
}
