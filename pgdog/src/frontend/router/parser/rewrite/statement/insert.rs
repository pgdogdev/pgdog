use indexmap::IndexSet;
use pg_raw_parse::{Node, NodeMut, deparse, make, nodes, walk};
use pgdog_config::RewriteMode;

use crate::frontend::router::Ast;
use crate::frontend::router::parser::Cache;
use crate::frontend::{BufferedQuery, ClientRequest};
use crate::net::{Bind, Parse, ProtocolMessage, Query};

use super::{Error, RewritePlan, StatementRewrite};

#[derive(Debug, Clone)]
pub struct InsertSplit {
    /// Parameter positions in the original Bind message
    /// that should be used to build the Bind message specific to this
    /// insert statement.
    params: IndexSet<u16>,

    /// The split up INSERT statement with parameters and/or values.
    stmt: String,

    /// The statement AST.
    ast: Ast,

    /// The global prepared statement name for this split.
    /// Only set when the original statement was a named prepared statement.
    statement_name: Option<String>,
}

impl InsertSplit {
    /// Get the SQL statement.
    pub fn stmt(&self) -> &str {
        &self.stmt
    }

    /// Get the AST.
    pub fn ast(&self) -> &Ast {
        &self.ast
    }

    /// Get the global prepared statement name, if this split was registered.
    pub fn statement_name(&self) -> Option<&str> {
        self.statement_name.as_deref()
    }

    /// Build a ClientRequest from this split and the original request.
    pub fn build_request(&self, request: &ClientRequest) -> Result<ClientRequest, Error> {
        let mut new_request = ClientRequest::default();
        let mut has_parse = false;

        for message in &request.messages {
            let new_message = match message {
                ProtocolMessage::Parse(parse) => {
                    has_parse = true;
                    let mut new_parse = parse.clone();
                    new_parse.set_query(&self.stmt);

                    if let Some(name) = self.statement_name() {
                        new_parse.rename_fast(name);
                    }

                    ProtocolMessage::Parse(new_parse)
                }
                ProtocolMessage::Query(query) => {
                    let mut new_query = query.clone();
                    new_query.set_query(&self.stmt);
                    ProtocolMessage::Query(new_query)
                }
                ProtocolMessage::Bind(bind) => {
                    let new_bind = self.extract_bind_params(bind)?;
                    ProtocolMessage::Bind(new_bind)
                }
                other => other.clone(),
            };
            new_request.messages.push(new_message);
            new_request.ast = Some(self.ast.clone());
        }

        // When the driver prepared the statement in a separate round-trip
        // (lib/pq: Parse/Describe/Sync, then Bind/Execute/Sync), the execute
        // batch carries no Parse of its own and relies on `last_parse` being
        // injected before the Bind. Rewrite that saved Parse to this split's
        // single-tuple statement so the backend prepares the right one;
        // otherwise it would still hold the original multi-tuple statement and
        // reject the Bind's parameter count.
        if !has_parse && let Some(parse) = &request.last_parse {
            let mut split_parse = parse.clone();
            split_parse.set_query(&self.stmt);
            if let Some(name) = self.statement_name() {
                split_parse.rename_fast(name);
            }
            new_request.last_parse = Some(split_parse);
        }

        Ok(new_request)
    }

    /// Extract specific parameters from a Bind message based on this split's param indices.
    fn extract_bind_params(&self, bind: &Bind) -> Result<Bind, Error> {
        let mut new = Bind::new_statement(self.statement_name().unwrap_or_default());
        for param in &self.params {
            let param = bind
                .parameter(*param as usize - 1)?
                .ok_or(Error::MissingParameter(*param))?;
            new.push_param(param.parameter().clone(), param.format());
        }

        Ok(new)
    }
}

/// Build separate ClientRequests for each insert split.
pub fn build_split_requests(
    splits: &[InsertSplit],
    request: &ClientRequest,
) -> Result<Vec<ClientRequest>, Error> {
    splits
        .iter()
        .map(|split| split.build_request(request))
        .collect()
}

impl StatementRewrite<'_> {
    /// Split up multi-tuple INSERT statements into separate single-tuple statements
    /// for individual execution.
    ///
    /// # Example
    ///
    /// ```sql
    /// INSERT INTO my_table (id, value) VALUES ($1, $2), ($3, $4)
    /// ```
    ///
    /// becomes
    ///
    /// ```sql
    /// INSERT INTO my_table (id, value) VALUES ($1, $2)
    /// INSERT INTO my_table (id, value) VALUES ($1, $2) -- These are copied from params $3 and $4
    /// ```
    ///
    pub(super) fn split_insert(
        &mut self,
        insert: &nodes::InsertStmt,
        plan: &mut RewritePlan,
    ) -> Result<(), Error> {
        // Don't rewrite INSERTs in unsharded databases.
        if self.schema.shards == 1 || self.schema.rewrite.split_inserts != RewriteMode::Rewrite {
            return Ok(());
        }

        let mut splits = Vec::new();
        make::try_owned(|mem| {
            let mut copy = mem.make_unique(insert);

            if let Node::SelectStmt(select) = insert.select_stmt() {
                for list in select.values_lists() {
                    let (params, select) = self.build_single_tuple_select(mem, list);
                    copy.as_mut().set_select_stmt(select.uncast());
                    splits.push((params, deparse(&*copy)?.as_str().to_string()));
                }
            }

            Ok::<_, Error>(copy)
        })?;

        if splits.len() <= 1 {
            return Ok(());
        }

        // FIXME(sage): This is extremely duplicated with the work we do for
        // multi-step updates. (#1178 for inserts) Both pieces of code have
        // distinct sets of bugs. We should unify those two parts of the code
        // base and make this behave consistently.

        // Now create Ast for each split (needs mutable borrow of prepared_statements)
        let cache = Cache::get();
        let ctx = self.ast_context();
        for (params, stmt) in splits {
            let query = if self.extended {
                BufferedQuery::Prepared(Parse::named("", &stmt))
            } else {
                BufferedQuery::Query(Query::new(&stmt))
            };
            let ast = cache
                .query(&query, &ctx, self.prepared_statements)
                .map_err(|e| Error::Cache(e.to_string()))?;

            // If this is a named prepared statement, register the split in the global cache
            // and store the assigned name for use in Bind messages.
            let statement_name = if self.prepared {
                // Name will be assigned by `insert`.
                let mut parse = Parse::named("", &stmt);
                self.prepared_statements.insert(&mut parse);
                Some(parse.name().to_owned())
            } else {
                None
            };

            plan.insert_split.push(InsertSplit {
                params,
                stmt,
                ast,
                statement_name,
            });
        }

        Ok(())
    }

    /// Build a single-tuple INSERT from the original statement with just one values_list.
    /// Returns the parameter positions (0-indexed) and the SQL string.
    fn build_single_tuple_select<'mem>(
        &self,
        mem: make::MemoryToken<'mem>,
        values_list: Node<'_>,
    ) -> (IndexSet<u16>, make::Unique<'mem, &'mem nodes::SelectStmt>) {
        let mut tuple = mem.make_unique(values_list);

        let mut params = IndexSet::new();
        walk::walk_mut(tuple.as_mut(), |node| {
            if let NodeMut::ParamRef(param) = node {
                params.insert(param.number as _);
                param.set_number(params.get_index_of(&(param.number as u16)).unwrap() as i32 + 1)
            }
        });

        let mut select = mem.make_node::<nodes::SelectStmt>();
        select.as_mut().set_values_lists(mem.make_list(&[tuple]));
        (params, select)
    }
}

#[cfg(test)]
mod tests {
    use pgdog_config::Rewrite;

    use super::*;
    use crate::backend::ShardingSchema;
    use crate::backend::schema::Schema;
    use crate::frontend::PreparedStatements;
    use crate::frontend::router::parser::StatementRewriteContext;
    use crate::net::messages::bind::{Format, Parameter};

    fn default_db_schema() -> Schema {
        Schema::default()
    }

    fn default_schema() -> ShardingSchema {
        ShardingSchema {
            shards: 2,
            rewrite: Rewrite {
                enabled: true,
                split_inserts: RewriteMode::Rewrite,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn parse_and_split(sql: &str) -> Vec<InsertSplit> {
        let root = pg_raw_parse::parse(sql).unwrap();
        let insert = match root.stmts().next() {
            Some(Node::InsertStmt(insert)) => insert,
            _ => unreachable!(),
        };
        let mut prepared = PreparedStatements::default();
        let schema = default_schema();
        let db_schema = default_db_schema();
        let mut rewriter = StatementRewrite::new(StatementRewriteContext {
            extended: false,
            prepared: false,
            prepared_statements: &mut prepared,
            schema: &schema,
            db_schema: &db_schema,
            user: "",
            search_path: None,
        });
        let mut plan = RewritePlan::default();
        rewriter.split_insert(insert, &mut plan).unwrap();
        plan.insert_split
    }

    #[test]
    fn test_split_insert_with_params() {
        let splits = parse_and_split("INSERT INTO my_table (id, value) VALUES ($1, $2), ($3, $4)");

        assert_eq!(splits.len(), 2);

        // First tuple uses params 0 and 1 (original $1, $2)
        assert_eq!(splits[0].params.as_slice(), &[1, 2]);
        assert_eq!(
            splits[0].stmt(),
            "INSERT INTO my_table (id, value) VALUES ($1, $2)"
        );

        // Second tuple uses params 2 and 3 (original $3, $4), renumbered to $1, $2
        assert_eq!(splits[1].params.as_slice(), &[3, 4]);
        assert_eq!(
            splits[1].stmt(),
            "INSERT INTO my_table (id, value) VALUES ($1, $2)"
        );
    }

    #[test]
    fn test_split_insert_single_tuple_no_split() {
        let splits = parse_and_split("INSERT INTO my_table (id, value) VALUES ($1, $2)");

        // Single tuple should not be split
        assert!(splits.is_empty());
    }

    #[test]
    fn test_split_insert_literal_values() {
        let splits = parse_and_split("INSERT INTO my_table (id, value) VALUES (1, 'a'), (2, 'b')");

        assert_eq!(splits.len(), 2);

        // No params for literal values
        assert!(splits[0].params.is_empty());
        assert_eq!(
            splits[0].stmt(),
            "INSERT INTO my_table (id, value) VALUES (1, 'a')"
        );

        assert!(splits[1].params.is_empty());
        assert_eq!(
            splits[1].stmt(),
            "INSERT INTO my_table (id, value) VALUES (2, 'b')"
        );
    }

    #[test]
    fn test_split_insert_mixed_params_and_literals() {
        let splits =
            parse_and_split("INSERT INTO my_table (id, value) VALUES ($1, 'a'), ($2, 'b')");

        assert_eq!(splits.len(), 2);

        assert_eq!(splits[0].params.as_slice(), &[1]);
        assert_eq!(
            splits[0].stmt(),
            "INSERT INTO my_table (id, value) VALUES ($1, 'a')"
        );

        assert_eq!(splits[1].params.as_slice(), &[2]);
        assert_eq!(
            splits[1].stmt(),
            "INSERT INTO my_table (id, value) VALUES ($1, 'b')"
        );
    }

    #[test]
    fn test_extract_bind_params() {
        let splits = parse_and_split("INSERT INTO t (a, b) VALUES ($1, $2), ($3, $4)");
        let bind = Bind::new_params(
            "test",
            &[
                Parameter::new(b"p0"),
                Parameter::new(b"p1"),
                Parameter::new(b"p2"),
                Parameter::new(b"p3"),
            ],
        );

        // First split uses params 0 and 1
        let extracted = splits[0].extract_bind_params(&bind).unwrap();
        assert_eq!(extracted.params_raw().len(), 2);
        assert_eq!(extracted.params_raw()[0].data.as_ref(), b"p0");
        assert_eq!(extracted.params_raw()[1].data.as_ref(), b"p1");

        // Second split uses params 2 and 3
        let extracted = splits[1].extract_bind_params(&bind).unwrap();
        assert_eq!(extracted.params_raw().len(), 2);
        assert_eq!(extracted.params_raw()[0].data.as_ref(), b"p2");
        assert_eq!(extracted.params_raw()[1].data.as_ref(), b"p3");
    }

    #[test]
    fn test_extract_bind_params_with_format_codes() {
        let splits = parse_and_split("INSERT INTO t (a, b) VALUES ($1, $2), ($3, $4)");
        let bind = Bind::new_params_codes(
            "test",
            &[
                Parameter::new(b"p0"),
                Parameter::new(b"p1"),
                Parameter::new(b"p2"),
                Parameter::new(b"p3"),
            ],
            &[Format::Text, Format::Binary, Format::Text, Format::Binary],
        );

        // Second split uses params 2 and 3 (Text, Binary)
        let extracted = splits[1].extract_bind_params(&bind).unwrap();
        assert_eq!(extracted.params_raw().len(), 2);
        assert_eq!(extracted.params_raw()[0].data.as_ref(), b"p2");
        assert_eq!(extracted.params_raw()[1].data.as_ref(), b"p3");
        assert_eq!(extracted.format_codes_raw().len(), 2);
        assert_eq!(extracted.format_codes_raw()[0], Format::Text);
        assert_eq!(extracted.format_codes_raw()[1], Format::Binary);
    }

    #[test]
    fn test_extract_bind_params_uniform_format() {
        let splits = parse_and_split("INSERT INTO t (a) VALUES ($1), ($2)");
        let bind = Bind::new_params_codes(
            "test",
            &[Parameter::new(b"p0"), Parameter::new(b"p1")],
            &[Format::Binary], // Uniform format
        );

        let extracted = splits[0].extract_bind_params(&bind).unwrap();
        assert_eq!(extracted.params_raw().len(), 1);
        assert_eq!(extracted.format_codes_raw().len(), 1);
        assert_eq!(extracted.format_codes_raw()[0], Format::Binary);
    }

    #[test]
    fn test_extract_bind_params_mixed_params_and_literals() {
        let splits = parse_and_split("INSERT INTO t (a, b) VALUES ($1, 'lit1'), ($2, 'lit2')");
        let bind = Bind::new_params(
            "test",
            &[
                Parameter::new(b"value_for_param1"),
                Parameter::new(b"value_for_param2"),
            ],
        );

        assert_eq!(splits.len(), 2);

        // First split: statement uses $1 with literal, bind extracts param 0
        assert_eq!(splits[0].stmt(), "INSERT INTO t (a, b) VALUES ($1, 'lit1')");
        let extracted = splits[0].extract_bind_params(&bind).unwrap();
        assert_eq!(extracted.params_raw().len(), 1);
        assert_eq!(extracted.params_raw()[0].data.as_ref(), b"value_for_param1");

        // Second split: statement uses $1 (renumbered from $2) with literal, bind extracts param 1
        assert_eq!(splits[1].stmt(), "INSERT INTO t (a, b) VALUES ($1, 'lit2')");
        let extracted = splits[1].extract_bind_params(&bind).unwrap();
        assert_eq!(extracted.params_raw().len(), 1);
        assert_eq!(extracted.params_raw()[0].data.as_ref(), b"value_for_param2");
    }

    #[test]
    fn test_extract_bind_params_varying_param_counts() {
        // First tuple has 2 params, second tuple has 1 param and 1 literal
        let splits = parse_and_split("INSERT INTO t (a, b) VALUES ($1, $2), ($3, 'literal')");
        let bind = Bind::new_params(
            "test",
            &[
                Parameter::new(b"p1"),
                Parameter::new(b"p2"),
                Parameter::new(b"p3"),
            ],
        );

        assert_eq!(splits.len(), 2);

        // First split: uses params 0 and 1 (original $1, $2)
        assert_eq!(splits[0].stmt(), "INSERT INTO t (a, b) VALUES ($1, $2)");
        let extracted = splits[0].extract_bind_params(&bind).unwrap();
        assert_eq!(extracted.params_raw().len(), 2);
        assert_eq!(extracted.params_raw()[0].data.as_ref(), b"p1");
        assert_eq!(extracted.params_raw()[1].data.as_ref(), b"p2");

        // Second split: uses param 2 (original $3), renumbered to $1
        assert_eq!(
            splits[1].stmt(),
            "INSERT INTO t (a, b) VALUES ($1, 'literal')"
        );
        let extracted = splits[1].extract_bind_params(&bind).unwrap();
        assert_eq!(extracted.params_raw().len(), 1);
        assert_eq!(extracted.params_raw()[0].data.as_ref(), b"p3");
    }

    #[test]
    fn test_extract_bind_params_incorrect_count() {
        let splits = parse_and_split("INSERT INTO t (a, b) VALUES ($1, $2), ($3, $4)");
        let bind = Bind::new_params(
            "test",
            &[
                Parameter::new(b"p1"),
                Parameter::new(b"p2"),
                Parameter::new(b"p3"),
            ],
        );

        std::assert_matches!(splits[0].extract_bind_params(&bind), Ok(_));
        std::assert_matches!(
            splits[1].extract_bind_params(&bind),
            Err(Error::MissingParameter(_))
        );
    }
}
