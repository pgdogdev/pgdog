use pg_query::{Node, NodeEnum};

use crate::frontend::router::parser::Cache;
use crate::frontend::router::Ast;
use crate::frontend::{BufferedQuery, ClientRequest};
use crate::net::messages::bind::{Format, Parameter};
use crate::net::{Bind, Parse, ProtocolMessage, Query};

use super::{Error, RewritePlan, StatementRewrite};

#[derive(Debug, Clone)]
pub struct InsertSplit {
    /// Parameter positions in the original Bind message
    /// that should be used to build the Bind message specific to this
    /// insert statement.
    params: Vec<u16>,

    /// The split up INSERT statement with parameters and/or values.
    stmt: String,

    /// The statement AST.
    ast: Ast,

    /// The global prepared statement name for this split.
    /// Only set when the original statement was a named prepared statement.
    statement_name: Option<String>,
}

impl InsertSplit {
    /// Get the parameter positions from the original Bind message.
    pub fn params(&self) -> &[u16] {
        &self.params
    }

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
    pub fn build_request(&self, request: &ClientRequest) -> ClientRequest {
        let mut new_request = ClientRequest::default();

        for message in &request.messages {
            let new_message = match message {
                ProtocolMessage::Parse(parse) => {
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
                    let new_bind = self.extract_bind_params(bind);
                    ProtocolMessage::Bind(new_bind)
                }
                other => other.clone(),
            };
            new_request.messages.push(new_message);
            new_request.ast = Some(self.ast.clone());
        }

        new_request
    }

    /// Extract specific parameters from a Bind message based on this split's param indices.
    fn extract_bind_params(&self, bind: &Bind) -> Bind {
        let params: Vec<Parameter> = self
            .params
            .iter()
            .filter_map(|&idx| bind.params_raw().get(idx as usize).cloned())
            .collect();

        let codes: Vec<Format> = if bind.format_codes_raw().len() == 1 {
            // Uniform format: keep it
            bind.format_codes_raw().clone()
        } else if bind.format_codes_raw().len() == bind.params_raw().len() {
            // One-to-one mapping: extract corresponding codes
            self.params
                .iter()
                .filter_map(|&idx| bind.format_codes_raw().get(idx as usize).copied())
                .collect()
        } else {
            // No codes (all text)
            Vec::new()
        };

        // Use the split's registered statement name if available,
        // otherwise fall back to the original bind's statement name.
        let statement_name = self
            .statement_name
            .as_deref()
            .unwrap_or_else(|| bind.statement());

        Bind::new_params_codes(statement_name, &params, &codes)
    }
}

/// Build separate ClientRequests for each insert split.
pub fn build_split_requests(splits: &[InsertSplit], request: &ClientRequest) -> Vec<ClientRequest> {
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
    pub(super) fn split_insert(&mut self, plan: &mut RewritePlan) -> Result<(), Error> {
        // Don't rewrite INSERTs in unsharded databases.
        if self.schema.shards == 1 {
            return Ok(());
        }

        let splits: Vec<(Vec<u16>, String)> = {
            let values_lists = match self.get_insert_values_lists() {
                Some(lists) if lists.len() > 1 => lists,
                _ => return Ok(()),
            };

            values_lists
                .iter()
                .map(|values_list| self.build_single_tuple_insert(values_list))
                .collect::<Result<Vec<_>, _>>()?
        };

        // Now create Ast for each split (needs mutable borrow of prepared_statements)
        let cache = Cache::get();
        for (params, stmt) in splits {
            let query = if self.extended {
                BufferedQuery::Prepared(Parse::named("", &stmt))
            } else {
                BufferedQuery::Query(Query::new(&stmt))
            };
            let ast = cache
                .query(&query, self.schema, self.prepared_statements)
                .map_err(|e| Error::Cache(e.to_string()))?;

            // If this is a named prepared statement, register the split in the global cache
            // and store the assigned name for use in Bind messages.
            let statement_name = if self.prepared {
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

    /// Get the values_lists from an INSERT statement, if present.
    fn get_insert_values_lists(&self) -> Option<&[Node]> {
        let stmt = self.stmt.stmts.first()?;
        let node = stmt.stmt.as_ref()?;

        if let NodeEnum::InsertStmt(insert) = node.node.as_ref()? {
            let select = insert.select_stmt.as_ref()?;
            if let NodeEnum::SelectStmt(select_stmt) = select.node.as_ref()? {
                if !select_stmt.values_lists.is_empty() {
                    return Some(&select_stmt.values_lists);
                }
            }
        }
        None
    }

    /// Build a single-tuple INSERT from the original statement with just one values_list.
    /// Returns the parameter positions (0-indexed) and the SQL string.
    fn build_single_tuple_insert(&self, values_list: &Node) -> Result<(Vec<u16>, String), Error> {
        let mut ast = self.stmt.clone();
        let mut params = Vec::new();

        // Collect parameter references from this values_list
        Self::collect_params(values_list, &mut params);

        // Renumber parameters to start from $1
        let mut new_values_list = values_list.clone();
        Self::renumber_params(&mut new_values_list, &params);

        // Replace the values_lists with just this one tuple
        if let Some(stmt) = ast.stmts.first_mut() {
            if let Some(node) = stmt.stmt.as_mut() {
                if let Some(NodeEnum::InsertStmt(insert)) = node.node.as_mut() {
                    if let Some(select) = insert.select_stmt.as_mut() {
                        if let Some(NodeEnum::SelectStmt(select_stmt)) = select.node.as_mut() {
                            select_stmt.values_lists = vec![new_values_list];
                        }
                    }
                }
            }
        }

        let stmt = ast.deparse()?;
        Ok((params, stmt))
    }

    /// Collect all parameter references from a node tree.
    fn collect_params(node: &Node, params: &mut Vec<u16>) {
        if let Some(node_enum) = &node.node {
            match node_enum {
                NodeEnum::ParamRef(param) => {
                    if param.number > 0 {
                        params.push((param.number - 1) as u16);
                    }
                }
                NodeEnum::List(list) => {
                    for item in &list.items {
                        Self::collect_params(item, params);
                    }
                }
                NodeEnum::TypeCast(cast) => {
                    if let Some(arg) = &cast.arg {
                        Self::collect_params(arg, params);
                    }
                }
                _ => {}
            }
        }
    }

    /// Renumber parameters in a node tree based on their position in the params list.
    fn renumber_params(node: &mut Node, params: &[u16]) {
        if let Some(node_enum) = &mut node.node {
            match node_enum {
                NodeEnum::ParamRef(param) => {
                    if param.number > 0 {
                        let old_pos = (param.number - 1) as u16;
                        if let Some(new_pos) = params.iter().position(|&p| p == old_pos) {
                            param.number = (new_pos + 1) as i32;
                        }
                    }
                }
                NodeEnum::List(list) => {
                    for item in &mut list.items {
                        Self::renumber_params(item, params);
                    }
                }
                NodeEnum::TypeCast(cast) => {
                    if let Some(arg) = &mut cast.arg {
                        Self::renumber_params(arg, params);
                    }
                }
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::replication::{ShardedSchemas, ShardedTables};
    use crate::backend::ShardingSchema;
    use crate::frontend::router::parser::StatementRewriteContext;
    use crate::frontend::PreparedStatements;

    fn default_schema() -> ShardingSchema {
        ShardingSchema {
            shards: 2,
            tables: ShardedTables::default(),
            schemas: ShardedSchemas::default(),
        }
    }

    fn parse_and_split(sql: &str) -> Vec<InsertSplit> {
        let mut ast = pg_query::parse(sql).unwrap().protobuf;
        let mut prepared = PreparedStatements::default();
        let schema = default_schema();
        let mut rewriter = StatementRewrite::new(StatementRewriteContext {
            stmt: &mut ast,
            extended: false,
            prepared: false,
            prepared_statements: &mut prepared,
            schema: &schema,
        });
        let mut plan = RewritePlan::default();
        rewriter.split_insert(&mut plan).unwrap();
        plan.insert_split
    }

    #[test]
    fn test_split_insert_with_params() {
        let splits = parse_and_split("INSERT INTO my_table (id, value) VALUES ($1, $2), ($3, $4)");

        assert_eq!(splits.len(), 2);

        // First tuple uses params 0 and 1 (original $1, $2)
        assert_eq!(splits[0].params(), &[0, 1]);
        assert_eq!(
            splits[0].stmt(),
            "INSERT INTO my_table (id, value) VALUES ($1, $2)"
        );

        // Second tuple uses params 2 and 3 (original $3, $4), renumbered to $1, $2
        assert_eq!(splits[1].params(), &[2, 3]);
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
        assert!(splits[0].params().is_empty());
        assert_eq!(
            splits[0].stmt(),
            "INSERT INTO my_table (id, value) VALUES (1, 'a')"
        );

        assert!(splits[1].params().is_empty());
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

        assert_eq!(splits[0].params(), &[0]);
        assert_eq!(
            splits[0].stmt(),
            "INSERT INTO my_table (id, value) VALUES ($1, 'a')"
        );

        assert_eq!(splits[1].params(), &[1]);
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
        let extracted = splits[0].extract_bind_params(&bind);
        assert_eq!(extracted.params_raw().len(), 2);
        assert_eq!(extracted.params_raw()[0].data.as_ref(), b"p0");
        assert_eq!(extracted.params_raw()[1].data.as_ref(), b"p1");

        // Second split uses params 2 and 3
        let extracted = splits[1].extract_bind_params(&bind);
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
        let extracted = splits[1].extract_bind_params(&bind);
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

        let extracted = splits[0].extract_bind_params(&bind);
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
        let extracted = splits[0].extract_bind_params(&bind);
        assert_eq!(extracted.params_raw().len(), 1);
        assert_eq!(extracted.params_raw()[0].data.as_ref(), b"value_for_param1");

        // Second split: statement uses $1 (renumbered from $2) with literal, bind extracts param 1
        assert_eq!(splits[1].stmt(), "INSERT INTO t (a, b) VALUES ($1, 'lit2')");
        let extracted = splits[1].extract_bind_params(&bind);
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
        assert_eq!(splits[0].params(), &[0, 1]);
        let extracted = splits[0].extract_bind_params(&bind);
        assert_eq!(extracted.params_raw().len(), 2);
        assert_eq!(extracted.params_raw()[0].data.as_ref(), b"p1");
        assert_eq!(extracted.params_raw()[1].data.as_ref(), b"p2");

        // Second split: uses param 2 (original $3), renumbered to $1
        assert_eq!(
            splits[1].stmt(),
            "INSERT INTO t (a, b) VALUES ($1, 'literal')"
        );
        assert_eq!(splits[1].params(), &[2]);
        let extracted = splits[1].extract_bind_params(&bind);
        assert_eq!(extracted.params_raw().len(), 1);
        assert_eq!(extracted.params_raw()[0].data.as_ref(), b"p3");
    }
}
