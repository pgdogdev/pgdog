use indexmap::{IndexMap, IndexSet};
use pg_raw_parse::{Node, NodeMut, deparse, make, nodes, walk};
use pgdog_config::RewriteMode;
use smallvec::SmallVec;

use crate::frontend::router::parser::{Cache, Shard};
use crate::frontend::{ClientRequest, PreparedStatements};
use crate::net::{Bind, Parse, ProtocolMessage, Query};

use super::{Error, InsertSplitRewriteResult, RewritePlan, StatementRewrite};

#[derive(Debug, Clone)]
pub(crate) struct Subquery {
    /// The inner-SQL of the outer statement (the Subquery's SQL) that we wish to execute
    /// and re-write in our multi-step code.
    pub(crate) inner_stmt: String,
}

#[derive(Debug, Clone)]
pub(crate) struct InsertSplit {
    /// Parameter positions in the original Bind message
    /// that should be used to build the Bind message specific to this
    /// insert statement.
    params: IndexSet<u16>,

    /// The split up INSERT statement with parameters and/or values.
    stmt: String,

    /// The global prepared statement name for this split.
    /// Only set when the original statement was a named prepared statement.
    statement_name: Option<String>,
}

impl InsertSplit {
    /// Get the global prepared statement name, if this split was registered.
    pub(crate) fn statement_name(&self) -> Option<&str> {
        self.statement_name.as_deref()
    }

    /// Build a ClientRequest from this split and the original request.
    pub(crate) fn build_request(&self, request: &ClientRequest) -> Result<ClientRequest, Error> {
        let mut new_request = ClientRequest::default();
        let mut has_parse = false;

        for message in &request.messages {
            let new_message = match message {
                ProtocolMessage::Parse(parse) => {
                    has_parse = true;
                    let mut new_parse = parse.clone();
                    new_parse.set_query(&self.stmt);

                    if let Some(name) = self.statement_name() {
                        new_parse.rename(name);
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
            let cache = Cache::get();
            new_request.ast = Some(cache.record(&self.stmt)?.ast);
        }

        // When the driver prepared the statement in a separate round-trip
        // (lib/pq: Parse/Describe/Sync, then Bind/Execute/Sync), the execute
        // batch carries no Parse of its own and relies on `last_parse` being
        // injected before the Bind. Rewrite that saved Parse to this split's
        // single-tuple statement so the backend prepares the right one;
        // otherwise it would still hold the original multi-tuple statement and
        // reject the Bind's parameter count.
        if !has_parse && let Some(parse) = &request.last_parse {
            // FIXME: We should be able to use the previously cached `Parse` here
            let mut split_parse = parse.clone();
            split_parse.set_query(&self.stmt);
            if let Some(name) = self.statement_name() {
                split_parse.rename(name);
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
pub(crate) fn build_split_requests(
    splits: &[InsertSplit],
    request: &ClientRequest,
) -> Result<Vec<ClientRequest>, Error> {
    splits
        .iter()
        .map(|split| split.build_request(request))
        .collect()
}

/// Split a simple query after sequence calls have been resolved. Both the SQL
/// and routing AST must use the values allocated for this execution.
pub(super) fn build_resolved_split_requests(
    query: &Query,
    request: &ClientRequest,
) -> Result<Vec<ClientRequest>, Error> {
    let ast = pg_raw_parse::parse(query.query())?;
    let Some(Node::InsertStmt(insert)) = ast.stmts().next() else {
        return Err(Error::EmptyQuery);
    };
    split_insert_statements(insert)?
        .into_iter()
        .map(|(params, stmt)| {
            InsertSplit {
                params,
                stmt,
                statement_name: None,
            }
            .build_request(request)
        })
        .collect()
}

/// Combine split requests that were routed to the same shard
/// into one multi-tuple INSERT per shard.
pub(crate) fn group_split_requests_by_shard(
    requests: Vec<ClientRequest>,
    request: &ClientRequest,
    prepared_statements: &mut PreparedStatements,
) -> Result<InsertSplitRewriteResult, Error> {
    if !needs_grouping(&requests) {
        return Ok(InsertSplitRewriteResult::new(requests));
    }

    let mut groups: IndexMap<&Shard, Vec<usize>> = IndexMap::new();

    for (tuple, split) in requests.iter().enumerate() {
        groups.entry(split.route().shard()).or_default().push(tuple);
    }

    let query = request.query()?.ok_or(Error::EmptyQuery)?;
    let ast = pg_raw_parse::parse(query.query())?;
    let Some(Node::InsertStmt(insert)) = ast.stmts().next() else {
        return Err(Error::EmptyQuery);
    };

    let tuple_groups: Vec<Vec<usize>> = groups.into_values().collect();
    let statements = group_insert_statements(insert, &tuple_groups)?;

    // TODO(jkaczman): This'll need some consideration in the future w.r.t subquery re-writes,
    // e.g., if we have an INSERT split stemming from generate_series with
    // an excessively large number, this could blow up.. there's probably
    // a query size limit or Bind param limit.

    let mut grouped = Vec::new();
    for ((params, stmt), tuples) in statements.into_iter().zip(&tuple_groups) {
        let statement_name = if query.prepared() {
            let mut parse = Parse::named("", &stmt);
            prepared_statements.insert(&mut parse);
            Some(parse.name().to_owned())
        } else {
            None
        };

        let mut new_request = InsertSplit {
            params,
            stmt,
            statement_name,
        }
        .build_request(request)?;
        new_request.route = requests[tuples[0]].route.clone();
        grouped.push(new_request);
    }

    let reordered = !tuple_groups.iter().flatten().is_sorted();

    Ok(InsertSplitRewriteResult {
        requests: grouped,
        tuples: reordered.then_some(tuple_groups),
    })
}

fn needs_grouping(requests: &[ClientRequest]) -> bool {
    let Some((first, rest)) = requests.split_first() else {
        return false;
    };

    let same_shard = rest
        .iter()
        .all(|split| split.route().shard() == first.route().shard());

    let distinct_shards = requests.iter().enumerate().all(|(index, split)| {
        requests[..index]
            .iter()
            .all(|other| other.route().shard() != split.route().shard())
    });

    !same_shard && !distinct_shards
}

fn split_insert_statements(
    insert: &nodes::InsertStmt,
) -> Result<Vec<(IndexSet<u16>, String)>, Error> {
    let mut splits = Vec::new();
    make::try_owned(|mem| {
        let mut copy = mem.make_unique(insert);

        if let Node::SelectStmt(select) = insert.select_stmt() {
            for list in select.values_lists() {
                let (params, select) = StatementRewrite::build_tuples_select(mem, &[list]);
                copy.as_mut().set_select_stmt(select.uncast());
                splits.push((params, deparse(&*copy)?.as_str().to_string()));
            }
        }

        Ok::<_, Error>(copy)
    })?;
    Ok(splits)
}

/// Build one INSERT for each group of tuple positions in the original INSERT.
fn group_insert_statements(
    insert: &nodes::InsertStmt,
    groups: &[Vec<usize>],
) -> Result<Vec<(IndexSet<u16>, String)>, Error> {
    let mut splits = Vec::new();
    make::try_owned(|mem| {
        let mut copy = mem.make_unique(insert);

        if let Node::SelectStmt(select) = insert.select_stmt() {
            let values_lists: Vec<Node<'_>> = select.values_lists().into_iter().collect();

            for group in groups {
                let tuples: Vec<Node<'_>> =
                    group.iter().map(|&tuple| values_lists[tuple]).collect();
                let (params, select) = StatementRewrite::build_tuples_select(mem, &tuples);
                copy.as_mut().set_select_stmt(select.uncast());
                splits.push((params, deparse(&*copy)?.as_str().to_string()));
            }
        }

        Ok::<_, Error>(copy)
    })?;
    Ok(splits)
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

        let splits = split_insert_statements(insert)?;

        if splits.len() <= 1 {
            return Ok(());
        }

        // FIXME(sage): This is extremely duplicated with the work we do for
        // multi-step updates. (#1178 for inserts) Both pieces of code have
        // distinct sets of bugs. We should unify those two parts of the code
        // base and make this behave consistently.

        // Now create Ast for each split (needs mutable borrow of prepared_statements)
        for (params, stmt) in splits {
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
                statement_name,
            });
        }

        Ok(())
    }

    /// Build a VALUES list from the tuples of the original statement.
    /// Returns the original parameter positions (1-indexed) and SELECT AST.
    fn build_tuples_select<'mem>(
        mem: make::MemoryToken<'mem>,
        values_lists: &[Node<'_>],
    ) -> (IndexSet<u16>, make::Unique<'mem, &'mem nodes::SelectStmt>) {
        let mut params = IndexSet::new();
        let mut tuples: SmallVec<[_; 8]> = SmallVec::new();

        for values_list in values_lists {
            let mut tuple = mem.make_unique(*values_list);
            walk::walk_mut(tuple.as_mut(), |node| {
                if let NodeMut::ParamRef(param) = node {
                    params.insert(param.number as _);
                    param
                        .set_number(params.get_index_of(&(param.number as u16)).unwrap() as i32 + 1)
                }
            });
            tuples.push(tuple);
        }

        let mut select = mem.make_node::<nodes::SelectStmt>();
        select.as_mut().set_values_lists(mem.make_list(&tuples));
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
    use crate::frontend::client::QueryTimestamps;
    use crate::frontend::router::parser::{Route, ShardWithPriority, StatementRewriteContext};
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
            timezone: None,
            query_timestamps: QueryTimestamps::default(),
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
            splits[0].stmt,
            "INSERT INTO my_table (id, value) VALUES ($1, $2)"
        );

        // Second tuple uses params 2 and 3 (original $3, $4), renumbered to $1, $2
        assert_eq!(splits[1].params.as_slice(), &[3, 4]);
        assert_eq!(
            splits[1].stmt,
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
            splits[0].stmt,
            "INSERT INTO my_table (id, value) VALUES (1, 'a')"
        );

        assert!(splits[1].params.is_empty());
        assert_eq!(
            splits[1].stmt,
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
            splits[0].stmt,
            "INSERT INTO my_table (id, value) VALUES ($1, 'a')"
        );

        assert_eq!(splits[1].params.as_slice(), &[2]);
        assert_eq!(
            splits[1].stmt,
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
        assert_eq!(splits[0].stmt, "INSERT INTO t (a, b) VALUES ($1, 'lit1')");
        let extracted = splits[0].extract_bind_params(&bind).unwrap();
        assert_eq!(extracted.params_raw().len(), 1);
        assert_eq!(extracted.params_raw()[0].data.as_ref(), b"value_for_param1");

        // Second split: statement uses $1 (renumbered from $2) with literal, bind extracts param 1
        assert_eq!(splits[1].stmt, "INSERT INTO t (a, b) VALUES ($1, 'lit2')");
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
        assert_eq!(splits[0].stmt, "INSERT INTO t (a, b) VALUES ($1, $2)");
        let extracted = splits[0].extract_bind_params(&bind).unwrap();
        assert_eq!(extracted.params_raw().len(), 2);
        assert_eq!(extracted.params_raw()[0].data.as_ref(), b"p1");
        assert_eq!(extracted.params_raw()[1].data.as_ref(), b"p2");

        // Second split: uses param 2 (original $3), renumbered to $1
        assert_eq!(
            splits[1].stmt,
            "INSERT INTO t (a, b) VALUES ($1, 'literal')"
        );
        let extracted = splits[1].extract_bind_params(&bind).unwrap();
        assert_eq!(extracted.params_raw().len(), 1);
        assert_eq!(extracted.params_raw()[0].data.as_ref(), b"p3");
    }

    /// This makes sure that grouping works correctly, as in, it doesn't just split one tuple per statement..
    /// if we have multiple tuples that resolve to the same shard, then they should be grouped together
    /// in the same statement.
    #[test]
    fn test_group_split_requests_by_shard() {
        let sql = "INSERT INTO t (id, value) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')";
        let request = ClientRequest::from(vec![ProtocolMessage::Query(Query::new(sql))]);
        let splits = parse_and_split(sql);

        let mut requests = build_split_requests(&splits, &request).unwrap();

        // Fake routing to be able to assert that it correctly divides them into group.
        for (split, shard) in requests.iter_mut().zip([0, 0, 1, 0]) {
            split.route = Some(Route::write(ShardWithPriority::new_default_unset(
                Shard::Direct(shard),
            )));
        }

        let mut prepared = PreparedStatements::default();
        let grouped = group_split_requests_by_shard(requests, &request, &mut prepared).unwrap();

        assert_eq!(grouped.len(), 2);
        assert_eq!(grouped.tuples, Some(vec![vec![0, 1, 3], vec![2]]));

        assert_eq!(grouped[0].route().shard(), &Shard::Direct(0));

        // First 3 were correctly grouped together, as they were mapped to the same shard.
        assert_eq!(
            grouped[0].query().unwrap().unwrap().query(),
            "INSERT INTO t (id, value) VALUES (1, 'a'), (2, 'b'), (4, 'd')"
        );

        assert_eq!(grouped[1].route().shard(), &Shard::Direct(1));
        assert_eq!(
            grouped[1].query().unwrap().unwrap().query(),
            "INSERT INTO t (id, value) VALUES (3, 'c')"
        );
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
