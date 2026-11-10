use indexmap::{IndexMap, IndexSet};
use pg_raw_parse::{Node, NodeMut, deparse, make, nodes, walk};
use pgdog_config::RewriteMode;
use pgdog_postgres_types::{Array, DataType, Datum};
use smallvec::SmallVec;

use crate::frontend::router::parser::{Cache, Shard, Table};
use crate::frontend::router::sharding::Tables;
use crate::frontend::{ClientRequest, PreparedStatements};
use crate::net::{
    Bind, Parse, ProtocolMessage, Query,
    messages::bind::{Format, Parameter},
};

use super::{Error, InsertSplitRewriteResult, RewritePlan, StatementRewrite};

#[derive(Debug, Clone)]
pub(crate) enum Subquery {
    /// Determine if there's a RangeSubselect within a FROM clause of a SELECT / UPDATE statement.
    FromClause,

    /// SELECT / UPDATE / DELETE with a subquery in a WHERE clause.
    WhereClause,

    /// Select subquery within an INSERT statement with a SELECT FROM
    /// e.g., INSERT INTO .. SELECT FROM generate_series(..)
    InsertSelect,

    /// CTE present in query
    /// e.g., <EXAMPLE>
    /// Inner param `String` is the alias
    Cte(String),

    /// CTE present in WHERE clause
    WhereClauseCte(String),
}

#[derive(Debug, Clone)]
pub(crate) enum InsertSplitPlan {
    // INSERT .. VALUES (..), (..), split into one INSERT per tuple.
    Values(Vec<InsertSplit>),
    // INSERT .. SELECT * FROM unnest($1, ..), split by shards of the rows.
    Unnest(Box<UnnestInsert>),
}

// INSERT INTO .. SELECT * FROM unnest($1::pg_catalog._int8, ..)
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct UnnestInsert {
    // The Bind parameter numbers of the unnest(..) arguments (in argument order) + OIDs of the array elements
    array_params: Vec<(u16, i32)>,
    // INSERT with VALUES (..) instead of unnest(..); only used to route the rows one at a time (there's
    // a perf TODO to remove this later).
    single_row_insert_sql: String,
}

// Target of INSERT INTO .. SELECT <targets> FROM unnest(..) (e.g., u.value or NULL in SELECT u.value, NULL FROM unnest($1) AS u(value)).
// We copy the targets (in the same order as the SELECT) into the VALUES (..) for the INSERT that we use to route the rows (the routing INSERT, aka single_row_insert_sql)
#[derive(Debug, Clone, Copy)]
enum UnnestSelectTarget<'a> {
    // Output column of unnest(..) (e.g., in SELECT u.value FROM unnest($1, $2) AS u(id, value), the second arg would produce u.value)
    // We store the position of that argument, starting at 0 (e.g., 1 for u.value).
    // In the routing INSERT (single_row_insert_sql) for row N, we bind $1 to element N of the first argument, $2 to element N of the second argument, and so on.
    // Therefore, we write the column as $<position + 1> in VALUES (e.g., $2 for u.value).
    UnnestColumn { unnest_argument_index: usize },
    // Constant that's the same for all rows (e.g., NULL or 'VIP'::text), which we copy into VALUES as is.
    Constant(Node<'a>),
}

// Singular decoded unnest(..) array
// We cut this into one array per shard after routing the rows
#[derive(Debug, Clone)]
pub(crate) struct UnnestArray {
    bind_index: usize,
    element_oid: i32,
    format: Format,
    elements: Vec<Datum>,
}

impl UnnestInsert {
    pub(crate) fn build(&self, request: &ClientRequest) -> Result<InsertSplitRewriteResult, Error> {
        let client_bind = request
            .messages
            .iter()
            .find_map(|message| match message {
                ProtocolMessage::Bind(bind) => Some(bind),
                _ => None,
            })
            .ok_or_else(|| Error::MissingParameter(self.array_params[0].0))?;

        let unnest_arrays = self
            .array_params
            .iter()
            .map(|&(param_number, element_oid)| {
                let bind_index = param_number as usize - 1;
                let array_param = client_bind
                    .parameter(bind_index)?
                    .ok_or_else(|| Error::MissingParameter(param_number))?;

                let elements = if array_param.is_null() {
                    vec![]
                } else {
                    Array::decode_typed(array_param.data(), array_param.format(), element_oid)?
                        .elements()
                        .clone()
                };

                Ok(UnnestArray {
                    bind_index,
                    element_oid,
                    format: array_param.format(),
                    elements,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;

        // unnest(..) pads shorter arrays with NULLs, (longest array = the number of rows)
        let row_count = unnest_arrays
            .iter()
            .map(|unnest_array| unnest_array.elements.len())
            .max()
            .unwrap_or(0);

        // If the arrays are empty, we send the original INSERT (inserts no rows)
        if row_count == 0 {
            return Ok(InsertSplitRewriteResult::new(vec![request.clone()]));
        }

        // PERF(jkaczman): We route the rows through the router one at a time (one Router::query call per row) to ensure that the rows use the same sharding logic as VALUES
        // For tables without lookups, we could compute the shards of the rows from the sharding key column directly instead
        // This is especially important if theres TONS of rows; would be a lot of speedup
        let single_row_ast = Cache::get().record(&self.single_row_insert_sql)?.ast;

        let single_row_requests = (0..row_count)
            .map(|row_index| {
                let mut single_row_bind = Bind::new_statement("");
                for unnest_array in &unnest_arrays {
                    let element_param = match unnest_array.elements.get(row_index) {
                        Some(element) if !element.is_null() => {
                            Parameter::new(&element.encode(unnest_array.format)?)
                        }
                        _ => Parameter::new_null(),
                    };
                    single_row_bind.push_param(element_param, unnest_array.format);
                }

                Ok(ClientRequest {
                    messages: vec![
                        ProtocolMessage::Parse(Parse::new_anonymous(&self.single_row_insert_sql)),
                        ProtocolMessage::Bind(single_row_bind),
                    ],
                    ast: Some(single_row_ast.clone()),
                    ..Default::default()
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;

        Ok(InsertSplitRewriteResult::new_unnest_insert(
            single_row_requests,
            unnest_arrays,
        ))
    }
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
                .ok_or_else(|| Error::MissingParameter(*param))?;
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

    let query = request.query()?.ok_or_else(|| Error::EmptyQuery)?;
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
        unnest_arrays: None,
    })
}

// We send the original INSERT to all shards that the router sent rows to with the unnest(..) arrays cut down to the rows of each shard.
// The single-row requests only route the rows so we always group unnest INSERTs (even if all rows go to different shards).
pub(crate) fn group_unnest_insert_by_shard(
    single_row_requests: Vec<ClientRequest>,
    request: &ClientRequest,
    unnest_arrays: Vec<UnnestArray>,
) -> Result<InsertSplitRewriteResult, Error> {
    let mut rows_by_shard: IndexMap<&Shard, Vec<usize>> = IndexMap::new();
    for (row_index, single_row_request) in single_row_requests.iter().enumerate() {
        rows_by_shard
            .entry(single_row_request.route().shard())
            .or_default()
            .push(row_index);
    }

    let mut shard_requests = Vec::with_capacity(rows_by_shard.len());
    for row_indices in rows_by_shard.values() {
        let mut shard_request = request.clone();

        for message in &mut shard_request.messages {
            let ProtocolMessage::Bind(shard_bind) = message else {
                continue;
            };

            for unnest_array in &unnest_arrays {
                let shard_elements = row_indices
                    .iter()
                    .map(|&row_index| {
                        unnest_array
                            .elements
                            .get(row_index)
                            .cloned()
                            .unwrap_or(Datum::Null)
                    })
                    .collect();
                let shard_array = Array::new(shard_elements, unnest_array.element_oid)
                    .encode(unnest_array.format)?;

                shard_bind
                    .set_param(unnest_array.bind_index, Parameter::new(&shard_array))
                    .ok_or_else(|| Error::MissingParameter(unnest_array.bind_index as u16 + 1))?;
            }
        }

        shard_request.route = single_row_requests[row_indices[0]].route.clone();
        shard_requests.push(shard_request);
    }

    let tuple_groups: Vec<Vec<usize>> = rows_by_shard.into_values().collect();
    let reordered = !tuple_groups.iter().flatten().is_sorted();

    Ok(InsertSplitRewriteResult {
        requests: shard_requests,
        tuples: reordered.then_some(tuple_groups),
        unnest_arrays: None,
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

// If the unnest(..) argument is a Bind parameter with a type cast (e.g., $1::pg_catalog._int8), this returns the parameter and the OID of the array elements.
// This reads the type from the last part of the name, since the first part can be the schema (e.g., pg_catalog in pg_catalog._int8).
fn typed_array_param<'a>(arg: Node<'a>) -> Option<(&'a nodes::ParamRef, i32)> {
    let Node::TypeCast(cast) = arg else {
        return None;
    };

    let param_ref = param_ref_under_casts(arg)?;
    let type_name = cast.type_name()?.names().into_iter().next_back()?.sval()?;
    let type_oid = DataType::pg_catalog_type_to_datatype(type_name)?.to_oid()?;

    Some((param_ref, type_oid))
}

fn param_ref_under_casts<'a>(mut node: Node<'a>) -> Option<&'a nodes::ParamRef> {
    while let Node::TypeCast(cast) = node {
        node = cast.arg();
    }

    match node {
        Node::ParamRef(param_ref) => Some(param_ref),
        _ => None,
    }
}

// If the INSERT selects from unnest(..), returns the SELECT, the RangeFunction and the unnest(..) call.
fn find_unnest_call(
    insert: &nodes::InsertStmt,
) -> Option<(&nodes::SelectStmt, &nodes::RangeFunction, &nodes::FuncCall)> {
    if let Node::SelectStmt(select) = insert.select_stmt()
        && let Some(Node::RangeFunction(unnest_range_function)) = select.from_clause().first()
        && let Some(Node::NodeList(node_list)) = unnest_range_function.functions().first()
        && let Some(Node::FuncCall(unnest_call)) = node_list.first()
        && let Some(func_name) = unnest_call.funcname().first()
        && let Some(func_name) = func_name.as_str()
        && func_name.eq("unnest")
    {
        return Some((select, unnest_range_function, unnest_call));
    }

    None
}

// The table name and the column names that Postgres uses for the unnest(..) output.
// We look up the column references in the SELECT (e.g., u.id) in these names.
// E.g., for SELECT .. FROM unnest($1, $2) AS u(id, value), the table name is u and the column names are id and value.
struct UnnestOutputNames<'a> {
    table_name: &'a str,
    column_names: Vec<&'a str>,
}

impl<'a> UnnestOutputNames<'a> {
    // We use the same table name and column names that Postgres uses for the unnest(..) output.
    // If the unnest(..) call doesn't have an alias, Postgres would call the table "unnest" (after the function)
    // For the column that the N-th argument produces (counting from 1), Postgres uses the N-th column name in the alias if the alias has N or more column names (e.g., id for the first argument in AS u(id, value)).
    // Otherwise, if unnest(..) has one argument, Postgres would call the column after the table (e.g., u in unnest($1) AS u).
    // Otherwise, Postgres would call the column "unnest".
    fn new(unnest_range_function: &'a nodes::RangeFunction, unnest_argument_count: usize) -> Self {
        let alias = unnest_range_function.alias();

        let table_name = alias
            .and_then(|alias| alias.aliasname())
            .unwrap_or("unnest");

        let column_aliases: Vec<&str> = alias
            .map(|alias| {
                alias
                    .colnames()
                    .into_iter()
                    .filter_map(|column_alias| column_alias.as_str())
                    .collect()
            })
            .unwrap_or_default();

        let column_names = (0..unnest_argument_count)
            .map(|unnest_argument_index| {
                if let Some(column_alias) = column_aliases.get(unnest_argument_index) {
                    column_alias
                } else if unnest_argument_count == 1 {
                    table_name
                } else {
                    "unnest"
                }
            })
            .collect();

        Self {
            table_name,
            column_names,
        }
    }

    // Returns the positions (starting at 0) of the unnest(..) arguments that the column reference selects.
    // E.g., for SELECT .. FROM unnest($1, $2) AS u(id, value), we return [1] (the second argument, $2) for value or u.value, and [0, 1] (both arguments) for * or u.*.
    // Returns None if the column reference refers to a table that's different than the unnest(..) output (e.g., other.id) if the column reference refers to a column that the output of the unnest(..) doesn't include, or if two output columns have the name in the column reference.
    fn unnest_argument_indexes(&self, column_ref: &nodes::ColumnRef) -> Option<Vec<usize>> {
        let fields: Vec<Node<'_>> = column_ref.fields().into_iter().collect();

        let (referenced_table_name, column_field) = match fields.as_slice() {
            [column_field] => (None, *column_field),
            [table_field, column_field] => (Some(table_field.as_str()?), *column_field),
            _ => return None,
        };

        if referenced_table_name
            .is_some_and(|referenced_table_name| referenced_table_name != self.table_name)
        {
            return None;
        }

        if let Node::A_Star(_) = column_field {
            return Some((0..self.column_names.len()).collect());
        }

        let referenced_column_name = column_field.as_str()?;

        let matching_unnest_argument_indexes: Vec<usize> = self
            .column_names
            .iter()
            .enumerate()
            .filter(|(_, column_name)| **column_name == referenced_column_name)
            .map(|(unnest_argument_index, _)| unnest_argument_index)
            .collect();

        // We only get here for column names since we return early for * and u.* above.
        // If none of the unnest(..) output columns are the name, Postgres rejects the reference as unknown.
        // If two unnest(..) output columns are the name (e.g., two columns named "unnest" without column aliases), Postgres rejects the reference as ambiguous.
        // In both cases, we can't map the reference to one unnest(..) argument, so we return None here.
        if matching_unnest_argument_indexes.len() != 1 {
            return None;
        }

        Some(matching_unnest_argument_indexes)
    }
}

// Returns true for constants with or without a type cast, e.g., NULL, 'VIP' and 'VIP'::text.
pub(crate) fn is_constant(node: Node<'_>) -> bool {
    match node {
        Node::A_Const(_) => true,
        Node::TypeCast(type_cast) => matches!(type_cast.arg(), Node::A_Const(_)),
        _ => false,
    }
}

// Maps the targets of the SELECT to unnest(..) output columns (including * and u.*) and constants.
// Returns None if the SELECT has targets other than unnest(..) output columns and constants (e.g., u.id + 1).
fn unnest_select_targets<'a>(
    select: &'a nodes::SelectStmt,
    unnest_range_function: &nodes::RangeFunction,
    unnest_argument_count: usize,
) -> Option<Vec<UnnestSelectTarget<'a>>> {
    let unnest_output_names = UnnestOutputNames::new(unnest_range_function, unnest_argument_count);

    let mut select_targets = Vec::new();
    for target in select.target_list() {
        let target_value = target.val();

        if let Node::ColumnRef(column_ref) = target_value {
            let unnest_argument_indexes =
                unnest_output_names.unnest_argument_indexes(column_ref)?;

            for unnest_argument_index in unnest_argument_indexes {
                select_targets.push(UnnestSelectTarget::UnnestColumn {
                    unnest_argument_index,
                });
            }
        } else if is_constant(target_value) {
            select_targets.push(UnnestSelectTarget::Constant(target_value));
        } else {
            return None;
        }
    }

    Some(select_targets)
}

// We only split INSERT INTO .. SELECT .. FROM unnest(..) if all unnest(..) arguments are Bind parameters with an array type cast (e.g., $1::pg_catalog._int8).
// We need the arrays in the Bind to cut the arrays into one Bind per shard, and we need the type cast to decode the arrays (when we have a text array such as {1,2,3} we have no idea what the type of the elements are)
// If the SELECT has clauses other than FROM unnest(..) (e.g., WHERE or LIMIT), Postgres applies those clauses to all rows at once.
// Therefore, cutting the rows into one INSERT per shard would change the result (e.g., LIMIT 1 would insert one row on all shards instead of one row in total), so we don't split those INSERTs, yet.
fn plan_unnest_insert(
    insert: &nodes::InsertStmt,
    select: &nodes::SelectStmt,
    unnest_range_function: &nodes::RangeFunction,
    unnest_call: &nodes::FuncCall,
) -> Result<Option<UnnestInsert>, Error> {
    let only_unnest = select.from_clause().len() == 1
        && unnest_range_function.functions().len() == 1
        && !unnest_range_function.ordinality
        && matches!(select.where_clause(), Node::None)
        && select.group_clause().is_empty()
        && matches!(select.having_clause(), Node::None)
        && select.sort_clause().is_empty()
        && matches!(select.limit_count(), Node::None)
        && matches!(select.limit_offset(), Node::None)
        && select.distinct_clause().is_empty()
        && select.with_clause().is_none()
        && insert.with_clause().is_none();

    if !only_unnest {
        return Ok(None);
    }

    let Some(array_params) = unnest_call
        .args()
        .into_iter()
        .map(|arg| {
            typed_array_param(arg)
                .map(|(param_ref, element_oid)| (param_ref.number as u16, element_oid))
        })
        .collect::<Option<Vec<_>>>()
        .filter(|array_params| !array_params.is_empty())
    else {
        return Ok(None);
    };

    let Some(select_targets) =
        unnest_select_targets(select, unnest_range_function, array_params.len())
    else {
        return Ok(None);
    };

    let mut single_row_insert_sql = String::new();
    make::try_owned(|mem| {
        let mut single_row_insert = mem.make_unique(insert);

        let values: Vec<_> = select_targets
            .iter()
            .map(|select_target| match *select_target {
                UnnestSelectTarget::UnnestColumn {
                    unnest_argument_index,
                } => {
                    let mut value_param = mem.make_node::<nodes::ParamRef>();
                    value_param
                        .as_mut()
                        .set_number(unnest_argument_index as i32 + 1);
                    value_param.uncast()
                }

                UnnestSelectTarget::Constant(constant) => mem.make_unique(constant),
            })
            .collect();

        let mut values_select = mem.make_node::<nodes::SelectStmt>();
        values_select
            .as_mut()
            .set_values_lists(mem.make_list(&[mem.make_list(&values)]));

        single_row_insert
            .as_mut()
            .set_select_stmt(values_select.uncast());
        single_row_insert
            .as_mut()
            .set_on_conflict_clause(mem.make_unique(None::<&nodes::OnConflictClause>));
        single_row_insert
            .as_mut()
            .set_returning_clause(mem.make_unique(None::<&nodes::ReturningClause>));

        single_row_insert_sql = deparse(&*single_row_insert)?.as_str().to_string();

        Ok::<_, Error>(single_row_insert)
    })?;

    Ok(Some(UnnestInsert {
        array_params,
        single_row_insert_sql,
    }))
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

        let tables = Tables::new(self.schema);
        if let Some((select, unnest_range_function, unnest_call)) = find_unnest_call(insert)
            && let Some(relation) = insert.relation()
            && tables
                .sharded(Table::from(relation))
                .is_some_and(|sharded_table| sharded_table.insert_sends_key_column(insert))

            // Without a column list, the router reads the columns from the schema, so we only split if the schema has the table (otherwise, the router can't find the keys, and the rows would go to random shards).
            && (!insert.cols().is_empty()
                || self
                    .db_schema
                    .table(Table::from(relation), self.user, self.search_path)
                    .is_some())
            && let Some(unnest_insert) =
                plan_unnest_insert(insert, select, unnest_range_function, unnest_call)?
        {
            plan.insert_split = Some(InsertSplitPlan::Unnest(Box::new(unnest_insert)));
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
        let mut values_splits = Vec::with_capacity(splits.len());
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

            values_splits.push(InsertSplit {
                params,
                stmt,
                statement_name,
            });
        }

        plan.insert_split = Some(InsertSplitPlan::Values(values_splits));

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
    use crate::backend::schema::Schema;
    use crate::backend::{ShardedTables, ShardingSchema};
    use crate::frontend::PreparedStatements;
    use crate::frontend::client::QueryTimestamps;
    use crate::frontend::router::parser::{Route, ShardWithPriority, StatementRewriteContext};
    use crate::frontend::router::sharding::ShardedTable;
    use crate::net::messages::bind::{Format, Parameter};

    fn default_db_schema() -> Schema {
        Schema::default()
    }

    fn default_schema() -> ShardingSchema {
        let sharded_tables = ["my_table", "t"]
            .map(|name| ShardedTable {
                name: Some(name.into()),
                column: "id".into(),
                ..Default::default()
            })
            .to_vec();

        ShardingSchema {
            shards: 2,
            tables: ShardedTables::new(
                sharded_tables,
                vec![],
                false,
                pgdog_config::SystemCatalogsBehavior::default(),
            ),
            rewrite: Rewrite {
                enabled: true,
                split_inserts: RewriteMode::Rewrite,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn parse_and_plan(sql: &str) -> RewritePlan {
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
        plan
    }

    fn parse_and_split(sql: &str) -> Vec<InsertSplit> {
        match parse_and_plan(sql).insert_split {
            Some(InsertSplitPlan::Values(splits)) => splits,
            _ => Vec::new(),
        }
    }

    // check that we plan the unnest(..) split for `INSERT .. SELECT * FROM unnest(..)` with typed array parameters
    #[test]
    fn test_split_insert_with_unnest_and_params() {
        let plan = parse_and_plan(
            "INSERT INTO my_table (id, value) SELECT * FROM unnest($1::pg_catalog._int8, $2::pg_catalog._int8)",
        );

        assert!(matches!(
            plan.insert_split,
            Some(InsertSplitPlan::Unnest(_))
        ));

        // Without a column list and without the table in the schema, the router can't find the keys, so we don't split the INSERT.
        let plan = parse_and_plan(
            "INSERT INTO my_table SELECT * FROM unnest($1::pg_catalog._int8, $2::pg_catalog._int8)",
        );

        assert!(plan.insert_split.is_none());
    }

    // To route the rows of INSERT .. SELECT .. FROM unnest(..), we build an INSERT .. VALUES (..) with one row (the routing INSERT), and we route that INSERT one time per row of the unnest(..) arrays.
    // We only use the routing INSERT to find the shards.
    // The shards get the INSERT that the client sent (with the arrays cut down to the rows that the router thinks should be sent to that corresponding shard)
    // The routing INSERT uses $1 for the element of the first unnest(..) argument, $2 for the element of the second argument, and so on.
    //
    // We check these cases.
    // - We split INSERT .. SELECT <targets> FROM unnest(..) if the targets are unnest(..) output columns (including *) and constants (with or without a type cast, e.g., NULL and 'VIP'::text).
    // - The routing INSERT has the targets in the same order as the SELECT.
    //   E.g., for SELECT value, 'VIP'::text, id FROM unnest($1, $2) AS u(id, value), the routing INSERT has VALUES ($2, 'VIP'::text, $1).
    // - We don't split the INSERT if the targets have expressions (e.g., id + 1 or 1 + 1), ambiguous column names (e.g., unnest when both arguments produce a column named unnest) or references to a table other than the unnest(..) output (e.g., other.id).
    #[test]
    fn test_split_insert_with_unnest_select_targets() {
        let split_cases = [
            (
                "INSERT INTO my_table (id, value) SELECT * FROM unnest($1::pg_catalog._int8, $2::pg_catalog._text)",
                "INSERT INTO my_table (id, value) VALUES ($1, $2)",
                vec![(1, 20), (2, 25)],
            ),
            (
                "INSERT INTO my_table (id, status, value) SELECT u.id, NULL, u.value FROM unnest($1::pg_catalog._int8, $2::pg_catalog._text) AS u(id, value)",
                "INSERT INTO my_table (id, status, value) VALUES ($1, NULL, $2)",
                vec![(1, 20), (2, 25)],
            ),
            (
                "INSERT INTO my_table (value, status, id) SELECT value, 'VIP'::text, id FROM unnest($1::pg_catalog._int8, $2::pg_catalog._text) AS u(id, value)",
                "INSERT INTO my_table (value, status, id) VALUES ($2, 'VIP'::text, $1)",
                vec![(1, 20), (2, 25)],
            ),
            (
                "INSERT INTO my_table (id, value, status) SELECT u.*, NULL FROM unnest($1::pg_catalog._int8, $2::pg_catalog._text) AS u(id, value)",
                "INSERT INTO my_table (id, value, status) VALUES ($1, $2, NULL)",
                vec![(1, 20), (2, 25)],
            ),
            (
                "INSERT INTO my_table (id) SELECT u FROM unnest($1::pg_catalog._int8) AS u",
                "INSERT INTO my_table (id) VALUES ($1)",
                vec![(1, 20)],
            ),
        ];

        for (sql, expected_single_row_insert_sql, expected_array_params) in split_cases {
            let Some(InsertSplitPlan::Unnest(unnest_insert)) = parse_and_plan(sql).insert_split
            else {
                panic!("expected an unnest(..) split for {sql}");
            };

            assert_eq!(
                *unnest_insert,
                UnnestInsert {
                    array_params: expected_array_params,
                    single_row_insert_sql: expected_single_row_insert_sql.to_string(),
                },
                "unnest(..) split for {sql}"
            );
        }

        let not_split_cases = [
            "INSERT INTO my_table (id, value) SELECT id + 1, value FROM unnest($1::pg_catalog._int8, $2::pg_catalog._text) AS u(id, value)",
            "INSERT INTO my_table (id, value) SELECT unnest, unnest FROM unnest($1::pg_catalog._int8, $2::pg_catalog._text)",
            "INSERT INTO my_table (id) SELECT other.id FROM unnest($1::pg_catalog._int8) AS u(id)",
            "INSERT INTO my_table (id) SELECT u.missing FROM unnest($1::pg_catalog._int8) AS u(id)",
            "INSERT INTO my_table (id) SELECT s.u.id FROM unnest($1::pg_catalog._int8) AS u(id)",
            "INSERT INTO my_table (id) SELECT other.* FROM unnest($1::pg_catalog._int8) AS u(id)",
            "INSERT INTO my_table (id, value, pos) SELECT * FROM unnest($1::pg_catalog._int8, $2::pg_catalog._text) WITH ORDINALITY",
        ];

        for sql in not_split_cases {
            assert!(
                parse_and_plan(sql).insert_split.is_none(),
                "expected no split for {sql}"
            );
        }
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
