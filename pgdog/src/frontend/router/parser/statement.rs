use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::StatementParameters;
use crate::util::ResultControlFlowExt;
use itertools::*;
use pg_raw_parse::walk::Recurse;
use pg_raw_parse::{Node, list, nodes, walk};
use std::ops::ControlFlow;

fn advisory_locks_from_func_call(
    func: &nodes::FuncCall,
    bind: Option<StatementParameters<'_>>,
    values_columns: Option<&ValuesColumns<'_>>,
) -> Vec<AdvisoryLock> {
    let mut name_parts = func.funcname().into_iter().filter_map(Node::as_str);

    if func.funcname().len() != 1 {
        return Vec::new();
    }
    let name = name_parts.next().unwrap();

    let (unlock, scope) = match name {
        "pg_advisory_lock"
        | "pg_advisory_lock_shared"
        | "pg_try_advisory_lock"
        | "pg_try_advisory_lock_shared" => (false, LockScope::Session),
        "pg_advisory_xact_lock"
        | "pg_advisory_xact_lock_shared"
        | "pg_try_advisory_xact_lock"
        | "pg_try_advisory_xact_lock_shared" => (false, LockScope::Transaction),
        // Session-scoped unlocks. xact locks can't be released by name;
        // Postgres drops them automatically at COMMIT/ROLLBACK.
        "pg_advisory_unlock" => (true, LockScope::Session),
        "pg_advisory_unlock_all" => {
            return vec![AdvisoryLock {
                id: None,
                unlock: true,
                scope: LockScope::Session,
            }];
        }
        _ => return Vec::new(),
    };

    let Some(arg) = func.args().into_iter().next() else {
        return vec![AdvisoryLock {
            id: None,
            unlock,
            scope,
        }];
    };

    // Fast path: the key is a literal / param / cast we can resolve directly.
    if let Some(id) = integer_arg(arg, bind) {
        return vec![AdvisoryLock {
            id: Some(id),
            unlock,
            scope,
        }];
    }

    // If the argument is a parameter placeholder ($1) and we have no Bind message,
    // this is just a prepared statement being parsed — the lock isn't actually
    // being taken yet. Return empty so we don't route as if a lock is held.
    if bind.is_none() && is_param_ref(arg) {
        return Vec::new();
    }

    // Slow path: `SELECT pg_advisory_lock(value) FROM (VALUES (1),(2)) AS t(value)`.
    // The function is called once per row, so we emit one lock per resolved value.
    if let Node::ColumnRef(cref) = arg
        // FIXME: Don't assume the name is unqualified
        && let Some(col) = last_column_name(cref.fields())
        && let Some(rows) = values_columns.and_then(|m| m.get(col))
    {
        return rows
            .iter()
            // Skip unresolvable param refs when there is no Bind.
            .filter(|v| bind.is_some() || !is_param_ref(**v))
            .map(|v| AdvisoryLock {
                id: integer_arg(*v, bind),
                unlock,
                scope,
            })
            .collect();
    }

    vec![AdvisoryLock {
        id: None,
        unlock,
        scope,
    }]
}

fn last_column_name<'a>(fields: impl IntoIterator<Item = Node<'a>>) -> Option<&'a str> {
    fields.into_iter().last().and_then(Node::as_str)
}

/// Map from unqualified VALUES column alias to the list of value nodes — one
/// per row — introduced by a `FROM (VALUES (...), ...) AS t(col, ...)` in the
/// current SELECT's FROM clause.
type ValuesColumns<'a> = std::collections::HashMap<Cow<'a, str>, Vec<Node<'a>>>;

fn collect_values_columns(stmt: &nodes::SelectStmt) -> Option<ValuesColumns<'_>> {
    let Node::RangeSubselect(rs) = stmt.from_clause().into_iter().exactly_one().ok()? else {
        return None;
    };
    let alias = rs.alias();
    let Node::SelectStmt(s) = rs.subquery() else {
        return None;
    };
    if s.values_lists().is_empty() {
        return None;
    }
    let colnames = alias
        .map(|a| {
            a.colnames()
                .into_iter()
                .filter_map(Node::as_str)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let values = s
        .values_lists()
        .into_iter()
        .map(|values| values.expect_node_list())
        .flat_map(|row| row.into_iter().enumerate())
        .map(|(i, v)| {
            let colname = colnames
                .get(i)
                .map(|&s| Cow::Borrowed(s))
                .unwrap_or_else(|| format!("column{}", i + 1).into());
            (colname, v)
        })
        .into_group_map();
    Some(values)
}

fn integer_arg(node: Node<'_>, bind: Option<StatementParameters<'_>>) -> Option<i64> {
    match node {
        Node::A_Const(a) => a.val()?.numeric_value(),
        Node::TypeCast(c) => integer_arg(c.arg(), bind),
        Node::ParamRef(param_ref) => {
            let index = (param_ref.number as usize).checked_sub(1)?;
            let param = bind?.parameter(index).ok()??;
            param.decode::<i64>()
        }
        _ => None,
    }
}

/// Check whether a node is (or wraps) a parameter placeholder (`$N`).
fn is_param_ref(node: Node<'_>) -> bool {
    match node {
        Node::ParamRef(_) => true,
        Node::TypeCast(cast) => is_param_ref(cast.arg()),
        _ => false,
    }
}

use super::{
    super::sharding::Value as ShardingValue, Column, Error, Table, Value,
    explain_trace::ExplainRecorder,
};

/// Lifetime of an advisory lock.
///
/// Used by the query engine to decide whether the lock should survive
/// COMMIT/ROLLBACK (`Session`) or be dropped along with the transaction
/// (`Transaction` — the `pg_advisory_xact_lock*` family).
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum LockScope {
    Session,
    Transaction,
}

/// A pg_advisory_lock / pg_advisory_unlock call observed in a statement.
///
/// `id` is `None` when the key isn't a literal we can resolve (parameter placeholder,
/// subquery, etc.) or when the call takes no key at all (`pg_advisory_unlock_all()`).
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct AdvisoryLock {
    pub id: Option<i64>,
    pub unlock: bool,
    pub scope: LockScope,
}

/// Set of advisory locks discovered while walking a statement.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AdvisoryLocks {
    locks: HashSet<AdvisoryLock>,
}

impl AdvisoryLocks {
    pub fn iter(&self) -> impl Iterator<Item = &AdvisoryLock> {
        self.locks.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.locks.is_empty()
    }

    pub fn len(&self) -> usize {
        self.locks.len()
    }

    /// True if any advisory lock (pg_advisory_lock, etc.) was taken.
    pub fn has_lock(&self) -> bool {
        self.locks.iter().any(|l| !l.unlock)
    }

    /// True if an unlock call appears in the statement.
    pub fn has_unlock(&self) -> bool {
        self.locks.iter().any(|l| l.unlock)
    }
}

/// Accumulator shared across statement walkers — lets a single traversal
/// collect tables and advisory locks without walking the AST twice.
#[derive(Debug, Clone, Default)]
struct Walk<'a> {
    tables: Vec<Table<'a>>,
    advisory_locks: HashSet<AdvisoryLock>,
}
use crate::{
    backend::{Schema, ShardingSchema},
    frontend::router::{
        parser::{Shard, ee::ParserHooks},
        round_robin,
        sharding::{
            ContextBuilder, LookupTable, PendingLookup, ResolvedLookups, SchemaSharder,
            ShardedTable, Tables, lookup,
        },
    },
    net::{messages::Format, parameter::ParameterValue},
};
use pgdog_config::LookupResult;

/// Context for searching a SELECT statement, tracking table aliases.
#[derive(Debug, Default, Clone)]
struct SearchContext<'a> {
    /// Maps alias -> full Table (including schema)
    aliases: HashMap<&'a str, Table<'a>>,
    /// The primary table from the FROM clause (if simple)
    table: Option<Table<'a>>,
}

impl<'a> SearchContext<'a> {
    /// Build context from a FROM clause, extracting table aliases.
    fn from_from_clause(nodes: &'a list::NodeList) -> Self {
        let mut aliases = HashMap::new();

        for node in nodes {
            Self::extract_alias_from_node(&mut aliases, node);
        }

        let table = nodes
            .into_iter()
            .exactly_one()
            .ok()
            .and_then(|n| Table::try_from(n).ok());

        Self { aliases, table }
    }

    fn extract_alias_from_node(aliases: &mut HashMap<&'a str, Table<'a>>, node: Node<'a>) {
        match node {
            Node::RangeVar(rv) if let Some(alias) = rv.alias() => {
                let table = Table::from(rv);
                aliases.insert(alias.aliasname().expect("alias name always present"), table);
            }

            Node::JoinExpr(join) => {
                Self::extract_alias_from_node(aliases, join.larg());
                Self::extract_alias_from_node(aliases, join.rarg());
            }

            Node::RangeSubselect(subselect) if let Some(alias) = subselect.alias() => {
                // For subselects, we don't have a real table name
                // but we record the alias anyway for future use
                let aliasname = alias.aliasname().expect("alias name always present");
                aliases.insert(
                    aliasname,
                    Table {
                        name: aliasname,
                        schema: None,
                        alias: None,
                    },
                );
            }

            _ => {}
        }
    }

    /// Resolve a table reference (which may be an alias) to the actual Table.
    fn resolve_table(&self, name: &str) -> Option<Table<'a>> {
        self.aliases.get(name).copied()
    }
}

#[derive(Debug)]
enum SearchResult<'a> {
    Column(Column<'a>),
    Value(Value<'a>),
    Values(Vec<Value<'a>>),
}

struct ValueIterator<'a, 'b> {
    source: &'b SearchResult<'a>,
    pos: usize,
}

impl<'a, 'b> Iterator for ValueIterator<'a, 'b> {
    type Item = &'b Value<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = match self.source {
            SearchResult::Value(val) => {
                if self.pos == 0 {
                    Some(val)
                } else {
                    None
                }
            }
            SearchResult::Values(values) => values.get(self.pos),
            _ => None,
        };

        self.pos += 1;

        next
    }
}

impl<'a> SearchResult<'a> {
    fn iter<'b>(&'b self) -> ValueIterator<'a, 'b> {
        ValueIterator {
            source: self,
            pos: 0,
        }
    }
}

/// Context for looking up table columns from the database schema.
/// Used for INSERT statements without explicit column lists.
pub struct SchemaLookupContext<'a> {
    /// The loaded database schema.
    pub db_schema: &'a Schema,
    /// The database user (for resolving $user in search_path).
    pub user: &'a str,
    /// The search_path parameter (for table resolution).
    pub search_path: Option<&'a ParameterValue>,
}

pub struct StatementParser<'a, 'b, 'c> {
    stmt: pg_raw_parse::Node<'a>,
    bind: Option<StatementParameters<'b>>,
    schema: &'b ShardingSchema,
    recorder: Option<&'c mut ExplainRecorder>,
    /// Optional schema lookup context for INSERT without column list.
    schema_lookup: Option<SchemaLookupContext<'b>>,
    hooks: ParserHooks,
    /// Cached walk result (tables + advisory locks).
    cached_walk: Option<Walk<'a>>,
    /// Cached result of all_omnisharded check (None = not yet computed)
    all_omnisharded: Option<bool>,
    /// Sharding key lookups that missed the cache and need to be resolved.
    pending_lookups: Vec<PendingLookup>,
    resolved_lookups: Option<&'b ResolvedLookups>,
}

impl<'a, 'b: 'a, 'c> StatementParser<'a, 'b, 'c> {
    pub(crate) fn new(
        stmt: Node<'a>,
        bind: Option<StatementParameters<'b>>,
        schema: &'b ShardingSchema,
        recorder: Option<&'c mut ExplainRecorder>,
    ) -> Self {
        Self {
            stmt,
            bind,
            schema,
            recorder,
            schema_lookup: None,
            hooks: ParserHooks::default(),
            cached_walk: None,
            all_omnisharded: None,
            pending_lookups: Vec::new(),
            resolved_lookups: None,
        }
    }

    /// Attach sharding key translations resolved for this statement.
    /// Consulted before the lookup cache, so a routing pass that runs
    /// after its pending lookups resolved can't miss.
    pub(crate) fn set_resolved_lookups(&mut self, resolved: &'b ResolvedLookups) {
        self.resolved_lookups = Some(resolved);
    }

    fn walk(&mut self) -> &Walk<'a> {
        if self.cached_walk.is_none() {
            self.cached_walk = Some(self.run_walk());
        }
        self.cached_walk.as_ref().unwrap()
    }

    /// Get extracted tables, caching the result.
    pub(crate) fn tables(&mut self) -> &[Table<'a>] {
        &self.walk().tables
    }

    /// Check if all tables in the query are in the omnisharded config.
    /// Result is cached after first computation.
    pub(crate) fn is_all_omnisharded(&mut self) -> bool {
        if let Some(cached) = self.all_omnisharded {
            return cached;
        }

        let omnishards = self.schema.tables.omnishards();
        let tables = self.tables();

        let result = !omnishards.is_empty()
            && !tables.is_empty()
            && tables
                .iter()
                .all(|table| omnishards.contains_key(table.name));

        self.all_omnisharded = Some(result);
        result
    }

    /// Set the schema lookup context for INSERT without column list.
    pub fn with_schema_lookup(mut self, ctx: SchemaLookupContext<'b>) -> Self {
        self.schema_lookup = Some(ctx);
        self
    }

    /// Record a sharding key match.
    fn record_sharding_key(&mut self, shard: &Shard, column: Column<'_>, value: &Value<'_>) {
        self.hooks
            .record_sharding_key(shard, &column, value, self.bind);

        if let Some(recorder) = self.recorder.as_mut() {
            let col_str = if let Some(table) = column.table {
                format!("{}.{}", table, column.name)
            } else {
                column.name.to_string()
            };
            let description = match value {
                Value::Placeholder(pos) => {
                    format!("matched sharding key {} using parameter ${}", col_str, pos)
                }
                _ => format!("matched sharding key {} using constant", col_str),
            };
            recorder.record_entry(Some(shard.clone()), description);
        }
    }

    pub fn shard(&mut self) -> Result<Option<Shard>, Error> {
        // Omnisharded config overrides sharded: if all tables are omnisharded,
        // don't try to find a sharding key - let omnisharded routing handle it
        if self.is_all_omnisharded() {
            return Ok(None);
        }

        let result = self.shard_stmt(self.stmt)?;

        // Key-based sharding succeeded
        if result.is_some() {
            return Ok(result);
        } else if self.schema.schemas.is_empty() {
            return Ok(None);
        }

        // Fallback to schema-based sharding
        // Ensure tables are cached first
        let _ = self.tables();
        let mut schema_sharder = SchemaSharder::default();
        for table in &self.cached_walk.as_ref().unwrap().tables {
            schema_sharder.resolve(table.schema(), &self.schema.schemas);
        }

        if let Some((shard, schema_name)) = schema_sharder.get() {
            if let Some(recorder) = self.recorder.as_mut() {
                recorder.record_entry(
                    Some(shard.clone()),
                    format!("matched schema {}", schema_name),
                );
                self.hooks.record_sharded_schema(&shard, schema_name);
            }
            return Ok(Some(shard));
        }

        Ok(None)
    }

    /// Check that the query references a table that contains a sharded
    /// column. This check is needed in case sharded tables config
    /// doesn't specify a table name and should short-circuit if it does.
    pub fn is_sharded(
        &mut self,
        db_schema: &Schema,
        user: &str,
        search_path: Option<&ParameterValue>,
    ) -> bool {
        // Omnisharded config overrides sharded: if all tables are omnisharded, return false
        if self.is_all_omnisharded() {
            return false;
        }

        let sharded_tables = self.schema.tables.tables();

        // Separate configs with explicit table names from those without
        let (named, nameless): (Vec<_>, Vec<_>) =
            sharded_tables.iter().partition(|t| t.name.is_some());

        for table in self.tables() {
            // Check named sharded table configs (fast path, no schema lookup needed)
            for config in &named {
                if let Some(ref name) = config.name
                    && table.name == name
                {
                    // Also check schema match if specified in config
                    if let Some(ref config_schema) = config.schema
                        && table.schema != Some(config_schema.as_str())
                    {
                        continue;
                    }
                    return true;
                }
            }

            // Check nameless configs by looking up the table in the db schema
            // to see if it has the sharding column
            if !nameless.is_empty()
                && let Some(relation) = db_schema.table(*table, user, search_path)
            {
                for config in &nameless {
                    if relation.has_column(&config.column) {
                        return true;
                    }
                }
            }
        }

        false
    }

    /// Extract all tables referenced in the statement.
    pub(crate) fn extract_tables(&self) -> Vec<Table<'a>> {
        self.run_walk().tables
    }

    /// Extract pg_advisory_lock / pg_advisory_unlock calls with literal integer keys.
    pub fn extract_advisory_locks(&mut self) -> AdvisoryLocks {
        AdvisoryLocks {
            locks: self.walk().advisory_locks.clone(),
        }
    }

    // Are we running? Or walking? MAKE UP YOUR MIND DAMMIT
    fn run_walk(&self) -> Walk<'a> {
        let mut walk = Walk::default();
        self.walk_stmt(self.stmt, &mut walk);
        walk
    }

    fn walk_stmt(&self, stmt: Node<'a>, walk: &mut Walk<'a>) {
        let values_columns = match stmt {
            Node::SelectStmt(s) => collect_values_columns(s),
            _ => None,
        };
        walk::walk_manual::<()>(stmt, |node| match node {
            // Walk the select statement separately, as it may have its own
            // set of VALUES columns.
            Node::SelectStmt(_) => {
                self.walk_stmt(node, walk);
                Recurse::no()
            }

            // Extract any advisory locks that this function call may
            // represent, using the values clause of the current statement
            Node::FuncCall(func) => {
                walk.advisory_locks.extend(advisory_locks_from_func_call(
                    func,
                    self.bind,
                    values_columns.as_ref(),
                ));
                Recurse::no()
            }

            Node::RangeVar(r) => {
                walk.tables.push(Table::from(r));
                Recurse::yes()
            }

            _ => Recurse::yes(),
        });
    }

    fn shard_stmt(&mut self, stmt: Node<'a>) -> Result<Option<Shard>, Error> {
        self.search_stmt(stmt).break_value().transpose()
    }

    fn context_from_relation(&self, relation: Option<&'a nodes::RangeVar>) -> SearchContext<'a> {
        let mut ctx = SearchContext::default();
        if let Some(range_var) = relation {
            let table = Table::from(range_var);
            ctx.table = Some(table);
            if let Some(alias) = range_var.alias() {
                ctx.aliases
                    .insert(alias.aliasname().expect("Alias name always present"), table);
            }
        }
        ctx
    }

    fn converge(shards: &[Shard]) -> Option<Shard> {
        let shards: HashSet<Shard> = shards.iter().cloned().collect();
        match shards.len() {
            0 => None,
            1 => shards.into_iter().next(),
            _ => {
                let mut multi = vec![];
                for shard in shards.into_iter() {
                    match shard {
                        Shard::All => return Some(Shard::All),
                        Shard::Direct(direct) => multi.push(direct),
                        Shard::Multi(many) => multi.extend(many),
                    }
                }
                Some(Shard::Multi(multi))
            }
        }
    }

    /// Find sharded table config for a column.
    /// Named configs (with explicit table names) match specific table+column.
    /// Column-only configs match any table with that column name.
    fn get_sharded_table(&self, column: Column<'a>) -> Option<&'b ShardedTable> {
        self.get_sharded_table_by_name(column.name, column.table, column.schema)
    }

    /// Find sharded table config by column name (for INSERT without column list).
    fn get_sharded_table_by_name(
        &self,
        column_name: &str,
        table_name: Option<&str>,
        schema: Option<&str>,
    ) -> Option<&'b ShardedTable> {
        // Try named table configs first
        if let Some(table_name) = table_name {
            let column = Column {
                name: column_name,
                table: Some(table_name),
                schema,
            };
            if let Some(sharded_table) = self.schema.tables().get_table(column)
                && sharded_table.name.is_some()
            {
                return Some(sharded_table);
            }
        }

        // Column-only config: user explicitly wants any table with this column to be sharded
        self.schema
            .tables
            .tables()
            .iter()
            .find(|t| t.name.is_none() && t.column == column_name)
    }

    fn compute_shard(
        &mut self,
        column: Column<'a>,
        value: Value<'a>,
    ) -> Result<Option<Shard>, Error> {
        let sharded_table = self.get_sharded_table(column);
        self.compute_shard_for_table(sharded_table, value)
    }

    /// Compute shard for a given sharded table config and value.
    fn compute_shard_for_table(
        &mut self,
        sharded_table: Option<&ShardedTable>,
        value: Value<'a>,
    ) -> Result<Option<Shard>, Error> {
        if let Some(table) = sharded_table {
            // Own the extracted values so the context can borrow them
            // past the match arms below.
            let translated: Option<Arc<str>>;
            let param;
            let context = ContextBuilder::new(table);
            let context = match value {
                Value::Placeholder(pos) => {
                    let bound = self
                        .bind
                        .map(|bind| bind.parameter(pos as usize - 1))
                        .transpose()?
                        .flatten();
                    // Expect params to be accurate.
                    param = if let Some(bound) = bound {
                        bound
                    } else {
                        return Ok(None);
                    };
                    // NULL sharding key broadcasts to all shards
                    if param.is_null() {
                        return Ok(Some(Shard::All));
                    }
                    translated = match param.format() {
                        Format::Text => param
                            .text()
                            .and_then(|text| self.translate_sharding_key(table, text)),
                        // Binary values are decoded to their text form
                        // to translate through the lookup.
                        Format::Binary => {
                            if table.lookup_query.is_some() {
                                ShardingValue::from_param(&param, table.data_type)?
                                    .to_text()?
                                    .and_then(|text| self.translate_sharding_key(table, &text))
                            } else {
                                None
                            }
                        }
                    };
                    match translated.as_deref() {
                        Some(translated) => context.data(translated),
                        None => context.value(ShardingValue::from_param(&param, table.data_type)?),
                    }
                }

                Value::String(val) => {
                    translated = self.translate_sharding_key(table, val);
                    context.data(translated.as_deref().unwrap_or(val))
                }

                Value::Integer(val) => {
                    // The text form is only needed when a lookup is
                    // configured, and itoa formats it on the stack.
                    translated = if table.lookup_query.is_some() {
                        let mut buf = itoa::Buffer::new();
                        self.translate_sharding_key(table, buf.format(val))
                    } else {
                        None
                    };
                    match translated.as_deref() {
                        Some(translated) => context.data(translated),
                        None => context.data(val),
                    }
                }
                Value::Null => return Ok(Some(Shard::All)),
                _ => return Ok(None),
            };

            // Shard-mode lookups never hash, not even into the throwaway
            // first-pass route: the translation is the shard number, and
            // a cold cache contributes no shard at all (the pending
            // lookup resolves and the statement routes again).
            if table.lookup_result == LookupResult::Shard {
                return match translated.as_deref() {
                    Some(translated) => Ok(Some(lookup::parse_shard_index(
                        translated,
                        self.schema.shards,
                    )?)),
                    None => Ok(None),
                };
            }

            Ok(Some(context.shards(self.schema.shards).build()?.apply()?))
        } else {
            Ok(None)
        }
    }

    /// Translate a sharding key value through the table's lookup, if one
    /// is configured. Returns the translated value on a cache hit. On a
    /// cache miss, records a pending lookup and returns `None`; the
    /// query engine resolves it and routes the statement again, or
    /// fails it if the lookup table has no row for the value.
    fn translate_sharding_key(&mut self, table: &ShardedTable, value: &str) -> Option<Arc<str>> {
        let query = table.lookup_query.as_deref()?;

        // Translations resolved for this statement come first: they
        // can't be evicted, unlike cache entries. Both gets run with
        // borrowed keys; the owned key is only built on a miss.
        if let Some(translated) = self
            .resolved_lookups
            .and_then(|resolved| resolved.get_for_table(table, value))
        {
            return Some(translated);
        }

        match self
            .schema
            .tables
            .lookup_cache()
            .get_for_table(table, value)
        {
            Some(translated) => Some(translated),
            None => {
                self.pending_lookups.push(PendingLookup {
                    table: LookupTable::from(table),
                    query: query.to_owned(),
                    value: value.to_owned(),
                });
                None
            }
        }
    }

    /// Take the pending sharding key lookups recorded while parsing.
    pub(crate) fn take_pending_lookups(&mut self) -> Vec<PendingLookup> {
        std::mem::take(&mut self.pending_lookups)
    }

    fn search_stmt(&mut self, stmt: Node<'a>) -> ControlFlow<Result<Shard, Error>> {
        use nodes::{A_Expr_Kind, BoolExprType};

        let ctx = match stmt {
            Node::SelectStmt(s) => SearchContext::from_from_clause(s.from_clause()),
            Node::UpdateStmt(s) => self.context_from_relation(s.relation()),
            Node::DeleteStmt(s) => self.context_from_relation(s.relation()),
            Node::InsertStmt(s) => {
                return match self.search_insert_stmt(s).break_err()? {
                    Some(shard) => ControlFlow::Break(Ok(shard)),
                    None => ControlFlow::Continue(()),
                };
            }
            // FIXME(sage): Do we want to error here?
            _ => return ControlFlow::Continue(()),
        };

        let result = walk::walk_manual(stmt, |node| match node {
            Node::SelectStmt(_) => {
                self.search_stmt(node)?;
                Recurse::no()
            }

            Node::A_Expr(expr) => {
                let expr_name = expr
                    .name()
                    .into_iter()
                    .exactly_one()
                    .ok()
                    .and_then(Node::as_str);
                match expr.kind {
                    A_Expr_Kind::AEXPR_NOT_DISTINCT => {}
                    A_Expr_Kind::AEXPR_OP | A_Expr_Kind::AEXPR_IN | A_Expr_Kind::AEXPR_OP_ANY
                        if expr_name == Some("=") => {}
                    _ => return Recurse::no(),
                }

                let is_any = matches!(expr.kind, A_Expr_Kind::AEXPR_OP_ANY);

                let left = self.search_expr(expr.lexpr(), &ctx)?;
                let right = self.search_expr(expr.rexpr(), &ctx)?;

                let Some(left) = left else {
                    return Recurse::no();
                };

                match (left, right, is_any) {
                    // For ANY expressions with sharding columns, we can't reliably
                    // parse array literals or parameters, so route to all shards.
                    (SearchResult::Column(column), _, true)
                        if self.get_sharded_table(column).is_some() =>
                    {
                        ControlFlow::Break(Ok(Shard::All))
                    }
                    (SearchResult::Column(column), Some(values), false)
                    | (values, Some(SearchResult::Column(column)), false) => {
                        let shards = values
                            .iter()
                            .filter_map(|value| {
                                self.compute_shard_with_ctx(column, value.clone(), &ctx)
                                    .transpose()
                            })
                            .collect::<Result<Vec<_>, _>>()
                            .break_err()?;
                        match Self::converge(&shards) {
                            Some(shard) => ControlFlow::Break(Ok(shard)),
                            None => Recurse::no(),
                        }
                    }
                    _ => Recurse::no(),
                }
            }

            Node::BoolExpr(expr) => {
                // Only AND expressions can determine a shard.
                // OR expressions could route to multiple shards.
                Recurse::recurse_if(expr.boolop == BoolExprType::AND_EXPR)
            }

            _ => Recurse::yes(),
        });

        match result {
            Some(r) => ControlFlow::Break(r),
            None => ControlFlow::Continue(()),
        }
    }

    fn search_expr(
        &mut self,
        node: Node<'a>,
        ctx: &SearchContext<'a>,
    ) -> ControlFlow<Result<Shard, Error>, Option<SearchResult<'a>>> {
        use itertools::Either;

        match node {
            // Value types - these are leaf nodes representing actual values
            Node::A_Const(_) | Node::ParamRef(_) | Node::FuncCall(_) => {
                ControlFlow::Continue(Value::try_from(node).map(SearchResult::Value).ok())
            }

            Node::ColumnRef(c) => {
                let mut column = Column::try_from(c).break_err()?;

                // If column has no table, qualify with context table
                if column.table().is_none()
                    && let Some(ref table) = ctx.table
                {
                    column.qualify(*table);
                }
                ControlFlow::Continue(Some(SearchResult::Column(column)))
            }

            Node::NodeList(l) => ControlFlow::Continue(Some(SearchResult::Values(
                l.into_iter()
                    .filter_map(|n| Value::try_from(n).ok())
                    .collect(),
            ))),

            Node::SelectStmt(_) => self.search_stmt(node).map_continue(|_| None),

            _ => {
                let result = walk::walk_manual(node, |node| {
                    match self
                        .search_expr(node, ctx)
                        .map_break(|b| b.map(Either::Left))?
                    {
                        Some(result) => ControlFlow::Break(Ok(Either::Right(result))),
                        // We're manually recursing
                        None => Recurse::no(),
                    }
                })
                .transpose()
                .break_err()?;

                match result {
                    Some(Either::Left(shard)) => ControlFlow::Break(Ok(shard)),
                    Some(Either::Right(values)) => ControlFlow::Continue(Some(values)),
                    None => ControlFlow::Continue(None),
                }
            }
        }
    }

    /// Compute shard with alias resolution from context.
    fn compute_shard_with_ctx(
        &mut self,
        column: Column<'a>,
        value: Value<'a>,
        ctx: &SearchContext<'a>,
    ) -> Result<Option<Shard>, Error> {
        // Resolve table alias if present
        let resolved_column = if let Some(table_ref) = column.table() {
            if let Some(resolved) = ctx.resolve_table(table_ref.name) {
                Column {
                    name: column.name,
                    table: Some(resolved.name),
                    schema: resolved.schema,
                }
            } else {
                column
            }
        } else {
            column
        };

        let shard = self.compute_shard(resolved_column, value.clone())?;
        if let Some(ref shard) = shard {
            self.record_sharding_key(shard, resolved_column, &value);
        }
        Ok(shard)
    }

    /// Get column names from the INSERT statement, or look them up from schema if not specified.
    fn get_insert_columns(
        &self,
        stmt: &'a nodes::InsertStmt,
        ctx: &SearchContext<'a>,
    ) -> Result<Vec<&'a str>, Error> {
        // First try to get columns from the INSERT statement itself
        let cols = stmt
            .cols()
            .into_iter()
            .map(|n| match n {
                Node::ResTarget(r) => r.name().ok_or(Error::ColumnDecode),
                _ => Err(Error::ColumnDecode),
            })
            .collect::<Result<Vec<_>, _>>()?;

        if !cols.is_empty() {
            Ok(cols)
        // No columns specified in INSERT, try to look them up from schema
        } else if let (Some(table), Some(schema_lookup)) = (ctx.table, &self.schema_lookup)
            && let Some(relation) =
                schema_lookup
                    .db_schema
                    .table(table, schema_lookup.user, schema_lookup.search_path)
        {
            Ok(relation.column_names().collect())
        } else {
            // FIXME(sage): What scenarios are leading to us not being able
            // to look up the columns in the schema? This seems like it should
            // be an error.
            Ok(Vec::new())
        }
    }

    fn search_insert_stmt(&mut self, stmt: &'a nodes::InsertStmt) -> Result<Option<Shard>, Error> {
        let ctx = self.context_from_relation(stmt.relation());

        // Schema-based routing takes priority for INSERTs
        if let Some(table) = ctx.table
            && let Some(schema) = self.schema.schemas.get(table.schema())
        {
            return Ok(Some(schema.shard().into()));
        }

        if let Node::SelectStmt(select_stmt) = stmt.select_stmt() {
            // Get the column names from INSERT INTO table (col1, col2, ...) or from schema
            let columns = self.get_insert_columns(stmt, &ctx)?;

            let mut values_lists = select_stmt
                .values_lists()
                .into_iter()
                .map(|l| l.expect_node_list().into_iter());

            // Multi-row VALUES broadcasts to all shards
            if values_lists.len() > 1 {
                return Ok(Some(Shard::All));
            }

            // Grab either the single VALUES list or the targets list
            let targets = select_stmt
                .target_list()
                .into_iter()
                .map(|t| t.val())
                .collect();
            let row: Vec<_> = values_lists.next().map(|r| r.collect()).unwrap_or(targets);

            for (column_name, target_node) in columns.into_iter().zip(row) {
                let table_name = ctx.table.map(|t| t.name);
                let table_schema = ctx.table.and_then(|t| t.schema);
                let sharded_table =
                    self.get_sharded_table_by_name(column_name, table_name, table_schema);

                if let Ok(value) = Value::try_from(target_node)
                    && let Some(shard) = self.compute_shard_for_table(sharded_table, value)?
                {
                    return Ok(Some(shard));
                }
            }
        };

        // No sharding key literals being inserted, check if any subselects
        // determine the shard
        // FIXME(sage): This has no test coverage. Do we actually need/want this
        // behavior?
        let result = walk::walk_manual(Node::InsertStmt(stmt), |node| match node {
            Node::SelectStmt(_) => {
                self.search_stmt(node)?;
                Recurse::no()
            }
            _ => Recurse::yes(),
        });

        if let Some(shard) = result {
            return shard.map(Some);
        }

        // Round-robin fallback: if table is sharded but no sharding key found,
        // pick a shard at random
        if let Some(table) = ctx.table
            && Tables::new(self.schema).sharded(table).is_some()
        {
            Ok(Some(Shard::Direct(round_robin::next(self.schema.shards))))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod test {
    use crate::frontend::router::sharding::{Mapping, ShardedTable};
    use pgdog_config::{
        DataType, FlexibleType, ShardedMappingConfig, ShardedMappingList, SystemCatalogsBehavior,
    };

    use crate::backend::ShardedTables;
    use crate::net::messages::{Bind, Parameter};

    use super::*;

    fn test_schema() -> ShardingSchema {
        ShardingSchema {
            shards: 3,
            tables: ShardedTables::new(
                vec![
                    ShardedTable {
                        column: "id".into(),
                        name: Some("sharded".into()),
                        ..Default::default()
                    },
                    ShardedTable {
                        column: "sharded_id".into(),
                        ..Default::default()
                    },
                    ShardedTable {
                        column: "list_id".into(),
                        mapping: Mapping::new(vec![ShardedMappingConfig::List(
                            ShardedMappingList {
                                values: vec![FlexibleType::Integer(1), FlexibleType::Integer(2)],
                                shard: 0,
                            },
                        )]),
                        ..Default::default()
                    },
                    // Schema-qualified sharded table with different column name
                    ShardedTable {
                        column: "tenant_id".into(),
                        name: Some("schema_sharded".into()),
                        schema: Some("myschema".into()),
                        ..Default::default()
                    },
                    // Sharding keys translated through a lookup table.
                    ShardedTable {
                        column: "org_id".into(),
                        name: Some("sharded_lookup".into()),
                        data_type: DataType::Varchar,
                        lookup_query: Some(ORG_LOOKUP_QUERY.into()),
                        ..Default::default()
                    },
                    ShardedTable {
                        column: "customer_id".into(),
                        name: Some("sharded_lookup_bigint".into()),
                        lookup_query: Some(CUSTOMER_LOOKUP_QUERY.into()),
                        ..Default::default()
                    },
                ],
                vec![],
                false,
                SystemCatalogsBehavior::default(),
            ),
            ..Default::default()
        }
    }

    fn run_test(stmt: &str, bind: Option<&Bind>) -> Result<Option<Shard>, Error> {
        let schema = test_schema();
        let raw = pg_raw_parse::parse(stmt).unwrap();
        let stmt = raw.stmts().next().unwrap();
        let mut parser = StatementParser::new(stmt, bind.map(Into::into), &schema, None);
        parser.shard()
    }

    /// Run the parser on a statement and return the computed shard along
    /// with the sharding key lookups that missed the cache.
    fn run_lookup_test(
        stmt: &str,
        bind: Option<&Bind>,
        schema: &ShardingSchema,
    ) -> (Option<Shard>, Vec<PendingLookup>) {
        let raw = pg_raw_parse::parse(stmt).unwrap();
        let stmt = raw.stmts().next().unwrap();
        let mut parser = StatementParser::new(stmt, bind.map(Into::into), schema, None);
        let shard = parser.shard().unwrap();
        (shard, parser.take_pending_lookups())
    }

    fn org_lookup_table() -> LookupTable {
        LookupTable {
            schema: None,
            name: Some("sharded_lookup".into()),
            column: "org_id".into(),
        }
    }

    fn customer_lookup_table() -> LookupTable {
        LookupTable {
            schema: None,
            name: Some("sharded_lookup_bigint".into()),
            column: "customer_id".into(),
        }
    }

    const ORG_LOOKUP_QUERY: &str = "SELECT root_org_id FROM org_family_roots WHERE org_id = $1";
    const CUSTOMER_LOOKUP_QUERY: &str = "SELECT root_id FROM customer_roots WHERE customer_id = $1";

    fn varchar_shard(value: &str) -> Shard {
        crate::frontend::router::sharding::shard_value(value, &DataType::Varchar, 3, &vec![], 0)
    }

    #[test]
    fn test_simple_select() {
        let result = run_test("SELECT * FROM sharded WHERE id = 1", None);
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_lookup_cache_hit_translates_key() {
        // The cache lives in the schema's sharded tables, so each
        // test schema starts empty.
        let schema = test_schema();
        schema.tables.lookup_cache().insert(
            org_lookup_table(),
            "org_child_hit".into(),
            "org_root_hit".into(),
        );

        let (shard, pending) = run_lookup_test(
            "SELECT * FROM sharded_lookup WHERE org_id = 'org_child_hit'",
            None,
            &schema,
        );

        // The translated value picks the shard, not the original.
        assert_ne!(
            varchar_shard("org_root_hit"),
            varchar_shard("org_child_hit")
        );
        assert_eq!(shard, Some(varchar_shard("org_root_hit")));
        assert!(pending.is_empty());
    }

    #[test]
    fn test_lookup_cache_hit_bind_parameter() {
        let schema = test_schema();
        schema.tables.lookup_cache().insert(
            org_lookup_table(),
            "org_child_bind".into(),
            "org_root_bind".into(),
        );

        let bind = Bind::new_params("", &[Parameter::new(b"org_child_bind")]);
        let (shard, pending) = run_lookup_test(
            "SELECT * FROM sharded_lookup WHERE org_id = $1",
            Some(&bind),
            &schema,
        );

        assert_eq!(shard, Some(varchar_shard("org_root_bind")));
        assert!(pending.is_empty());
    }

    #[test]
    fn test_lookup_cache_hit_binary_bind_parameter() {
        let schema = test_schema();
        schema
            .tables
            .lookup_cache()
            .insert(customer_lookup_table(), "42".into(), "1000".into());

        // Binary-format parameters are decoded to text before the lookup.
        let bind = Bind::new_params_codes(
            "",
            &[Parameter {
                len: 8,
                data: 42_i64.to_be_bytes().to_vec().into(),
            }],
            &[Format::Binary],
        );
        let (shard, pending) = run_lookup_test(
            "SELECT * FROM sharded_lookup_bigint WHERE customer_id = $1",
            Some(&bind),
            &schema,
        );

        let expected = crate::frontend::router::sharding::shard_value(
            "1000",
            &DataType::Bigint,
            3,
            &vec![],
            0,
        );
        assert_eq!(shard, Some(expected));
        assert!(pending.is_empty());
    }

    #[test]
    fn test_lookup_cache_hit_integer_key() {
        let schema = test_schema();
        schema
            .tables
            .lookup_cache()
            .insert(customer_lookup_table(), "42".into(), "1000".into());

        let (shard, pending) = run_lookup_test(
            "SELECT * FROM sharded_lookup_bigint WHERE customer_id = 42",
            None,
            &schema,
        );

        let expected = crate::frontend::router::sharding::shard_value(
            "1000",
            &DataType::Bigint,
            3,
            &vec![],
            0,
        );
        assert_eq!(shard, Some(expected));
        assert!(pending.is_empty());
    }

    #[test]
    fn test_lookup_cache_miss_records_pending() {
        let schema = test_schema();
        let (shard, pending) = run_lookup_test(
            "SELECT * FROM sharded_lookup WHERE org_id = 'org_child_miss'",
            None,
            &schema,
        );

        // Identity routing until the lookup is resolved.
        assert_eq!(shard, Some(varchar_shard("org_child_miss")));
        assert_eq!(
            pending,
            vec![PendingLookup {
                table: org_lookup_table(),
                query: ORG_LOOKUP_QUERY.into(),
                value: "org_child_miss".into(),
            }]
        );
    }

    #[test]
    fn test_select_with_and() {
        let result = run_test("SELECT * FROM sharded WHERE id = 1 AND name = 'foo'", None).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_select_with_or_returns_none() {
        // OR expressions can't determine a single shard
        let result = run_test("SELECT * FROM sharded WHERE id = 1 OR id = 2", None).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_select_with_subquery() {
        let result = run_test(
            "SELECT * FROM sharded WHERE id IN (SELECT sharded_id FROM other WHERE sharded_id = 1)",
            None,
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_select_with_cte() {
        let result = run_test(
            "WITH cte AS (SELECT * FROM sharded WHERE id = 1) SELECT * FROM cte",
            None,
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_select_with_join() {
        let result = run_test(
            "SELECT * FROM sharded s JOIN other o ON s.id = o.sharded_id WHERE s.id = 1",
            None,
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_select_with_type_cast() {
        let result = run_test("SELECT * FROM sharded WHERE id = '1'::int", None).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_select_from_subquery() {
        let result = run_test(
            "SELECT * FROM (SELECT * FROM sharded WHERE id = 1) AS sub",
            None,
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_select_with_nested_cte() {
        let result = run_test(
            "WITH cte1 AS (SELECT * FROM sharded WHERE id = 1), \
             cte2 AS (SELECT * FROM cte1) \
             SELECT * FROM cte2",
            None,
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_select_no_where_returns_none() {
        let result = run_test("SELECT * FROM sharded", None).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_select_with_in_list() {
        let result = run_test("SELECT * FROM sharded WHERE id IN (1, 2, 3)", None).unwrap();
        // IN with multiple values should return a shard match
        assert!(result.is_some());
    }

    #[test]
    fn test_select_with_not_equals_returns_none() {
        // != operator is not supported for sharding
        let result = run_test("SELECT * FROM sharded WHERE id != 1", None).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_select_with_greater_than_returns_none() {
        // > operator is not supported for sharding
        let result = run_test("SELECT * FROM sharded WHERE id > 1", None).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_select_with_complex_and() {
        let result = run_test(
            "SELECT * FROM sharded WHERE id = 1 AND status = 'active' AND created_at > now()",
            None,
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_select_with_left_join() {
        let result = run_test(
            "SELECT * FROM sharded s LEFT JOIN other o ON s.id = o.sharded_id WHERE s.id = 1",
            None,
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_select_with_multiple_joins() {
        let result = run_test(
            "SELECT * FROM sharded s \
             JOIN other o ON s.id = o.sharded_id \
             JOIN third t ON o.id = t.other_id \
             WHERE s.id = 1",
            None,
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_select_with_exists_subquery() {
        let result = run_test(
            "SELECT * FROM sharded WHERE EXISTS (SELECT 1 FROM other WHERE sharded_id = 1)",
            None,
        )
        .unwrap();
        // EXISTS subquery should find the shard condition inside
        assert!(result.is_some());
    }

    #[test]
    fn test_select_with_scalar_subquery() {
        // Scalar subquery where shard is determined by the subquery's WHERE clause
        let result = run_test(
            "SELECT * FROM sharded WHERE id = (SELECT sharded_id FROM other WHERE sharded_id = 1 LIMIT 1)",
            None,
        )
        .unwrap();
        // The subquery's sharded_id = 1 should be found
        assert!(result.is_some());
    }

    #[test]
    fn test_select_with_recursive_cte() {
        // Recursive CTEs have UNION - we look at the base case
        let result = run_test(
            "WITH RECURSIVE cte AS ( \
                SELECT * FROM sharded WHERE id = 1 \
                UNION ALL \
                SELECT s.* FROM sharded s JOIN cte c ON s.parent_id = c.id \
             ) SELECT * FROM cte",
            None,
        )
        .unwrap();
        // The base case has id = 1
        assert!(result.is_some());
    }

    #[test]
    fn test_select_with_union() {
        let result = run_test(
            "SELECT * FROM sharded WHERE id = 1 UNION SELECT * FROM sharded WHERE id = 2",
            None,
        )
        .unwrap();
        // UNION queries should find at least one shard
        assert!(result.is_some());
    }

    #[test]
    fn test_select_with_nested_subselects() {
        let result = run_test(
            "SELECT * FROM sharded WHERE id IN ( \
                SELECT * FROM other WHERE x IN ( \
                    SELECT y FROM third WHERE sharded_id = 1 \
                ) \
            )",
            None,
        )
        .unwrap();
        // The innermost subquery has sharded_id = 1
        assert!(result.is_some());
    }

    // Bound parameter tests

    #[test]
    fn test_bound_simple_select() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_test("SELECT * FROM sharded WHERE id = $1", Some(&bind)).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_bound_select_with_and() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_test(
            "SELECT * FROM sharded WHERE id = $1 AND name = 'foo'",
            Some(&bind),
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_bound_select_with_or_returns_none() {
        let bind = Bind::new_params("", &[Parameter::new(b"1"), Parameter::new(b"2")]);
        let result = run_test(
            "SELECT * FROM sharded WHERE id = $1 OR id = $2",
            Some(&bind),
        )
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_bound_select_with_subquery() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_test(
            "SELECT * FROM sharded WHERE id IN (SELECT sharded_id FROM other WHERE sharded_id = $1)",
            Some(&bind),
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_bound_select_with_cte() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_test(
            "WITH cte AS (SELECT * FROM sharded WHERE id = $1) SELECT * FROM cte",
            Some(&bind),
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_bound_select_with_join() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_test(
            "SELECT * FROM sharded s JOIN other o ON s.id = o.sharded_id WHERE s.id = $1",
            Some(&bind),
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_bound_select_with_type_cast() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_test("SELECT * FROM sharded WHERE id = $1::int", Some(&bind)).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_bound_select_from_subquery() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_test(
            "SELECT * FROM (SELECT * FROM sharded WHERE id = $1) AS sub",
            Some(&bind),
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_bound_select_with_nested_cte() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_test(
            "WITH cte1 AS (SELECT * FROM sharded WHERE id = $1), \
             cte2 AS (SELECT * FROM cte1) \
             SELECT * FROM cte2",
            Some(&bind),
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_bound_select_with_in_list() {
        let bind = Bind::new_params(
            "",
            &[
                Parameter::new(b"1"),
                Parameter::new(b"2"),
                Parameter::new(b"3"),
            ],
        );
        let result = run_test(
            "SELECT * FROM sharded WHERE id IN ($1, $2, $3)",
            Some(&bind),
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_bound_select_with_any_array() {
        // ANY($1) with an array parameter - $1 is a single array value like '{1,2,3}'
        // Array parameters route to all shards since we can't reliably parse them
        let bind = Bind::new_params("", &[Parameter::new(b"{1,2,3}")]);
        let result = run_test("SELECT * FROM sharded WHERE id = ANY($1)", Some(&bind)).unwrap();
        assert_eq!(result, Some(Shard::All));
    }

    #[test]
    fn test_bound_select_with_not_equals_returns_none() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_test("SELECT * FROM sharded WHERE id != $1", Some(&bind)).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_bound_select_with_greater_than_returns_none() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_test("SELECT * FROM sharded WHERE id > $1", Some(&bind)).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_bound_select_with_complex_and() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_test(
            "SELECT * FROM sharded WHERE id = $1 AND status = 'active' AND created_at > now()",
            Some(&bind),
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_bound_select_with_left_join() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_test(
            "SELECT * FROM sharded s LEFT JOIN other o ON s.id = o.sharded_id WHERE s.id = $1",
            Some(&bind),
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_bound_select_with_multiple_joins() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_test(
            "SELECT * FROM sharded s \
             JOIN other o ON s.id = o.sharded_id \
             JOIN third t ON o.id = t.other_id \
             WHERE s.id = $1",
            Some(&bind),
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_bound_select_with_exists_subquery() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_test(
            "SELECT * FROM sharded WHERE EXISTS (SELECT 1 FROM other WHERE sharded_id = $1)",
            Some(&bind),
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_bound_select_with_scalar_subquery() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_test(
            "SELECT * FROM sharded WHERE id = (SELECT sharded_id FROM other WHERE sharded_id = $1 LIMIT 1)",
            Some(&bind),
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_bound_select_with_recursive_cte() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_test(
            "WITH RECURSIVE cte AS ( \
                SELECT * FROM sharded WHERE id = $1 \
                UNION ALL \
                SELECT s.* FROM sharded s JOIN cte c ON s.parent_id = c.id \
             ) SELECT * FROM cte",
            Some(&bind),
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_bound_select_with_union() {
        let bind = Bind::new_params("", &[Parameter::new(b"1"), Parameter::new(b"2")]);
        let result = run_test(
            "SELECT * FROM sharded WHERE id = $1 UNION SELECT * FROM sharded WHERE id = $2",
            Some(&bind),
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_bound_select_with_nested_subselects() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_test(
            "SELECT * FROM sharded WHERE id IN ( \
                SELECT * FROM other WHERE x IN ( \
                    SELECT y FROM third WHERE sharded_id = $1 \
                ) \
            )",
            Some(&bind),
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_bound_select_with_cte_and_subquery() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_test(
            "WITH cte AS (SELECT * FROM sharded WHERE id = $1) \
             SELECT * FROM cte WHERE id IN (SELECT sharded_id FROM other)",
            Some(&bind),
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_bound_select_with_multiple_ctes_and_subquery() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_test(
            "WITH cte1 AS (SELECT * FROM sharded WHERE id = $1), \
             cte2 AS (SELECT * FROM other WHERE sharded_id IN (SELECT id FROM cte1)) \
             SELECT * FROM cte2",
            Some(&bind),
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_bound_select_with_cte_subquery_and_join() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_test(
            "WITH cte AS (SELECT * FROM sharded WHERE id = $1) \
             SELECT c.*, o.* FROM cte c \
             JOIN other o ON c.id = o.sharded_id \
             WHERE o.x IN (SELECT y FROM third)",
            Some(&bind),
        )
        .unwrap();
        assert!(result.is_some());
    }

    // Schema-qualified table tests

    #[test]
    fn test_select_with_schema_qualified_table() {
        let result = run_test(
            "SELECT * FROM myschema.schema_sharded WHERE tenant_id = 1",
            None,
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_select_with_schema_qualified_alias() {
        let result = run_test(
            "SELECT * FROM myschema.schema_sharded s WHERE s.tenant_id = 1",
            None,
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_bound_select_with_schema_qualified_alias() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_test(
            "SELECT * FROM myschema.schema_sharded s WHERE s.tenant_id = $1",
            Some(&bind),
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_select_with_schema_qualified_join() {
        let result = run_test(
            "SELECT * FROM myschema.schema_sharded s \
             JOIN other o ON s.id = o.sharded_id WHERE s.tenant_id = 1",
            None,
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_select_wrong_schema_returns_none() {
        let result = run_test(
            "SELECT * FROM otherschema.schema_sharded WHERE tenant_id = 1",
            None,
        )
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_select_wrong_schema_alias_returns_none() {
        let result = run_test(
            "SELECT * FROM otherschema.schema_sharded s WHERE s.tenant_id = 1",
            None,
        )
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_select_with_any_array_literal() {
        let result = run_test("SELECT * FROM sharded WHERE id = ANY('{1, 2, 3}')", None).unwrap();
        // ANY with array literal routes to all shards
        assert_eq!(result, Some(Shard::All));
    }

    // UPDATE statement tests

    #[test]
    fn test_simple_update() {
        let result = run_test("UPDATE sharded SET name = 'foo' WHERE id = 1", None);
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_update_with_bound_param() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_test("UPDATE sharded SET name = 'foo' WHERE id = $1", Some(&bind));
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_update_with_and() {
        let result = run_test(
            "UPDATE sharded SET name = 'foo' WHERE id = 1 AND status = 'active'",
            None,
        );
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_update_no_where_returns_none() {
        let result = run_test("UPDATE sharded SET name = 'foo'", None);
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_update_with_subquery() {
        let result = run_test(
            "UPDATE sharded SET name = 'foo' WHERE id IN (SELECT sharded_id FROM other WHERE sharded_id = 1)",
            None,
        );
        assert!(result.unwrap().is_some());
    }

    // DELETE statement tests

    #[test]
    fn test_simple_delete() {
        let result = run_test("DELETE FROM sharded WHERE id = 1", None);
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_delete_with_bound_param() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_test("DELETE FROM sharded WHERE id = $1", Some(&bind));
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_delete_with_and() {
        let result = run_test(
            "DELETE FROM sharded WHERE id = 1 AND status = 'active'",
            None,
        );
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_delete_no_where_returns_none() {
        let result = run_test("DELETE FROM sharded", None);
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_delete_with_subquery() {
        let result = run_test(
            "DELETE FROM sharded WHERE id IN (SELECT sharded_id FROM other WHERE sharded_id = 1)",
            None,
        );
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_delete_with_cte() {
        let result = run_test(
            "WITH to_delete AS (SELECT id FROM sharded WHERE id = 1) DELETE FROM sharded WHERE id IN (SELECT id FROM to_delete)",
            None,
        );
        assert!(result.unwrap().is_some());
    }

    // INSERT statement tests

    #[test]
    fn test_simple_insert_with_value() {
        let result = run_test("INSERT INTO sharded (id, name) VALUES (1, 'foo')", None);
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_insert_with_bound_param() {
        let bind = Bind::new_params("", &[Parameter::new(b"1"), Parameter::new(b"foo")]);
        let result = run_test(
            "INSERT INTO sharded (id, name) VALUES ($1, $2)",
            Some(&bind),
        );
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_insert_with_subquery_in_values() {
        let result = run_test(
            "INSERT INTO sharded (id, name) VALUES ((SELECT sharded_id FROM other WHERE sharded_id = 1), 'foo')",
            None,
        );
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_insert_with_subquery_in_values_param() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_test(
            "INSERT INTO sharded (id, name) VALUES ((SELECT sharded_id FROM other WHERE sharded_id = $1), 'foo')",
            Some(&bind),
        );
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_insert_select() {
        let result = run_test(
            "INSERT INTO sharded (id, name) SELECT sharded_id, name FROM other WHERE sharded_id = 1",
            None,
        );
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_insert_select_with_param() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_test(
            "INSERT INTO sharded (id, name) SELECT sharded_id, name FROM other WHERE sharded_id = $1",
            Some(&bind),
        );
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_insert_with_cte() {
        let result = run_test(
            "WITH src AS (SELECT id, name FROM sharded WHERE id = 1) INSERT INTO sharded (id, name) SELECT id, name FROM src",
            None,
        );
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_insert_no_sharding_key_uses_round_robin() {
        // When sharding key is missing but table is sharded, use round-robin
        let result = run_test("INSERT INTO sharded (name) VALUES ('foo')", None);
        std::assert_matches!(result.unwrap(), Some(Shard::Direct(_)));
    }

    #[test]
    fn test_insert_multi_row_broadcasts() {
        // Multi-row INSERTs should broadcast to all shards
        let result = run_test(
            "INSERT INTO sharded (id, name) VALUES (1, 'foo'), (2, 'bar')",
            None,
        );
        assert_eq!(result.unwrap(), Some(Shard::All));
    }

    #[test]
    fn test_insert_multi_row_with_params_broadcasts() {
        // Multi-row INSERTs with params should also broadcast
        let bind = Bind::new_params(
            "",
            &[
                Parameter::new(b"1"),
                Parameter::new(b"foo"),
                Parameter::new(b"2"),
                Parameter::new(b"bar"),
            ],
        );
        let result = run_test(
            "INSERT INTO sharded (id, name) VALUES ($1, $2), ($3, $4)",
            Some(&bind),
        );
        assert_eq!(result.unwrap(), Some(Shard::All));
    }

    #[test]
    fn test_insert_unsharded_table_returns_none() {
        // Unsharded table should return None (not round-robin)
        let result = run_test("INSERT INTO unsharded_table (name) VALUES ('foo')", None);
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_insert_select_with_constant() {
        // INSERT ... SELECT where the sharding key is a constant in the SELECT target list
        let result = run_test("INSERT INTO sharded (id, name) SELECT 1, 'test'", None);
        assert!(matches!(result.unwrap(), Some(Shard::Direct(_))));
    }

    #[test]
    fn test_insert_select_with_constant_param() {
        // INSERT ... SELECT where the sharding key is a parameter in the SELECT target list
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_test(
            "INSERT INTO sharded (id, name) SELECT $1, 'test'",
            Some(&bind),
        );
        assert!(matches!(result.unwrap(), Some(Shard::Direct(_))));
    }

    #[test]
    fn test_insert_null_sharding_key_param_broadcasts() {
        // NULL sharding key as param should broadcast to all shards
        let bind = Bind::new_params("", &[Parameter::new_null(), Parameter::new(b"test")]);
        let result = run_test(
            "INSERT INTO sharded (id, name) VALUES ($1, $2)",
            Some(&bind),
        );
        assert_eq!(result.unwrap(), Some(Shard::All));
    }

    #[test]
    fn test_insert_null_sharding_key_literal_broadcasts() {
        // NULL sharding key as literal should broadcast to all shards
        let result = run_test("INSERT INTO sharded (id, name) VALUES (NULL, 'test')", None);
        assert_eq!(result.unwrap(), Some(Shard::All));
    }

    // Schema-based sharding fallback tests
    use crate::backend::replication::ShardedSchemas;
    use pgdog_config::sharding::ShardedSchema;

    fn run_test_with_schemas(stmt: &str, bind: Option<&Bind>) -> Result<Option<Shard>, Error> {
        let schema = ShardingSchema {
            shards: 3,
            tables: ShardedTables::new(
                vec![ShardedTable {
                    column: "id".into(),
                    name: Some("sharded".into()),
                    ..Default::default()
                }],
                vec![],
                false,
                SystemCatalogsBehavior::default(),
            ),
            schemas: ShardedSchemas::new(vec![
                ShardedSchema {
                    database: "test".to_string(),
                    name: Some("sales".to_string()),
                    shard: 1,
                    all: false,
                },
                ShardedSchema {
                    database: "test".to_string(),
                    name: Some("inventory".to_string()),
                    shard: 2,
                    all: false,
                },
            ]),
            ..Default::default()
        };
        let raw = pg_raw_parse::parse(stmt).unwrap();
        let stmt = raw.stmts().next().unwrap();
        let mut parser = StatementParser::new(stmt, bind.map(Into::into), &schema, None);
        parser.shard()
    }

    #[test]
    fn test_schema_sharding_select_fallback() {
        // No sharding key in WHERE clause, but table is in a sharded schema
        let result = run_test_with_schemas("SELECT * FROM sales.products", None).unwrap();
        assert_eq!(result, Some(Shard::Direct(1)));
    }

    #[test]
    fn test_schema_sharding_select_with_join() {
        // JOIN between tables in the same sharded schema
        let result = run_test_with_schemas(
            "SELECT * FROM sales.products p JOIN sales.orders o ON p.id = o.product_id",
            None,
        )
        .unwrap();
        assert_eq!(result, Some(Shard::Direct(1)));
    }

    #[test]
    fn test_schema_sharding_update_fallback() {
        // No sharding key in WHERE clause, but table is in a sharded schema
        let result = run_test_with_schemas("UPDATE sales.products SET name = 'foo'", None).unwrap();
        assert_eq!(result, Some(Shard::Direct(1)));
    }

    #[test]
    fn test_schema_sharding_delete_fallback() {
        // No sharding key in WHERE clause, but table is in a sharded schema
        let result = run_test_with_schemas("DELETE FROM sales.products", None).unwrap();
        assert_eq!(result, Some(Shard::Direct(1)));
    }

    #[test]
    fn test_schema_sharding_insert_fallback() {
        // No sharding key in values, but table is in a sharded schema
        let result =
            run_test_with_schemas("INSERT INTO sales.products (name) VALUES ('foo')", None)
                .unwrap();
        assert_eq!(result, Some(Shard::Direct(1)));
    }

    #[test]
    fn test_schema_sharding_with_subquery() {
        // Subquery references table in a sharded schema
        let result = run_test_with_schemas(
            "SELECT * FROM unsharded WHERE id IN (SELECT id FROM sales.products)",
            None,
        )
        .unwrap();
        assert_eq!(result, Some(Shard::Direct(1)));
    }

    #[test]
    fn test_schema_sharding_with_cte() {
        // CTE references table in a sharded schema
        let result = run_test_with_schemas(
            "WITH cte AS (SELECT * FROM sales.products) SELECT * FROM cte",
            None,
        )
        .unwrap();
        assert_eq!(result, Some(Shard::Direct(1)));
    }

    #[test]
    fn test_key_sharding_takes_precedence() {
        // Both key-based and schema-based sharding could match,
        // but key-based should take precedence
        let result = run_test_with_schemas("SELECT * FROM sharded WHERE id = 1", None).unwrap();
        // Key-based sharding returns a shard (not necessarily shard 1)
        assert!(result.is_some());
    }

    #[test]
    fn test_no_schema_no_key_returns_none() {
        // Table not in sharded schema and no sharding key
        let result = run_test_with_schemas("SELECT * FROM public.unknown", None).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_schema_sharding_different_schemas() {
        // Different sharded schemas route to different shards
        let result1 = run_test_with_schemas("SELECT * FROM sales.products", None).unwrap();
        let result2 = run_test_with_schemas("SELECT * FROM inventory.items", None).unwrap();
        assert_eq!(result1, Some(Shard::Direct(1)));
        assert_eq!(result2, Some(Shard::Direct(2)));
    }

    // Column-only sharded table detection tests (using loaded schema)

    fn run_test_column_only(stmt: &str, bind: Option<&Bind>) -> Result<Option<Shard>, Error> {
        // Use column-only sharded table config (no table name)
        let schema = ShardingSchema {
            shards: 3,
            tables: ShardedTables::new(
                vec![ShardedTable {
                    column: "tenant_id".into(),
                    // No table name - column-only config
                    ..Default::default()
                }],
                vec![],
                false,
                SystemCatalogsBehavior::default(),
            ),
            ..Default::default()
        };
        let raw = pg_raw_parse::parse(stmt).unwrap();
        let stmt = raw.stmts().next().unwrap();
        let mut parser = StatementParser::new(stmt, bind.map(Into::into), &schema, None);
        parser.shard()
    }

    #[test]
    fn test_column_only_select() {
        let result = run_test_column_only("SELECT * FROM users WHERE tenant_id = 1", None).unwrap();
        assert!(result.is_some(), "Should detect column-only sharding key");
    }

    #[test]
    fn test_column_only_select_with_alias() {
        let result =
            run_test_column_only("SELECT * FROM users u WHERE u.tenant_id = 1", None).unwrap();
        assert!(
            result.is_some(),
            "Should detect column-only sharding key with alias"
        );
    }

    #[test]
    fn test_column_only_select_bound_param() {
        let bind = Bind::new_params("", &[Parameter::new(b"42")]);
        let result =
            run_test_column_only("SELECT * FROM users WHERE tenant_id = $1", Some(&bind)).unwrap();
        assert!(
            result.is_some(),
            "Should detect column-only sharding key with bound param"
        );
    }

    #[test]
    fn test_column_only_update() {
        let result =
            run_test_column_only("UPDATE users SET name = 'foo' WHERE tenant_id = 1", None)
                .unwrap();
        assert!(
            result.is_some(),
            "Should detect column-only sharding key in UPDATE"
        );
    }

    #[test]
    fn test_column_only_delete() {
        let result = run_test_column_only("DELETE FROM users WHERE tenant_id = 1", None).unwrap();
        assert!(
            result.is_some(),
            "Should detect column-only sharding key in DELETE"
        );
    }

    #[test]
    fn test_column_only_insert() {
        let result = run_test_column_only(
            "INSERT INTO users (tenant_id, name) VALUES (1, 'foo')",
            None,
        )
        .unwrap();
        assert!(
            result.is_some(),
            "Should detect column-only sharding key in INSERT"
        );
    }

    #[test]
    fn test_column_only_any_table() {
        // Column-only configs work with any table
        let result =
            run_test_column_only("SELECT * FROM unknown_table WHERE tenant_id = 1", None).unwrap();
        assert!(
            result.is_some(),
            "Column-only config should work with any table"
        );
    }

    #[test]
    fn test_column_only_wrong_column() {
        // Column-only config shouldn't match different column name
        let result = run_test_column_only("SELECT * FROM users WHERE other_id = 1", None).unwrap();
        assert!(
            result.is_none(),
            "Column-only config should not match different column, got {:?}",
            result
        );
    }

    // INSERT without column list tests
    use crate::backend::schema::Relation;
    use crate::backend::schema::columns::StatsColumn as SchemaColumn;
    use indexmap::IndexMap;

    fn make_test_schema_with_relation() -> crate::backend::Schema {
        let mut columns = IndexMap::new();
        columns.insert(
            "id".to_string(),
            SchemaColumn {
                table_catalog: "test".into(),
                table_schema: "public".into(),
                table_name: "sharded".into(),
                column_name: "id".into(),
                column_default: String::new(),
                is_nullable: false,
                data_type: "bigint".into(),
                ordinal_position: 1,
                is_primary_key: true,
                foreign_keys: Vec::new(),
            }
            .into(),
        );
        columns.insert(
            "name".to_string(),
            SchemaColumn {
                table_catalog: "test".into(),
                table_schema: "public".into(),
                table_name: "sharded".into(),
                column_name: "name".into(),
                column_default: String::new(),
                is_nullable: true,
                data_type: "text".into(),
                ordinal_position: 2,
                is_primary_key: false,
                foreign_keys: Vec::new(),
            }
            .into(),
        );
        let relation = Relation::test_table("public", "sharded", columns);
        let relations = HashMap::from([(("public".into(), "sharded".into()), relation)]);
        crate::backend::Schema::from_parts(vec!["public".into()], relations)
    }

    fn run_test_with_schema_lookup(
        stmt: &str,
        bind: Option<&Bind>,
    ) -> Result<Option<Shard>, Error> {
        let sharding_schema = ShardingSchema {
            shards: 3,
            tables: ShardedTables::new(
                vec![ShardedTable {
                    column: "id".into(),
                    name: Some("sharded".into()),
                    ..Default::default()
                }],
                vec![],
                false,
                SystemCatalogsBehavior::default(),
            ),
            ..Default::default()
        };
        let db_schema = make_test_schema_with_relation();
        let schema_lookup = SchemaLookupContext {
            db_schema: &db_schema,
            user: "test",
            search_path: None,
        };
        let raw = pg_raw_parse::parse(stmt).unwrap();
        let stmt = raw.stmts().next().unwrap();
        let mut parser = StatementParser::new(stmt, bind.map(Into::into), &sharding_schema, None)
            .with_schema_lookup(schema_lookup);
        parser.shard()
    }

    #[test]
    fn test_insert_without_column_list() {
        // INSERT INTO sharded VALUES (1, 'test') should find sharding key from schema
        let result = run_test_with_schema_lookup("INSERT INTO sharded VALUES (1, 'test')", None);
        assert!(
            result.as_ref().unwrap().is_some(),
            "Should detect sharding key in INSERT without column list, got: {:?}",
            result
        );
    }

    #[test]
    fn test_insert_without_column_list_bound_param() {
        // INSERT INTO sharded VALUES ($1, $2) with bound params
        let bind = Bind::new_params("", &[Parameter::new(b"1"), Parameter::new(b"test")]);
        let result =
            run_test_with_schema_lookup("INSERT INTO sharded VALUES ($1, $2)", Some(&bind));
        assert!(
            result.as_ref().unwrap().is_some(),
            "Should detect sharding key in INSERT without column list (bound param), got: {:?}",
            result
        );
    }

    #[test]
    fn test_insert_without_column_list_null_key() {
        // INSERT INTO sharded VALUES (NULL, 'test') should broadcast
        let result = run_test_with_schema_lookup("INSERT INTO sharded VALUES (NULL, 'test')", None);
        assert_eq!(
            result.unwrap(),
            Some(Shard::All),
            "NULL sharding key should broadcast"
        );
    }

    // Omnisharded override tests
    use pgdog_config::OmnishardedTable;

    fn make_omnisharded_sharding_schema() -> ShardingSchema {
        // Column-only sharded table config (no table name specified)
        // This would normally match any table with a "tenant_id" column
        ShardingSchema {
            shards: 3,
            tables: ShardedTables::new(
                vec![ShardedTable {
                    column: "tenant_id".into(),
                    // No table name - column-only config
                    ..Default::default()
                }],
                vec![
                    OmnishardedTable {
                        name: "users".into(),
                        sticky_routing: false,
                    },
                    OmnishardedTable {
                        name: "sessions".into(),
                        sticky_routing: false,
                    },
                ],
                false,
                SystemCatalogsBehavior::default(),
            ),
            ..Default::default()
        }
    }

    fn make_omnisharded_db_schema() -> Schema {
        // Create a db_schema with tables that have the tenant_id column
        // This ensures that column-only sharding config would match these tables
        let mut relations = HashMap::new();

        // Helper to create a table with id and tenant_id columns
        let make_table = |table_name: &str| {
            let mut columns = IndexMap::new();
            columns.insert(
                "id".to_string(),
                SchemaColumn {
                    table_name: table_name.into(),
                    column_name: "id".into(),
                    ordinal_position: 1,
                    is_primary_key: true,
                    ..Default::default()
                }
                .into(),
            );
            columns.insert(
                "tenant_id".to_string(),
                SchemaColumn {
                    table_name: table_name.into(),
                    column_name: "tenant_id".into(),
                    ordinal_position: 2,
                    ..Default::default()
                }
                .into(),
            );
            Relation::test_table("public", table_name, columns)
        };

        // "users" table (omnisharded)
        relations.insert(("public".into(), "users".into()), make_table("users"));

        // "sessions" table (omnisharded)
        relations.insert(("public".into(), "sessions".into()), make_table("sessions"));

        // "orders" table (NOT omnisharded)
        relations.insert(("public".into(), "orders".into()), make_table("orders"));

        Schema::from_parts(vec!["public".into()], relations)
    }

    fn run_is_sharded_test(stmt: &str) -> bool {
        let schema = make_omnisharded_sharding_schema();
        let db_schema = make_omnisharded_db_schema();
        let raw = pg_raw_parse::parse(stmt).unwrap();
        let stmt = raw.stmts().next().unwrap();
        let mut parser = StatementParser::new(stmt, None, &schema, None);
        parser.is_sharded(&db_schema, "test", None)
    }

    #[test]
    fn test_omnisharded_overrides_column_only_sharding() {
        // Table "users" is in omnisharded config and has tenant_id column
        // Even though column-only config would match, omnisharded should override
        let result = run_is_sharded_test("SELECT * FROM users WHERE tenant_id = 1");
        assert!(
            !result,
            "Omnisharded table should override column-only sharding config"
        );
    }

    #[test]
    fn test_omnisharded_overrides_for_multiple_omnisharded_tables() {
        // Both tables are in omnisharded config
        let result =
            run_is_sharded_test("SELECT * FROM users u JOIN sessions s ON u.id = s.user_id");
        assert!(
            !result,
            "Query with all omnisharded tables should return false"
        );
    }

    #[test]
    fn test_mixed_omnisharded_and_regular_table_is_sharded() {
        // "users" is omnisharded, but "orders" is not
        // Since not all tables are omnisharded, should check sharding config
        let result = run_is_sharded_test("SELECT * FROM users u JOIN orders o ON u.id = o.user_id");
        assert!(
            result,
            "Query with mixed omnisharded and regular tables should be sharded"
        );
    }

    #[test]
    fn test_non_omnisharded_table_is_sharded() {
        // Table "orders" is not in omnisharded config and has tenant_id column
        // Should match the column-only sharding config
        let result = run_is_sharded_test("SELECT * FROM orders WHERE tenant_id = 1");
        assert!(
            result,
            "Non-omnisharded table with sharding column should be sharded"
        );
    }

    #[test]
    fn test_omnisharded_insert_not_sharded() {
        let result = run_is_sharded_test("INSERT INTO users (tenant_id, name) VALUES (1, 'test')");
        assert!(
            !result,
            "INSERT into omnisharded table should not be sharded"
        );
    }

    #[test]
    fn test_omnisharded_update_not_sharded() {
        let result = run_is_sharded_test("UPDATE users SET name = 'test' WHERE tenant_id = 1");
        assert!(!result, "UPDATE on omnisharded table should not be sharded");
    }

    #[test]
    fn test_omnisharded_delete_not_sharded() {
        let result = run_is_sharded_test("DELETE FROM users WHERE tenant_id = 1");
        assert!(
            !result,
            "DELETE from omnisharded table should not be sharded"
        );
    }

    mod advisory_locks {
        use super::*;

        fn locks(query: &str) -> Vec<AdvisoryLock> {
            locks_with_bind(query, None)
        }

        fn locks_with_bind(query: &str, bind: Option<&Bind>) -> Vec<AdvisoryLock> {
            let schema = ShardingSchema::default();
            let raw = pg_raw_parse::parse(query).unwrap();
            let stmt = raw.stmts().next().unwrap();
            let mut parser = StatementParser::new(stmt, bind.map(Into::into), &schema, None);
            let mut v: Vec<_> = parser.extract_advisory_locks().iter().copied().collect();
            v.sort_by_key(|l| (l.id, l.unlock));
            v
        }

        fn session(id: Option<i64>, unlock: bool) -> AdvisoryLock {
            AdvisoryLock {
                id,
                unlock,
                scope: LockScope::Session,
            }
        }

        fn xact(id: Option<i64>, unlock: bool) -> AdvisoryLock {
            AdvisoryLock {
                id,
                unlock,
                scope: LockScope::Transaction,
            }
        }

        #[test]
        fn lock_and_unlock() {
            assert_eq!(
                locks("SELECT pg_advisory_lock(42)"),
                vec![session(Some(42), false)],
            );
            assert_eq!(
                locks("SELECT pg_advisory_unlock(42)"),
                vec![session(Some(42), true)],
            );
        }

        #[test]
        fn bigint_argument() {
            // Values larger than i32 are encoded as Float in PG internally.
            assert_eq!(
                locks("SELECT pg_advisory_lock(9000000000)"),
                vec![session(Some(9_000_000_000), false)],
            );
        }

        #[test]
        fn all_session_lock_variants() {
            for q in [
                "SELECT pg_try_advisory_lock(7)",
                "SELECT pg_advisory_lock_shared(7)",
                "SELECT pg_try_advisory_lock_shared(7)",
            ] {
                assert_eq!(locks(q), vec![session(Some(7), false)], "{q}");
            }
        }

        #[test]
        fn xact_variants_have_transaction_scope() {
            // xact locks must still pin the backend for the lifetime of the transaction,
            // but the engine drops them at COMMIT/ROLLBACK.
            for q in [
                "SELECT pg_advisory_xact_lock(7)",
                "SELECT pg_advisory_xact_lock_shared(7)",
                "SELECT pg_try_advisory_xact_lock(7)",
                "SELECT pg_try_advisory_xact_lock_shared(7)",
            ] {
                assert_eq!(locks(q), vec![xact(Some(7), false)], "{q}");
            }
        }

        #[test]
        fn multiple_and_dedup() {
            assert_eq!(
                locks("SELECT pg_advisory_lock(5), pg_advisory_lock(5), pg_advisory_lock(6)"),
                vec![session(Some(5), false), session(Some(6), false)],
            );
        }

        #[test]
        fn cast_and_cte() {
            assert_eq!(
                locks("SELECT pg_try_advisory_lock(9)::bool"),
                vec![session(Some(9), false)],
            );
            assert_eq!(
                locks("WITH x AS (SELECT pg_advisory_lock(11)) SELECT * FROM x"),
                vec![session(Some(11), false)],
            );
        }

        #[test]
        fn param_without_bind_is_ignored() {
            // Without a Bind message, a parameter placeholder means the prepared
            // statement is only being parsed — no lock is actually taken.
            assert!(locks("SELECT pg_advisory_lock($1)").is_empty());
        }

        #[test]
        fn unlock_all_without_bind() {
            // unlock_all takes no arguments, so it always applies.
            assert_eq!(
                locks("SELECT pg_advisory_unlock_all()"),
                vec![session(None, true)],
            );
        }

        #[test]
        fn ignored_cases() {
            // Schema-qualified — not the builtin.
            assert!(locks("SELECT other.pg_advisory_lock(1)").is_empty());
            // Unrelated functions.
            assert!(locks("SELECT 1, now()").is_empty());
        }

        #[test]
        fn key_from_values_subquery_no_bind() {
            // Without a Bind, parameter-based VALUES rows are skipped — the
            // prepared statement is only being parsed, no lock is taken.
            assert!(
                locks("SELECT pg_advisory_lock(value) FROM (VALUES ($1)) AS t(value)").is_empty()
            );
            assert!(
                locks("SELECT pg_advisory_unlock(value) FROM (VALUES ($1)) AS t(value)").is_empty()
            );
            assert!(
                locks("SELECT pg_try_advisory_lock(value) FROM (VALUES ($1)) AS t(value)")
                    .is_empty()
            );
        }

        #[test]
        fn xact_lock_with_param_no_bind() {
            // Without a Bind the prepared statement is just being parsed.
            assert!(locks("SELECT pg_advisory_xact_lock($1)").is_empty());
        }

        #[test]
        fn param_resolved_from_bind() {
            let bind = Bind::new_params("", &[Parameter::new(b"4242")]);
            assert_eq!(
                locks_with_bind("SELECT pg_advisory_lock($1)", Some(&bind)),
                vec![session(Some(4242), false)],
            );
            assert_eq!(
                locks_with_bind("SELECT pg_advisory_xact_lock($1)", Some(&bind)),
                vec![xact(Some(4242), false)],
            );
            assert_eq!(
                locks_with_bind("SELECT pg_advisory_unlock($1)", Some(&bind)),
                vec![session(Some(4242), true)],
            );
        }

        #[test]
        fn bind_bigint_value() {
            // Keys wider than i32 are encoded as text on the wire but still
            // decode cleanly through FromDataType<i64>.
            let bind = Bind::new_params("", &[Parameter::new(b"9000000000")]);
            assert_eq!(
                locks_with_bind("SELECT pg_advisory_lock($1)", Some(&bind)),
                vec![session(Some(9_000_000_000), false)],
            );
        }

        #[test]
        fn multiple_locks_in_one_query_with_bind() {
            // Single query taking multiple advisory locks from distinct bind params.
            let bind = Bind::new_params(
                "",
                &[
                    Parameter::new(b"11"),
                    Parameter::new(b"22"),
                    Parameter::new(b"33"),
                ],
            );
            assert_eq!(
                locks_with_bind(
                    "SELECT pg_advisory_lock($1), pg_advisory_xact_lock($2), pg_advisory_unlock($3)",
                    Some(&bind),
                ),
                vec![
                    session(Some(11), false),
                    xact(Some(22), false),
                    session(Some(33), true),
                ],
            );
        }

        #[test]
        fn multiple_literal_locks_in_one_query() {
            assert_eq!(
                locks(
                    "SELECT pg_advisory_lock(10), pg_advisory_xact_lock(20), \
                     pg_advisory_unlock(30), pg_advisory_unlock_all()",
                ),
                vec![
                    session(None, true),
                    session(Some(10), false),
                    xact(Some(20), false),
                    session(Some(30), true),
                ],
            );
        }

        #[test]
        fn values_multiple_rows_expand_to_multiple_locks() {
            // `pg_advisory_lock(value) FROM (VALUES (1),(2),(3)) AS t(value)` is
            // called once per row, so the parser should emit one lock per row.
            assert_eq!(
                locks("SELECT pg_advisory_lock(value) FROM (VALUES (10), (20), (30)) AS t(value)",),
                vec![
                    session(Some(10), false),
                    session(Some(20), false),
                    session(Some(30), false),
                ],
            );
        }

        #[test]
        fn advisory_lock_from_values_without_explicit_column_name() {
            assert_eq!(
                locks("SELECT pg_advisory_lock(column1) FROM (VALUES (10), (20), (30))",),
                vec![
                    session(Some(10), false),
                    session(Some(20), false),
                    session(Some(30), false),
                ],
            );
        }

        #[test]
        fn advisory_lock_when_client_is_sadistic() {
            assert_eq!(
                locks(
                    "SELECT pg_advisory_lock(column1), (SELECT pg_advisory_lock(c) FROM (VALUES (20), (30)) AS t(c)) FROM (VALUES (10))",
                ),
                vec![
                    session(Some(10), false),
                    session(Some(20), false),
                    session(Some(30), false),
                ],
            );
        }

        #[test]
        fn values_multiple_rows_with_bind() {
            let bind = Bind::new_params(
                "",
                &[
                    Parameter::new(b"41"),
                    Parameter::new(b"42"),
                    Parameter::new(b"43"),
                ],
            );
            assert_eq!(
                locks_with_bind(
                    "SELECT pg_advisory_lock(value) FROM (VALUES ($1), ($2), ($3)) AS t(value)",
                    Some(&bind),
                ),
                vec![
                    session(Some(41), false),
                    session(Some(42), false),
                    session(Some(43), false),
                ],
            );
        }

        #[test]
        fn values_multi_rows_unlock_and_xact() {
            // Same multi-row expansion for unlock and xact variants.
            assert_eq!(
                locks("SELECT pg_advisory_unlock(value) FROM (VALUES (1), (2)) AS t(value)",),
                vec![session(Some(1), true), session(Some(2), true)],
            );
            assert_eq!(
                locks("SELECT pg_advisory_xact_lock(value) FROM (VALUES (5), (6)) AS t(value)",),
                vec![xact(Some(5), false), xact(Some(6), false)],
            );
        }

        #[test]
        fn param_out_of_range_fallback() {
            // $2 has no bound value — we should still record the lock but leave id=None.
            let bind = Bind::new_params("", &[Parameter::new(b"99")]);
            assert_eq!(
                locks_with_bind(
                    "SELECT pg_advisory_lock($1), pg_advisory_lock($2)",
                    Some(&bind),
                ),
                vec![session(None, false), session(Some(99), false)],
            );
        }
    }
}
