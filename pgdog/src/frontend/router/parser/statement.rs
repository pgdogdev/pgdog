use std::collections::{HashMap, HashSet};

use pg_query::{
    protobuf::{
        AExprKind, BoolExprType, DeleteStmt, InsertStmt, RangeVar, RawStmt, SelectStmt, UpdateStmt,
    },
    Node, NodeEnum,
};

use super::{
    super::sharding::Value as ShardingValue, explain_trace::ExplainRecorder, Column, Error, Table,
    Value,
};
use crate::{
    backend::{Schema, ShardingSchema},
    frontend::router::{
        parser::Shard,
        sharding::{ContextBuilder, SchemaSharder},
    },
    net::{parameter::ParameterValue, Bind},
};

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
    fn from_from_clause(nodes: &'a [Node]) -> Self {
        let mut ctx = Self::default();
        ctx.extract_aliases(nodes);

        // Try to get the primary table for simple queries
        if nodes.len() == 1 {
            if let Some(table) = nodes.first().and_then(|n| Table::try_from(n).ok()) {
                ctx.table = Some(table);
            }
        }

        ctx
    }

    fn extract_aliases(&mut self, nodes: &'a [Node]) {
        for node in nodes {
            self.extract_alias_from_node(node);
        }
    }

    fn extract_alias_from_node(&mut self, node: &'a Node) {
        match &node.node {
            Some(NodeEnum::RangeVar(range_var)) => {
                if let Some(ref alias) = range_var.alias {
                    let table = Table::from(range_var);
                    self.aliases.insert(alias.aliasname.as_str(), table);
                }
            }
            Some(NodeEnum::JoinExpr(join)) => {
                if let Some(ref larg) = join.larg {
                    self.extract_alias_from_node(larg);
                }
                if let Some(ref rarg) = join.rarg {
                    self.extract_alias_from_node(rarg);
                }
            }
            Some(NodeEnum::RangeSubselect(subselect)) => {
                if let Some(ref alias) = subselect.alias {
                    // For subselects, we don't have a real table name
                    // but we record the alias anyway for future use
                    self.aliases.insert(
                        alias.aliasname.as_str(),
                        Table {
                            name: alias.aliasname.as_str(),
                            schema: None,
                            alias: None,
                        },
                    );
                }
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
    Match(Shard),
    Matches(Vec<Shard>),
    None,
}

struct ValueIterator<'a, 'b> {
    source: &'b SearchResult<'a>,
    pos: usize,
}

impl<'a, 'b> Iterator for ValueIterator<'a, 'b> {
    type Item = &'b Value<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = match self.source {
            SearchResult::Value(ref val) => {
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
    fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }

    fn is_match(&self) -> bool {
        matches!(self, Self::Match(_) | Self::Matches(_))
    }

    fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::Match(first), Self::Match(second)) => Self::Matches(vec![first, second]),
            (Self::Match(shard), Self::Matches(mut shards))
            | (Self::Matches(mut shards), Self::Match(shard)) => Self::Matches({
                shards.push(shard);
                shards
            }),
            (Self::None, other) | (other, Self::None) => other,
            _ => Self::None,
        }
    }

    fn iter<'b>(&'b self) -> ValueIterator<'a, 'b> {
        ValueIterator {
            source: self,
            pos: 0,
        }
    }
}

enum Statement<'a> {
    Select(&'a SelectStmt),
    Update(&'a UpdateStmt),
    Delete(&'a DeleteStmt),
    Insert(&'a InsertStmt),
}

pub struct StatementParser<'a, 'b, 'c> {
    stmt: Statement<'a>,
    bind: Option<&'b Bind>,
    schema: &'b ShardingSchema,
    recorder: Option<&'c mut ExplainRecorder>,
}

impl<'a, 'b, 'c> StatementParser<'a, 'b, 'c> {
    fn new(
        stmt: Statement<'a>,
        bind: Option<&'b Bind>,
        schema: &'b ShardingSchema,
        recorder: Option<&'c mut ExplainRecorder>,
    ) -> Self {
        Self {
            stmt,
            bind,
            schema,
            recorder,
        }
    }

    pub fn from_select(
        stmt: &'a SelectStmt,
        bind: Option<&'b Bind>,
        schema: &'b ShardingSchema,
        recorder: Option<&'c mut ExplainRecorder>,
    ) -> Self {
        Self::new(Statement::Select(stmt), bind, schema, recorder)
    }

    pub fn from_update(
        stmt: &'a UpdateStmt,
        bind: Option<&'b Bind>,
        schema: &'b ShardingSchema,
        recorder: Option<&'c mut ExplainRecorder>,
    ) -> Self {
        Self::new(Statement::Update(stmt), bind, schema, recorder)
    }

    pub fn from_delete(
        stmt: &'a DeleteStmt,
        bind: Option<&'b Bind>,
        schema: &'b ShardingSchema,
        recorder: Option<&'c mut ExplainRecorder>,
    ) -> Self {
        Self::new(Statement::Delete(stmt), bind, schema, recorder)
    }

    pub fn from_insert(
        stmt: &'a InsertStmt,
        bind: Option<&'b Bind>,
        schema: &'b ShardingSchema,
        recorder: Option<&'c mut ExplainRecorder>,
    ) -> Self {
        Self::new(Statement::Insert(stmt), bind, schema, recorder)
    }

    /// Record a sharding key match.
    fn record_sharding_key(&mut self, shard: &Shard, column: Column<'_>, value: &Value<'_>) {
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

    pub fn from_raw(
        raw: &'a RawStmt,
        bind: Option<&'b Bind>,
        schema: &'b ShardingSchema,
        recorder: Option<&'c mut ExplainRecorder>,
    ) -> Result<Self, Error> {
        match raw.stmt.as_ref().and_then(|n| n.node.as_ref()) {
            Some(NodeEnum::SelectStmt(stmt)) => Ok(Self::from_select(stmt, bind, schema, recorder)),
            Some(NodeEnum::UpdateStmt(stmt)) => Ok(Self::from_update(stmt, bind, schema, recorder)),
            Some(NodeEnum::DeleteStmt(stmt)) => Ok(Self::from_delete(stmt, bind, schema, recorder)),
            Some(NodeEnum::InsertStmt(stmt)) => Ok(Self::from_insert(stmt, bind, schema, recorder)),
            _ => Err(Error::NotASelect),
        }
    }

    pub fn shard(&mut self) -> Result<Option<Shard>, Error> {
        let result = match self.stmt {
            Statement::Select(stmt) => self.shard_select(stmt),
            Statement::Update(stmt) => self.shard_update(stmt),
            Statement::Delete(stmt) => self.shard_delete(stmt),
            Statement::Insert(stmt) => self.shard_insert(stmt),
        }?;

        // Key-based sharding succeeded
        if result.is_some() {
            return Ok(result);
        } else if self.schema.schemas.is_empty() {
            return Ok(None);
        }

        // Fallback to schema-based sharding
        let tables = self.extract_tables();
        let mut schema_sharder = SchemaSharder::default();
        for table in &tables {
            schema_sharder.resolve(table.schema(), &self.schema.schemas);
        }

        if let Some((shard, schema_name)) = schema_sharder.get() {
            if let Some(recorder) = self.recorder.as_mut() {
                recorder.record_entry(
                    Some(shard.clone()),
                    format!("matched schema {}", schema_name),
                );
            }
            return Ok(Some(shard));
        }

        Ok(None)
    }

    /// Check that the query references a table that contains a sharded
    /// column. This check is needed in case sharded tables config
    /// doesn't specify a table name and should short-circuit if it does.
    pub fn is_sharded(
        &self,
        db_schema: &Schema,
        user: &str,
        search_path: Option<&ParameterValue>,
    ) -> bool {
        let sharded_tables = self.schema.tables.tables();

        // Separate configs with explicit table names from those without
        let (named, nameless): (Vec<_>, Vec<_>) =
            sharded_tables.iter().partition(|t| t.name.is_some());

        let tables = self.extract_tables();

        for table in &tables {
            // Check named sharded table configs (fast path, no schema lookup needed)
            for config in &named {
                if let Some(ref name) = config.name {
                    if table.name == name {
                        // Also check schema match if specified in config
                        if let Some(ref config_schema) = config.schema {
                            if table.schema != Some(config_schema.as_str()) {
                                continue;
                            }
                        }
                        return true;
                    }
                }
            }

            // Check nameless configs by looking up the table in the db schema
            // to see if it has the sharding column
            if !nameless.is_empty() {
                if let Some(relation) = db_schema.table(*table, user, search_path) {
                    for config in &nameless {
                        if relation.has_column(&config.column) {
                            return true;
                        }
                    }
                }
            }
        }

        false
    }

    /// Extract all tables referenced in the statement.
    pub fn extract_tables(&self) -> Vec<Table<'a>> {
        let mut tables = Vec::new();
        match self.stmt {
            Statement::Select(stmt) => self.extract_tables_from_select(stmt, &mut tables),
            Statement::Update(stmt) => self.extract_tables_from_update(stmt, &mut tables),
            Statement::Delete(stmt) => self.extract_tables_from_delete(stmt, &mut tables),
            Statement::Insert(stmt) => self.extract_tables_from_insert(stmt, &mut tables),
        }
        tables
    }

    fn extract_tables_from_select(&self, stmt: &'a SelectStmt, tables: &mut Vec<Table<'a>>) {
        // Handle UNION/INTERSECT/EXCEPT
        if let Some(ref larg) = stmt.larg {
            self.extract_tables_from_select(larg, tables);
        }
        if let Some(ref rarg) = stmt.rarg {
            self.extract_tables_from_select(rarg, tables);
        }

        // FROM clause
        for node in &stmt.from_clause {
            self.extract_tables_from_node(node, tables);
        }

        // WITH clause (CTEs)
        if let Some(ref with_clause) = stmt.with_clause {
            for cte in &with_clause.ctes {
                if let Some(NodeEnum::CommonTableExpr(ref cte_expr)) = cte.node {
                    if let Some(ref ctequery) = cte_expr.ctequery {
                        if let Some(NodeEnum::SelectStmt(ref inner_select)) = ctequery.node {
                            self.extract_tables_from_select(inner_select, tables);
                        }
                    }
                }
            }
        }

        // WHERE clause subqueries
        if let Some(ref where_clause) = stmt.where_clause {
            self.extract_tables_from_node(where_clause, tables);
        }
    }

    fn extract_tables_from_update(&self, stmt: &'a UpdateStmt, tables: &mut Vec<Table<'a>>) {
        // Main relation
        if let Some(ref relation) = stmt.relation {
            tables.push(Table::from(relation));
        }

        // FROM clause
        for node in &stmt.from_clause {
            self.extract_tables_from_node(node, tables);
        }

        // WITH clause (CTEs)
        if let Some(ref with_clause) = stmt.with_clause {
            for cte in &with_clause.ctes {
                if let Some(NodeEnum::CommonTableExpr(ref cte_expr)) = cte.node {
                    if let Some(ref ctequery) = cte_expr.ctequery {
                        if let Some(NodeEnum::SelectStmt(ref inner_select)) = ctequery.node {
                            self.extract_tables_from_select(inner_select, tables);
                        }
                    }
                }
            }
        }

        // WHERE clause subqueries
        if let Some(ref where_clause) = stmt.where_clause {
            self.extract_tables_from_node(where_clause, tables);
        }
    }

    fn extract_tables_from_delete(&self, stmt: &'a DeleteStmt, tables: &mut Vec<Table<'a>>) {
        // Main relation
        if let Some(ref relation) = stmt.relation {
            tables.push(Table::from(relation));
        }

        // USING clause
        for node in &stmt.using_clause {
            self.extract_tables_from_node(node, tables);
        }

        // WITH clause (CTEs)
        if let Some(ref with_clause) = stmt.with_clause {
            for cte in &with_clause.ctes {
                if let Some(NodeEnum::CommonTableExpr(ref cte_expr)) = cte.node {
                    if let Some(ref ctequery) = cte_expr.ctequery {
                        if let Some(NodeEnum::SelectStmt(ref inner_select)) = ctequery.node {
                            self.extract_tables_from_select(inner_select, tables);
                        }
                    }
                }
            }
        }

        // WHERE clause subqueries
        if let Some(ref where_clause) = stmt.where_clause {
            self.extract_tables_from_node(where_clause, tables);
        }
    }

    fn extract_tables_from_insert(&self, stmt: &'a InsertStmt, tables: &mut Vec<Table<'a>>) {
        // Main relation
        if let Some(ref relation) = stmt.relation {
            tables.push(Table::from(relation));
        }

        // WITH clause (CTEs)
        if let Some(ref with_clause) = stmt.with_clause {
            for cte in &with_clause.ctes {
                if let Some(NodeEnum::CommonTableExpr(ref cte_expr)) = cte.node {
                    if let Some(ref ctequery) = cte_expr.ctequery {
                        if let Some(NodeEnum::SelectStmt(ref inner_select)) = ctequery.node {
                            self.extract_tables_from_select(inner_select, tables);
                        }
                    }
                }
            }
        }

        // SELECT part of INSERT ... SELECT
        if let Some(ref select_stmt) = stmt.select_stmt {
            if let Some(NodeEnum::SelectStmt(ref inner_select)) = select_stmt.node {
                self.extract_tables_from_select(inner_select, tables);
            }
        }
    }

    fn extract_tables_from_node(&self, node: &'a Node, tables: &mut Vec<Table<'a>>) {
        match &node.node {
            Some(NodeEnum::RangeVar(range_var)) => {
                tables.push(Table::from(range_var));
            }
            Some(NodeEnum::JoinExpr(join)) => {
                if let Some(ref larg) = join.larg {
                    self.extract_tables_from_node(larg, tables);
                }
                if let Some(ref rarg) = join.rarg {
                    self.extract_tables_from_node(rarg, tables);
                }
            }
            Some(NodeEnum::RangeSubselect(subselect)) => {
                if let Some(ref subquery) = subselect.subquery {
                    if let Some(NodeEnum::SelectStmt(ref inner_select)) = subquery.node {
                        self.extract_tables_from_select(inner_select, tables);
                    }
                }
            }
            Some(NodeEnum::SubLink(sublink)) => {
                if let Some(ref subselect) = sublink.subselect {
                    if let Some(NodeEnum::SelectStmt(ref inner_select)) = subselect.node {
                        self.extract_tables_from_select(inner_select, tables);
                    }
                }
            }
            Some(NodeEnum::SelectStmt(inner_select)) => {
                self.extract_tables_from_select(inner_select, tables);
            }
            Some(NodeEnum::BoolExpr(bool_expr)) => {
                for arg in &bool_expr.args {
                    self.extract_tables_from_node(arg, tables);
                }
            }
            Some(NodeEnum::AExpr(a_expr)) => {
                if let Some(ref lexpr) = a_expr.lexpr {
                    self.extract_tables_from_node(lexpr, tables);
                }
                if let Some(ref rexpr) = a_expr.rexpr {
                    self.extract_tables_from_node(rexpr, tables);
                }
            }
            _ => {}
        }
    }

    fn shard_select(&mut self, stmt: &'a SelectStmt) -> Result<Option<Shard>, Error> {
        let ctx = SearchContext::from_from_clause(&stmt.from_clause);
        let result = self.search_select_stmt(stmt, &ctx)?;

        match result {
            SearchResult::Match(shard) => Ok(Some(shard)),
            SearchResult::Matches(shards) => Ok(Self::converge(&shards)),
            _ => Ok(None),
        }
    }

    fn shard_update(&mut self, stmt: &'a UpdateStmt) -> Result<Option<Shard>, Error> {
        let ctx = self.context_from_relation(&stmt.relation);
        let result = self.search_update_stmt(stmt, &ctx)?;

        match result {
            SearchResult::Match(shard) => Ok(Some(shard)),
            SearchResult::Matches(shards) => Ok(Self::converge(&shards)),
            _ => Ok(None),
        }
    }

    fn shard_delete(&mut self, stmt: &'a DeleteStmt) -> Result<Option<Shard>, Error> {
        let ctx = self.context_from_relation(&stmt.relation);
        let result = self.search_delete_stmt(stmt, &ctx)?;

        match result {
            SearchResult::Match(shard) => Ok(Some(shard)),
            SearchResult::Matches(shards) => Ok(Self::converge(&shards)),
            _ => Ok(None),
        }
    }

    fn shard_insert(&mut self, stmt: &'a InsertStmt) -> Result<Option<Shard>, Error> {
        let ctx = self.context_from_relation(&stmt.relation);
        let result = self.search_insert_stmt(stmt, &ctx)?;

        match result {
            SearchResult::Match(shard) => Ok(Some(shard)),
            SearchResult::Matches(shards) => Ok(Self::converge(&shards)),
            _ => Ok(None),
        }
    }

    fn context_from_relation(&self, relation: &'a Option<RangeVar>) -> SearchContext<'a> {
        let mut ctx = SearchContext::default();
        if let Some(ref range_var) = relation {
            let table = Table::from(range_var);
            ctx.table = Some(table);
            if let Some(ref alias) = range_var.alias {
                ctx.aliases.insert(alias.aliasname.as_str(), table);
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

    fn compute_shard(
        &mut self,
        column: Column<'a>,
        value: Value<'a>,
    ) -> Result<Option<Shard>, Error> {
        if let Some(table) = self.schema.tables().get_table(column) {
            let context = ContextBuilder::new(table);
            let shard = match value {
                Value::Placeholder(pos) => {
                    let param = self
                        .bind
                        .map(|bind| bind.parameter(pos as usize - 1))
                        .transpose()?
                        .flatten();
                    // Expect params to be accurate.
                    let param = if let Some(param) = param {
                        param
                    } else {
                        return Ok(None);
                    };
                    let value = ShardingValue::from_param(&param, table.data_type)?;
                    Some(
                        context
                            .value(value)
                            .shards(self.schema.shards)
                            .build()?
                            .apply()?,
                    )
                }

                Value::String(val) => Some(
                    context
                        .data(val)
                        .shards(self.schema.shards)
                        .build()?
                        .apply()?,
                ),

                Value::Integer(val) => Some(
                    context
                        .data(val)
                        .shards(self.schema.shards)
                        .build()?
                        .apply()?,
                ),
                Value::Null => None,
                _ => None,
            };

            Ok(shard)
        } else {
            Ok(None)
        }
    }

    fn select_search(
        &mut self,
        node: &'a Node,
        ctx: &SearchContext<'a>,
    ) -> Result<SearchResult<'a>, Error> {
        match node.node {
            // Value types - these are leaf nodes representing actual values
            Some(NodeEnum::AConst(_))
            | Some(NodeEnum::ParamRef(_))
            | Some(NodeEnum::FuncCall(_)) => {
                if let Ok(value) = Value::try_from(&node.node) {
                    return Ok(SearchResult::Value(value));
                }
                Ok(SearchResult::None)
            }

            Some(NodeEnum::TypeCast(ref cast)) => {
                if let Some(ref arg) = cast.arg {
                    return self.select_search(arg, ctx);
                }
                Ok(SearchResult::None)
            }

            Some(NodeEnum::SelectStmt(ref stmt)) => {
                // Build context with aliases from the FROM clause
                let ctx = SearchContext::from_from_clause(&stmt.from_clause);
                self.search_select_stmt(stmt, &ctx)
            }

            Some(NodeEnum::RangeSubselect(ref subselect)) => {
                if let Some(ref node) = subselect.subquery {
                    self.select_search(node, ctx)
                } else {
                    Ok(SearchResult::None)
                }
            }

            Some(NodeEnum::ColumnRef(_)) => {
                let mut column = Column::try_from(&node.node)?;

                // If column has no table, qualify with context table
                if column.table().is_none() {
                    if let Some(ref table) = ctx.table {
                        column.qualify(*table);
                    }
                }

                Ok(SearchResult::Column(column))
            }

            Some(NodeEnum::AExpr(ref expr)) => {
                let kind = expr.kind();
                let mut supported = false;

                if matches!(
                    kind,
                    AExprKind::AexprOp | AExprKind::AexprIn | AExprKind::AexprOpAny
                ) {
                    supported = expr
                        .name
                        .first()
                        .map(|node| match node.node {
                            Some(NodeEnum::String(ref string)) => string.sval.as_str(),
                            _ => "",
                        })
                        .unwrap_or_default()
                        == "=";
                }

                if !supported {
                    return Ok(SearchResult::None);
                }

                let is_any = matches!(kind, AExprKind::AexprOpAny);

                let mut results = vec![];

                if let Some(ref left) = expr.lexpr {
                    results.push(self.select_search(left, ctx)?);
                }

                if let Some(ref right) = expr.rexpr {
                    results.push(self.select_search(right, ctx)?);
                }

                if results.len() != 2 {
                    Ok(SearchResult::None)
                } else {
                    let right = results.pop().unwrap();
                    let left = results.pop().unwrap();

                    // If either side is already a match (from subquery), return it
                    if right.is_match() {
                        return Ok(right);
                    }
                    if left.is_match() {
                        return Ok(left);
                    }

                    match (right, left) {
                        (SearchResult::Column(column), values)
                        | (values, SearchResult::Column(column)) => {
                            // For ANY expressions with sharding columns, we can't reliably
                            // parse array literals or parameters, so route to all shards.
                            if is_any
                                && matches!(values, SearchResult::Value(_))
                                && self.schema.tables().get_table(column).is_some()
                            {
                                return Ok(SearchResult::Match(Shard::All));
                            }

                            let mut shards = HashSet::new();
                            for value in values.iter() {
                                if let Some(shard) =
                                    self.compute_shard_with_ctx(column, value.clone(), ctx)?
                                {
                                    shards.insert(shard);
                                }
                            }

                            match shards.len() {
                                0 => Ok(SearchResult::None),
                                1 => Ok(SearchResult::Match(shards.into_iter().next().unwrap())),
                                _ => Ok(SearchResult::Matches(shards.into_iter().collect())),
                            }
                        }
                        _ => Ok(SearchResult::None),
                    }
                }
            }

            Some(NodeEnum::List(ref list)) => {
                let mut values = vec![];

                for value in &list.items {
                    if let Ok(value) = Value::try_from(&value.node) {
                        values.push(value);
                    }
                }

                Ok(SearchResult::Values(values))
            }

            Some(NodeEnum::WithClause(ref with_clause)) => {
                for cte in &with_clause.ctes {
                    let result = self.select_search(cte, ctx)?;
                    if !result.is_none() {
                        return Ok(result);
                    }
                }

                Ok(SearchResult::None)
            }

            Some(NodeEnum::JoinExpr(ref join)) => {
                let mut results = vec![];

                if let Some(ref left) = join.larg {
                    results.push(self.select_search(left, ctx)?);
                }
                if let Some(ref right) = join.rarg {
                    results.push(self.select_search(right, ctx)?);
                }

                results.retain(|result| result.is_match());

                let result = results
                    .into_iter()
                    .fold(SearchResult::None, |acc, x| acc.merge(x));

                Ok(result)
            }

            Some(NodeEnum::BoolExpr(ref expr)) => {
                // Only AND expressions can determine a shard.
                // OR expressions could route to multiple shards.
                if expr.boolop() != BoolExprType::AndExpr {
                    return Ok(SearchResult::None);
                }

                for arg in &expr.args {
                    let result = self.select_search(arg, ctx)?;
                    if result.is_match() {
                        return Ok(result);
                    }
                }

                Ok(SearchResult::None)
            }

            Some(NodeEnum::SubLink(ref sublink)) => {
                if let Some(ref subselect) = sublink.subselect {
                    return self.select_search(subselect, ctx);
                }
                Ok(SearchResult::None)
            }

            Some(NodeEnum::CommonTableExpr(ref cte)) => {
                if let Some(ref ctequery) = cte.ctequery {
                    return self.select_search(ctequery, ctx);
                }
                Ok(SearchResult::None)
            }

            _ => Ok(SearchResult::None),
        }
    }

    /// Search a SELECT statement with its own context.
    fn search_select_stmt(
        &mut self,
        stmt: &'a SelectStmt,
        ctx: &SearchContext<'a>,
    ) -> Result<SearchResult<'a>, Error> {
        // Handle UNION/INTERSECT/EXCEPT (set operations)
        // These have larg and rarg instead of a regular SELECT structure
        if let Some(ref larg) = stmt.larg {
            let larg_ctx = SearchContext::from_from_clause(&larg.from_clause);
            let result = self.search_select_stmt(larg, &larg_ctx)?;
            if !result.is_none() {
                return Ok(result);
            }
        }
        if let Some(ref rarg) = stmt.rarg {
            let rarg_ctx = SearchContext::from_from_clause(&rarg.from_clause);
            let result = self.search_select_stmt(rarg, &rarg_ctx)?;
            if !result.is_none() {
                return Ok(result);
            }
        }

        if let Some(ref with_clause) = stmt.with_clause {
            for cte in &with_clause.ctes {
                let result = self.select_search(cte, ctx)?;
                if !result.is_none() {
                    return Ok(result);
                }
            }
        }

        // Search WHERE clause
        if let Some(ref where_clause) = stmt.where_clause {
            let result = self.select_search(where_clause, ctx)?;
            if !result.is_none() {
                return Ok(result);
            }
        }

        for from_ in &stmt.from_clause {
            let result = self.select_search(from_, ctx)?;
            if !result.is_none() {
                return Ok(result);
            }
        }

        Ok(SearchResult::None)
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

    /// Search an UPDATE statement for sharding keys.
    fn search_update_stmt(
        &mut self,
        stmt: &'a UpdateStmt,
        ctx: &SearchContext<'a>,
    ) -> Result<SearchResult<'a>, Error> {
        // Handle CTEs (WITH clause)
        if let Some(ref with_clause) = stmt.with_clause {
            for cte in &with_clause.ctes {
                let result = self.select_search(cte, ctx)?;
                if !result.is_none() {
                    return Ok(result);
                }
            }
        }

        // Search WHERE clause
        if let Some(ref where_clause) = stmt.where_clause {
            let result = self.select_search(where_clause, ctx)?;
            if !result.is_none() {
                return Ok(result);
            }
        }

        // Search FROM clause (UPDATE ... FROM ...)
        for from_ in &stmt.from_clause {
            let result = self.select_search(from_, ctx)?;
            if !result.is_none() {
                return Ok(result);
            }
        }

        Ok(SearchResult::None)
    }

    /// Search a DELETE statement for sharding keys.
    fn search_delete_stmt(
        &mut self,
        stmt: &'a DeleteStmt,
        ctx: &SearchContext<'a>,
    ) -> Result<SearchResult<'a>, Error> {
        // Handle CTEs (WITH clause)
        if let Some(ref with_clause) = stmt.with_clause {
            for cte in &with_clause.ctes {
                let result = self.select_search(cte, ctx)?;
                if !result.is_none() {
                    return Ok(result);
                }
            }
        }

        // Search WHERE clause
        if let Some(ref where_clause) = stmt.where_clause {
            let result = self.select_search(where_clause, ctx)?;
            if !result.is_none() {
                return Ok(result);
            }
        }

        // Search USING clause (DELETE ... USING ...)
        for using_ in &stmt.using_clause {
            let result = self.select_search(using_, ctx)?;
            if !result.is_none() {
                return Ok(result);
            }
        }

        Ok(SearchResult::None)
    }

    /// Search an INSERT statement for sharding keys.
    fn search_insert_stmt(
        &mut self,
        stmt: &'a InsertStmt,
        ctx: &SearchContext<'a>,
    ) -> Result<SearchResult<'a>, Error> {
        // Handle CTEs (WITH clause)
        if let Some(ref with_clause) = stmt.with_clause {
            for cte in &with_clause.ctes {
                let result = self.select_search(cte, ctx)?;
                if !result.is_none() {
                    return Ok(result);
                }
            }
        }

        // Get the column names from INSERT INTO table (col1, col2, ...)
        let columns: Vec<&str> = stmt
            .cols
            .iter()
            .filter_map(|node| match &node.node {
                Some(NodeEnum::ResTarget(target)) => Some(target.name.as_str()),
                _ => None,
            })
            .collect();

        // The select_stmt field contains either VALUES or a SELECT subquery
        if let Some(ref select_node) = stmt.select_stmt {
            if let Some(NodeEnum::SelectStmt(ref select_stmt)) = select_node.node {
                // Check if this is VALUES (has values_lists) - need special handling
                // to match column positions with sharding keys
                if !select_stmt.values_lists.is_empty() {
                    for values_list in &select_stmt.values_lists {
                        if let Some(NodeEnum::List(ref list)) = values_list.node {
                            for (pos, value_node) in list.items.iter().enumerate() {
                                // Check if this position corresponds to a sharding key column
                                if let Some(column_name) = columns.get(pos) {
                                    let column = Column {
                                        name: column_name,
                                        table: ctx.table.map(|t| t.name),
                                        schema: ctx.table.and_then(|t| t.schema),
                                    };

                                    if self.schema.tables().get_table(column).is_some() {
                                        // Try to extract the value directly
                                        if let Ok(value) = Value::try_from(value_node) {
                                            if let Some(shard) =
                                                self.compute_shard_with_ctx(column, value, ctx)?
                                            {
                                                return Ok(SearchResult::Match(shard));
                                            }
                                        }
                                    }
                                }

                                // Search subqueries in values recursively
                                let result = self.select_search(value_node, ctx)?;
                                if result.is_match() {
                                    return Ok(result);
                                }
                            }
                        }
                    }
                }

                // Handle INSERT ... SELECT by recursively searching the SelectStmt
                let result = self.select_search(select_node, ctx)?;
                if !result.is_none() {
                    return Ok(result);
                }
            }
        }

        Ok(SearchResult::None)
    }
}

#[cfg(test)]
mod test {
    use pgdog_config::{FlexibleType, Mapping, ShardedMapping, ShardedMappingKind, ShardedTable};

    use crate::backend::ShardedTables;
    use crate::net::messages::{Bind, Parameter};

    use super::*;

    fn run_test(stmt: &str, bind: Option<&Bind>) -> Result<Option<Shard>, Error> {
        let schema = ShardingSchema {
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
                        mapping: Mapping::new(&[ShardedMapping {
                            kind: ShardedMappingKind::List,
                            values: vec![FlexibleType::Integer(1), FlexibleType::Integer(2)]
                                .into_iter()
                                .collect(),
                            ..Default::default()
                        }]),
                        ..Default::default()
                    },
                    // Schema-qualified sharded table with different column name
                    ShardedTable {
                        column: "tenant_id".into(),
                        name: Some("schema_sharded".into()),
                        schema: Some("myschema".into()),
                        ..Default::default()
                    },
                ],
                vec![],
                false,
            ),
            ..Default::default()
        };
        let raw = pg_query::parse(stmt)
            .unwrap()
            .protobuf
            .stmts
            .first()
            .cloned()
            .unwrap();
        let mut parser = StatementParser::from_raw(&raw, bind, &schema, None)?;
        parser.shard()
    }

    #[test]
    fn test_simple_select() {
        let result = run_test("SELECT * FROM sharded WHERE id = 1", None);
        assert!(result.unwrap().is_some());
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
    fn test_insert_no_sharding_key_returns_none() {
        let result = run_test("INSERT INTO sharded (name) VALUES ('foo')", None);
        assert!(result.unwrap().is_none());
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
        let raw = pg_query::parse(stmt)
            .unwrap()
            .protobuf
            .stmts
            .first()
            .cloned()
            .unwrap();
        let mut parser = StatementParser::from_raw(&raw, bind, &schema, None)?;
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
}
