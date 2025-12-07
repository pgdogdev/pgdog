use std::collections::{HashMap, HashSet};

use pg_query::{
    protobuf::{AExprKind, BoolExprType, RawStmt, SelectStmt},
    Node, NodeEnum,
};

use super::{
    super::sharding::Value as ShardingValue, explain_trace::ExplainRecorder, Column, Error, Table,
    Value,
};
use crate::{
    backend::ShardingSchema,
    frontend::router::{parser::Shard, sharding::ContextBuilder},
    net::Bind,
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

pub struct SelectParser<'a, 'b> {
    stmt: &'a SelectStmt,
    bind: Option<&'b Bind>,
    schema: &'b ShardingSchema,
}

impl<'a, 'b> SelectParser<'a, 'b> {
    pub fn new(stmt: &'a SelectStmt, bind: Option<&'b Bind>, schema: &'b ShardingSchema) -> Self {
        Self { stmt, bind, schema }
    }

    pub fn from_raw(
        raw: &'a RawStmt,
        bind: Option<&'b Bind>,
        schema: &'b ShardingSchema,
    ) -> Result<Self, Error> {
        match raw.stmt.as_ref().and_then(|n| n.node.as_ref()) {
            Some(NodeEnum::SelectStmt(stmt)) => Ok(Self::new(stmt, bind, schema)),
            _ => Err(Error::NotASelect),
        }
    }

    pub fn shard(&mut self) -> Result<Option<Shard>, Error> {
        let ctx = SearchContext::from_from_clause(&self.stmt.from_clause);
        let result = self.search_select_stmt(self.stmt, &ctx)?;

        match result {
            SearchResult::Match(shard) => Ok(Some(shard)),
            SearchResult::Matches(shards) => Ok(Self::converge(&shards)),
            _ => Ok(None),
        }
    }

    fn converge(shards: &[Shard]) -> Option<Shard> {
        let shards: HashSet<Shard> = shards.into_iter().cloned().collect();
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
        &'a self,
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
        &'a self,
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
                    return self.select_search(node, ctx);
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
                            if is_any {
                                if matches!(values, SearchResult::Value(_)) {
                                    if self.schema.tables().get_table(column).is_some() {
                                        return Ok(SearchResult::Match(Shard::All));
                                    }
                                }
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
        &'a self,
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
        &'a self,
        column: Column<'a>,
        value: Value<'a>,
        ctx: &SearchContext<'a>,
    ) -> Result<Option<Shard>, Error> {
        // Resolve table alias if present
        let column = if let Some(table_ref) = column.table() {
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

        self.compute_shard(column, value)
    }
}

#[cfg(test)]
mod test {
    use pgdog_config::{FlexibleType, Mapping, ShardedMapping, ShardedMappingKind, ShardedTable};

    use crate::backend::ShardedTables;
    use crate::net::messages::{Bind, Parameter};

    use super::*;

    fn run_select_test(stmt: &str, bind: Option<&Bind>) -> Result<Option<Shard>, Error> {
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
        let mut parser = SelectParser::from_raw(&raw, bind, &schema)?;
        parser.shard()
    }

    #[test]
    fn test_simple_select() {
        let result = run_select_test("SELECT * FROM sharded WHERE id = 1", None);
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_select_with_and() {
        let result =
            run_select_test("SELECT * FROM sharded WHERE id = 1 AND name = 'foo'", None).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_select_with_or_returns_none() {
        // OR expressions can't determine a single shard
        let result = run_select_test("SELECT * FROM sharded WHERE id = 1 OR id = 2", None).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_select_with_subquery() {
        let result = run_select_test(
            "SELECT * FROM sharded WHERE id IN (SELECT sharded_id FROM other WHERE sharded_id = 1)",
            None,
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_select_with_cte() {
        let result = run_select_test(
            "WITH cte AS (SELECT * FROM sharded WHERE id = 1) SELECT * FROM cte",
            None,
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_select_with_join() {
        let result = run_select_test(
            "SELECT * FROM sharded s JOIN other o ON s.id = o.sharded_id WHERE s.id = 1",
            None,
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_select_with_type_cast() {
        let result = run_select_test("SELECT * FROM sharded WHERE id = '1'::int", None).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_select_from_subquery() {
        let result = run_select_test(
            "SELECT * FROM (SELECT * FROM sharded WHERE id = 1) AS sub",
            None,
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_select_with_nested_cte() {
        let result = run_select_test(
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
        let result = run_select_test("SELECT * FROM sharded", None).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_select_with_in_list() {
        let result = run_select_test("SELECT * FROM sharded WHERE id IN (1, 2, 3)", None).unwrap();
        // IN with multiple values should return a shard match
        assert!(result.is_some());
    }

    #[test]
    fn test_select_with_not_equals_returns_none() {
        // != operator is not supported for sharding
        let result = run_select_test("SELECT * FROM sharded WHERE id != 1", None).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_select_with_greater_than_returns_none() {
        // > operator is not supported for sharding
        let result = run_select_test("SELECT * FROM sharded WHERE id > 1", None).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_select_with_complex_and() {
        let result = run_select_test(
            "SELECT * FROM sharded WHERE id = 1 AND status = 'active' AND created_at > now()",
            None,
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_select_with_left_join() {
        let result = run_select_test(
            "SELECT * FROM sharded s LEFT JOIN other o ON s.id = o.sharded_id WHERE s.id = 1",
            None,
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_select_with_multiple_joins() {
        let result = run_select_test(
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
        let result = run_select_test(
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
        let result = run_select_test(
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
        let result = run_select_test(
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
        let result = run_select_test(
            "SELECT * FROM sharded WHERE id = 1 UNION SELECT * FROM sharded WHERE id = 2",
            None,
        )
        .unwrap();
        // UNION queries should find at least one shard
        assert!(result.is_some());
    }

    #[test]
    fn test_select_with_nested_subselects() {
        let result = run_select_test(
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
        let result = run_select_test("SELECT * FROM sharded WHERE id = $1", Some(&bind)).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_bound_select_with_and() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_select_test(
            "SELECT * FROM sharded WHERE id = $1 AND name = 'foo'",
            Some(&bind),
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_bound_select_with_or_returns_none() {
        let bind = Bind::new_params("", &[Parameter::new(b"1"), Parameter::new(b"2")]);
        let result = run_select_test(
            "SELECT * FROM sharded WHERE id = $1 OR id = $2",
            Some(&bind),
        )
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_bound_select_with_subquery() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_select_test(
            "SELECT * FROM sharded WHERE id IN (SELECT sharded_id FROM other WHERE sharded_id = $1)",
            Some(&bind),
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_bound_select_with_cte() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_select_test(
            "WITH cte AS (SELECT * FROM sharded WHERE id = $1) SELECT * FROM cte",
            Some(&bind),
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_bound_select_with_join() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_select_test(
            "SELECT * FROM sharded s JOIN other o ON s.id = o.sharded_id WHERE s.id = $1",
            Some(&bind),
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_bound_select_with_type_cast() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result =
            run_select_test("SELECT * FROM sharded WHERE id = $1::int", Some(&bind)).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_bound_select_from_subquery() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_select_test(
            "SELECT * FROM (SELECT * FROM sharded WHERE id = $1) AS sub",
            Some(&bind),
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_bound_select_with_nested_cte() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_select_test(
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
        let result = run_select_test(
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
        let result =
            run_select_test("SELECT * FROM sharded WHERE id = ANY($1)", Some(&bind)).unwrap();
        assert_eq!(result, Some(Shard::All));
    }

    #[test]
    fn test_bound_select_with_not_equals_returns_none() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_select_test("SELECT * FROM sharded WHERE id != $1", Some(&bind)).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_bound_select_with_greater_than_returns_none() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_select_test("SELECT * FROM sharded WHERE id > $1", Some(&bind)).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_bound_select_with_complex_and() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_select_test(
            "SELECT * FROM sharded WHERE id = $1 AND status = 'active' AND created_at > now()",
            Some(&bind),
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_bound_select_with_left_join() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_select_test(
            "SELECT * FROM sharded s LEFT JOIN other o ON s.id = o.sharded_id WHERE s.id = $1",
            Some(&bind),
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_bound_select_with_multiple_joins() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_select_test(
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
        let result = run_select_test(
            "SELECT * FROM sharded WHERE EXISTS (SELECT 1 FROM other WHERE sharded_id = $1)",
            Some(&bind),
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_bound_select_with_scalar_subquery() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_select_test(
            "SELECT * FROM sharded WHERE id = (SELECT sharded_id FROM other WHERE sharded_id = $1 LIMIT 1)",
            Some(&bind),
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_bound_select_with_recursive_cte() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_select_test(
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
        let result = run_select_test(
            "SELECT * FROM sharded WHERE id = $1 UNION SELECT * FROM sharded WHERE id = $2",
            Some(&bind),
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_bound_select_with_nested_subselects() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_select_test(
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
        let result = run_select_test(
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
        let result = run_select_test(
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
        let result = run_select_test(
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
        let result = run_select_test(
            "SELECT * FROM myschema.schema_sharded WHERE tenant_id = 1",
            None,
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_select_with_schema_qualified_alias() {
        let result = run_select_test(
            "SELECT * FROM myschema.schema_sharded s WHERE s.tenant_id = 1",
            None,
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_bound_select_with_schema_qualified_alias() {
        let bind = Bind::new_params("", &[Parameter::new(b"1")]);
        let result = run_select_test(
            "SELECT * FROM myschema.schema_sharded s WHERE s.tenant_id = $1",
            Some(&bind),
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_select_with_schema_qualified_join() {
        let result = run_select_test(
            "SELECT * FROM myschema.schema_sharded s \
             JOIN other o ON s.id = o.sharded_id WHERE s.tenant_id = 1",
            None,
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_select_wrong_schema_returns_none() {
        let result = run_select_test(
            "SELECT * FROM otherschema.schema_sharded WHERE tenant_id = 1",
            None,
        )
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_select_wrong_schema_alias_returns_none() {
        let result = run_select_test(
            "SELECT * FROM otherschema.schema_sharded s WHERE s.tenant_id = 1",
            None,
        )
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_select_with_any_array_literal() {
        let result =
            run_select_test("SELECT * FROM sharded WHERE id = ANY('{1, 2, 3}')", None).unwrap();
        // ANY with array literal routes to all shards
        assert_eq!(result, Some(Shard::All));
    }
}
