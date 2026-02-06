//! FDW fallback detection for queries that cannot be executed across shards.
//!
//! Determines when a query should be sent to postgres_fdw instead of being
//! executed directly by pgdog's cross-shard query engine.

use pg_query::{protobuf::SelectStmt, Node, NodeEnum};

use crate::backend::Schema;
use crate::frontend::router::parser::statement::StatementParser;
use crate::frontend::router::parser::Table;
use crate::net::parameter::ParameterValue;

/// Context for FDW fallback checking that holds schema lookup information.
pub(crate) struct FdwFallbackContext<'a> {
    pub db_schema: &'a Schema,
    pub user: &'a str,
    pub search_path: Option<&'a ParameterValue>,
}

impl<'a, 'b, 'c> StatementParser<'a, 'b, 'c> {
    /// Check if a SELECT statement requires FDW fallback due to CTEs or subqueries
    /// that reference unsharded tables without proper sharding keys.
    ///
    /// A CTE/subquery is considered "safe" if:
    /// 1. It only references sharded or omnisharded tables, OR
    /// 2. It contains a sharding key in its WHERE clause (handled by correlation)
    pub(crate) fn needs_fdw_fallback_for_subqueries(
        &self,
        stmt: &SelectStmt,
        ctx: &FdwFallbackContext,
        has_sharding_key: bool,
    ) -> bool {
        // If the main query already has a sharding key, subqueries are considered
        // correlated and inherit the sharding context
        if has_sharding_key {
            return false;
        }

        // Check CTEs in WITH clause
        if let Some(ref with_clause) = stmt.with_clause {
            for cte in &with_clause.ctes {
                if let Some(NodeEnum::CommonTableExpr(ref cte_expr)) = cte.node {
                    if let Some(ref ctequery) = cte_expr.ctequery {
                        if let Some(NodeEnum::SelectStmt(ref inner_select)) = ctequery.node {
                            if self.check_select_needs_fallback(inner_select, ctx) {
                                return true;
                            }
                        }
                    }
                }
            }
        }

        // Check subqueries in FROM clause
        for from_node in &stmt.from_clause {
            if self.check_node_needs_fallback(from_node, ctx) {
                return true;
            }
        }

        // Check subqueries in WHERE clause
        if let Some(ref where_clause) = stmt.where_clause {
            if self.check_node_needs_fallback(where_clause, ctx) {
                return true;
            }
        }

        false
    }

    /// Recursively check if a SELECT statement needs FDW fallback.
    fn check_select_needs_fallback(&self, stmt: &SelectStmt, ctx: &FdwFallbackContext) -> bool {
        // Handle UNION/INTERSECT/EXCEPT
        if let Some(ref larg) = stmt.larg {
            if self.check_select_needs_fallback(larg, ctx) {
                return true;
            }
        }
        if let Some(ref rarg) = stmt.rarg {
            if self.check_select_needs_fallback(rarg, ctx) {
                return true;
            }
        }

        // Check tables in FROM clause
        for from_node in &stmt.from_clause {
            if self.check_from_node_has_unsafe_table(from_node, ctx) {
                return true;
            }
        }

        // Recursively check nested CTEs
        if let Some(ref with_clause) = stmt.with_clause {
            for cte in &with_clause.ctes {
                if let Some(NodeEnum::CommonTableExpr(ref cte_expr)) = cte.node {
                    if let Some(ref ctequery) = cte_expr.ctequery {
                        if let Some(NodeEnum::SelectStmt(ref inner_select)) = ctequery.node {
                            if self.check_select_needs_fallback(inner_select, ctx) {
                                return true;
                            }
                        }
                    }
                }
            }
        }

        // Recursively check subqueries in FROM
        for from_node in &stmt.from_clause {
            if self.check_node_needs_fallback(from_node, ctx) {
                return true;
            }
        }

        // Check subqueries in WHERE
        if let Some(ref where_clause) = stmt.where_clause {
            if self.check_node_needs_fallback(where_clause, ctx) {
                return true;
            }
        }

        false
    }

    /// Check if a node contains subqueries that need FDW fallback.
    fn check_node_needs_fallback(&self, node: &Node, ctx: &FdwFallbackContext) -> bool {
        match &node.node {
            Some(NodeEnum::RangeSubselect(subselect)) => {
                if let Some(ref subquery) = subselect.subquery {
                    if let Some(NodeEnum::SelectStmt(ref inner_select)) = subquery.node {
                        return self.check_select_needs_fallback(inner_select, ctx);
                    }
                }
                false
            }
            Some(NodeEnum::SubLink(sublink)) => {
                if let Some(ref subselect) = sublink.subselect {
                    if let Some(NodeEnum::SelectStmt(ref inner_select)) = subselect.node {
                        return self.check_select_needs_fallback(inner_select, ctx);
                    }
                }
                false
            }
            Some(NodeEnum::JoinExpr(join)) => {
                let mut needs_fallback = false;
                if let Some(ref larg) = join.larg {
                    needs_fallback |= self.check_node_needs_fallback(larg, ctx);
                }
                if let Some(ref rarg) = join.rarg {
                    needs_fallback |= self.check_node_needs_fallback(rarg, ctx);
                }
                needs_fallback
            }
            Some(NodeEnum::BoolExpr(bool_expr)) => {
                for arg in &bool_expr.args {
                    if self.check_node_needs_fallback(arg, ctx) {
                        return true;
                    }
                }
                false
            }
            Some(NodeEnum::AExpr(a_expr)) => {
                if let Some(ref lexpr) = a_expr.lexpr {
                    if self.check_node_needs_fallback(lexpr, ctx) {
                        return true;
                    }
                }
                if let Some(ref rexpr) = a_expr.rexpr {
                    if self.check_node_needs_fallback(rexpr, ctx) {
                        return true;
                    }
                }
                false
            }
            _ => false,
        }
    }

    /// Check if a FROM clause node references an unsafe (unsharded) table.
    fn check_from_node_has_unsafe_table(&self, node: &Node, ctx: &FdwFallbackContext) -> bool {
        match &node.node {
            Some(NodeEnum::RangeVar(range_var)) => {
                let table = Table::from(range_var);
                !self.is_table_safe(&table, ctx)
            }
            Some(NodeEnum::JoinExpr(join)) => {
                let mut has_unsafe = false;
                if let Some(ref larg) = join.larg {
                    has_unsafe |= self.check_from_node_has_unsafe_table(larg, ctx);
                }
                if let Some(ref rarg) = join.rarg {
                    has_unsafe |= self.check_from_node_has_unsafe_table(rarg, ctx);
                }
                has_unsafe
            }
            Some(NodeEnum::RangeSubselect(_)) => {
                // Subselects are checked separately for their contents
                false
            }
            _ => false,
        }
    }

    /// Check if a table is "safe" (sharded or omnisharded).
    fn is_table_safe(&self, table: &Table, ctx: &FdwFallbackContext) -> bool {
        let sharded_tables = self.sharding_schema().tables();

        // Check named sharded table configs
        for config in sharded_tables.tables() {
            if let Some(ref config_name) = config.name {
                if table.name == config_name {
                    // Also check schema match if specified in config
                    if let Some(ref config_schema) = config.schema {
                        let config_schema_str: &str = config_schema.as_str();
                        if table.schema != Some(config_schema_str) {
                            continue;
                        }
                    }
                    return true;
                }
            }
        }

        // Check nameless configs by looking up the table in the db schema
        let nameless_configs: Vec<_> = sharded_tables
            .tables()
            .iter()
            .filter(|t| t.name.is_none())
            .collect();

        if !nameless_configs.is_empty() {
            if let Some(relation) = ctx.db_schema.table(*table, ctx.user, ctx.search_path) {
                for config in &nameless_configs {
                    if relation.has_column(&config.column) {
                        return true;
                    }
                }
            }
        }

        // Check if it's an omnisharded table
        if sharded_tables.omnishards().contains_key(table.name) {
            return true;
        }

        false
    }
}
