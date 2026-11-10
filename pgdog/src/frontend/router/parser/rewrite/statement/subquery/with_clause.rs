//! Handle CTEs
//!

use crate::frontend::router::parser::rewrite::statement::{
    insert::Subquery,
    subquery::{SubqueryRewrite, where_clause::parse_where_clause},
};

use pg_raw_parse::{Node, nodes::WithClause, walk::walk};

impl<'a, 'mem> SubqueryRewrite<'a, 'mem> {
    pub(crate) fn handle_subquery_in_with_clause(&self) -> Option<Vec<Subquery>> {
        let with_clause = match self.outer_stmt {
            Node::SelectStmt(select) => select.with_clause(),
            Node::UpdateStmt(update) => update.with_clause(),
            _ => return None,
        }?;

        let cte_names = cte_names(with_clause);
        let ctes = with_clause.ctes();

        let mut list_of_subqueries: Vec<Subquery> = Vec::new();
        for cte in ctes {
            if !reads_tables(cte.ctequery(), &cte_names) {
                continue;
            }

            list_of_subqueries.push(Subquery::Cte(cte.ctename().unwrap().to_string()))
        }

        Some(list_of_subqueries)
    }
}

impl<'a, 'mem> SubqueryRewrite<'a, 'mem> {
    /// If there's a CTE within a WHERE clause, we need to evaluate the CTE separately from the WHERE,
    /// so that <>
    pub(crate) fn handle_ctes_in_where_clause_subquery(&self) -> Vec<Subquery> {
        let Some(Node::SelectStmt(subselect)) = parse_where_clause(self.outer_stmt) else {
            return vec![];
        };

        let Some(with_clause) = subselect.with_clause() else {
            return vec![];
        };

        let cte_names = cte_names(with_clause);

        with_clause
            .ctes()
            .into_iter()
            .filter(|cte| reads_tables(cte.ctequery(), &cte_names))
            .filter_map(|cte| cte.ctename())
            .map(|cte_name| Subquery::WhereClauseCte(cte_name.to_string()))
            .collect()
    }
}

fn cte_names(with_clause: &WithClause) -> Vec<&str> {
    with_clause
        .ctes()
        .into_iter()
        .filter_map(|cte| cte.ctename())
        .collect()
}

/// Determine if the CTE references actual tables (as opposed to other CTEs).
/// If not, then we don't need to re-write!
fn reads_tables(cte_query: Node<'_>, cte_names: &[&str]) -> bool {
    let mut reads_tables = false;

    walk(cte_query, |node| {
        if let Node::RangeVar(range_var) = node {
            let is_cte_reference = range_var.schemaname().is_none()
                && range_var
                    .relname()
                    .is_some_and(|relname| cte_names.contains(&relname));

            if !is_cte_reference {
                reads_tables = true;
            }
        }
    });

    reads_tables
}

/// If `entire_ast` contains a CTE with alias `alias`, then return the corresponding
/// AST for the query of that CTE.
/// This is used in our multi-step code, but provided here as it's parsing logic
pub(crate) fn parse_with_clause<'a>(entire_ast: Node<'a>, alias: &String) -> Option<Node<'a>> {
    let with_clause = match entire_ast {
        Node::SelectStmt(select) => select.with_clause(),
        Node::UpdateStmt(update) => update.with_clause(),
        _ => return None,
    }?;

    let ctes = with_clause.ctes();

    for cte in ctes {
        let cte_query = cte.ctequery();

        if let Node::SelectStmt(cte_query) = cte_query
            && !cte_query.values_lists().is_empty()
        {
            // If it does NOT have an empty VALUES list, then it has already been re-written by us (or we don't need to eval it)
            continue;
        }

        if let Some(cte_name) = cte.ctename()
            && cte_name == alias
        {
            return Some(cte.ctequery());
        }
    }

    None
}
