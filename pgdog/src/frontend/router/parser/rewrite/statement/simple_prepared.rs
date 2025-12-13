use pg_query::{Error as PgQueryError, NodeEnum};

use crate::frontend::PreparedStatements;
use crate::net::Parse;

use super::Error;

/// Result of rewriting a PREPARE or EXECUTE SQL command.
#[derive(Debug, Clone)]
pub enum SimplePreparedRewrite {
    /// Node was not a PREPARE or EXECUTE statement.
    None,
    /// PREPARE statement was rewritten.
    Prepared,
    /// EXECUTE statement was rewritten. Contains the global name and statement
    /// needed to prepend a ProtocolMessage::Prepare.
    Executed { name: String, statement: String },
}

/// Rewrites `PREPARE` and `EXECUTE` SQL commands to use
/// globally cached prepared statement names.
///
/// # More details
///
/// `PREPARE __stmt_1 AS SELECT $1` is rewritten as `PREPARE __pgdog_1 AS SELECT $1` and
/// `SELECT $1` is stored in the global cache using `insert_anyway`.
///
/// `EXECUTE __stmt_1(1)` is rewritten to `EXECUTE __pgdog_1(1)`. Additionally, the caller
/// should prepend `ProtocolMessage::Prepare` to the client request using the returned
/// name and statement.
///
pub fn rewrite_simple_prepared(
    node: &mut NodeEnum,
    prepared_statements: &mut PreparedStatements,
) -> Result<SimplePreparedRewrite, Error> {
    match node {
        NodeEnum::PrepareStmt(stmt) => {
            let query = stmt
                .query
                .as_ref()
                .ok_or(Error::PgQuery(PgQueryError::Parse(
                    "missing query in PREPARE".into(),
                )))?
                .deparse()
                .map_err(Error::PgQuery)?;

            let mut parse = Parse::named(&stmt.name, &query);
            prepared_statements.insert_anyway(&mut parse);
            stmt.name = parse.name().to_string();

            Ok(SimplePreparedRewrite::Prepared)
        }

        NodeEnum::ExecuteStmt(stmt) => {
            let parse = prepared_statements.parse(&stmt.name);
            if let Some(parse) = parse {
                let global_name = parse.name().to_string();
                let statement = parse.query().to_string();
                stmt.name = global_name.clone();

                Ok(SimplePreparedRewrite::Executed {
                    name: global_name,
                    statement,
                })
            } else {
                Err(Error::PgQuery(PgQueryError::Parse(format!(
                    "prepared statement '{}' does not exist",
                    stmt.name
                ))))
            }
        }

        _ => Ok(SimplePreparedRewrite::None),
    }
}

#[cfg(test)]
mod tests {
    use super::super::StatementRewrite;
    use super::*;
    use pg_query::parse;

    #[test]
    fn test_rewrite_prepare() {
        let mut ast = parse("PREPARE test_stmt AS SELECT $1, $2")
            .unwrap()
            .protobuf;
        let mut ps = PreparedStatements::default();
        let mut rewrite = StatementRewrite::new(&mut ast, false, &mut ps);
        let plan = rewrite.maybe_rewrite().unwrap();

        let sql = ast.deparse().unwrap();
        assert!(
            sql.contains("__pgdog_"),
            "PREPARE should be renamed to __pgdog_N, got: {sql}"
        );
        assert!(
            !sql.contains("test_stmt"),
            "Original name should be replaced: {sql}"
        );
        assert!(plan.prepares.is_empty());
        assert!(plan.stmt.is_some());
    }

    #[test]
    fn test_rewrite_execute() {
        let mut ps = PreparedStatements::default();

        // First, prepare a statement
        let mut ast = parse("PREPARE test_stmt AS SELECT 1").unwrap().protobuf;
        let mut rewrite = StatementRewrite::new(&mut ast, false, &mut ps);
        rewrite.maybe_rewrite().unwrap();

        // Now execute it
        let mut ast = parse("EXECUTE test_stmt").unwrap().protobuf;
        let mut rewrite = StatementRewrite::new(&mut ast, false, &mut ps);
        let plan = rewrite.maybe_rewrite().unwrap();

        let sql = ast.deparse().unwrap();
        assert!(
            sql.contains("__pgdog_"),
            "EXECUTE should use global name, got: {sql}"
        );
        assert_eq!(plan.prepares.len(), 1);
        let (name, statement) = &plan.prepares[0];
        assert!(name.starts_with("__pgdog_"));
        assert_eq!(statement, "SELECT 1");
    }

    #[test]
    fn test_rewrite_execute_with_params() {
        let mut ps = PreparedStatements::default();

        // First, prepare a statement with parameters
        let mut ast = parse("PREPARE test_stmt AS SELECT $1, $2")
            .unwrap()
            .protobuf;
        let mut rewrite = StatementRewrite::new(&mut ast, false, &mut ps);
        rewrite.maybe_rewrite().unwrap();

        // Now execute it with arguments
        let mut ast = parse("EXECUTE test_stmt(1, 'hello')").unwrap().protobuf;
        let mut rewrite = StatementRewrite::new(&mut ast, false, &mut ps);
        let plan = rewrite.maybe_rewrite().unwrap();

        let sql = ast.deparse().unwrap();
        assert!(
            sql.contains("__pgdog_"),
            "EXECUTE should use global name, got: {sql}"
        );
        assert!(
            sql.contains("(1, 'hello')"),
            "EXECUTE params should be preserved, got: {sql}"
        );
        assert_eq!(plan.prepares.len(), 1);
    }

    #[test]
    fn test_execute_nonexistent_fails() {
        let mut ps = PreparedStatements::default();

        let mut ast = parse("EXECUTE nonexistent_stmt").unwrap().protobuf;
        let mut rewrite = StatementRewrite::new(&mut ast, false, &mut ps);
        let result = rewrite.maybe_rewrite();

        assert!(result.is_err());
    }

    #[test]
    fn test_no_rewrite_for_regular_select() {
        let mut ast = parse("SELECT 1, 2, 3").unwrap().protobuf;
        let mut ps = PreparedStatements::default();
        let mut rewrite = StatementRewrite::new(&mut ast, false, &mut ps);
        let plan = rewrite.maybe_rewrite().unwrap();

        let sql = ast.deparse().unwrap();
        assert_eq!(sql, "SELECT 1, 2, 3");
        assert!(plan.prepares.is_empty());
        assert!(plan.stmt.is_none());
    }
}
