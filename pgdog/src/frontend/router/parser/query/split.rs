use pg_raw_parse::{deparse, raw::TransactionStmtKind::*};

use super::*;

impl QueryParser {
    /// Check if the statement contains multiple queries.
    ///
    /// If that's the case, return it back to the query engine for separate
    /// re-execution.
    pub(super) fn check_multi_query_statement(
        &self,
        ast: &Ast,
        context: &QueryParserContext<'_>,
    ) -> Result<Option<Command>, Error> {
        if ast.ast.stmts().count() <= 1 {
            return Ok(None);
        }

        // In session mode, you can do whatever you want.
        if context.is_session_mode() {
            return Ok(Some(Command::Query(Route::write(
                context.shards_calculator.shard(),
            ))));
        }

        let stmts = &ast.ast;

        match self.try_multi_set(&**stmts, context) {
            Ok(Some(set)) => Ok(Some(set)),
            Ok(None) => {
                // TODO(lev): We can still mis-route things here, e.g.,
                // SELECT 1; INSERT INTO [...];
                // will use the first statement for routing and send the whole
                // thing to a replica, causing an error.
                //
                // Extended protocol containing multiple statements will be rejected by Postgres.
                if context.shards == 1
                    || context.router_context.extended
                    || (context.shards > 1 && context.shards_calculator.shard().is_direct())
                {
                    Ok(None)
                } else if Self::split_execution_no_transaction_safe(ast) {
                    Ok(Some(Self::split(ast)?))
                } else {
                    Err(Error::MultiStatementSafety)
                }
            }
            Err(Error::MultiStatementMixedSet) => {
                if !Self::split_execution_no_transaction_safe(ast) {
                    Err(Error::MultiStatementSafety)
                } else {
                    let queries = stmts
                        .stmts()
                        .map(|stmt| {
                            let query = deparse(stmt)?;
                            Ok::<_, Error>(query.as_str().to_owned())
                        })
                        .collect::<Result<Vec<_>, _>>()?;

                    Ok(Some(Command::Split(queries)))
                }
            }

            Err(err) => Err(err),
        }
    }

    fn split(ast: &Ast) -> Result<Command, Error> {
        let stmts = &ast.ast;

        let queries = stmts
            .stmts()
            .map(|stmt| {
                let query = deparse(stmt)?;
                Ok::<_, Error>(query.as_str().to_owned())
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Command::Split(queries))
    }

    // Check that all statements in the request are safe to be executed
    // without a transactional guarantee.
    //
    // TODO(lev): start implicit transaction for multi-statement queries.
    //
    fn split_execution_no_transaction_safe(ast: &Ast) -> bool {
        let mut stmts = 0;
        let mut txn_stmts = 0;
        let mut inside_txn = false;

        for stmt in ast.ast.stmts() {
            match stmt {
                Node::TransactionStmt(stmt) => {
                    if matches!(stmt.kind, TRANS_STMT_BEGIN | TRANS_STMT_START) {
                        inside_txn = true;
                        txn_stmts += 1;
                    }

                    if matches!(stmt.kind, TRANS_STMT_ROLLBACK | TRANS_STMT_COMMIT) {
                        inside_txn = false;
                        txn_stmts += 1
                    }
                }

                Node::VariableSetStmt(_) => (),
                Node::VariableShowStmt(_) => (),
                Node::DeallocateStmt(_) => (),
                Node::VacuumRelation(_) | Node::VacuumStmt(_) => (),
                Node::PrepareStmt(_) => (), // We intercept prepared statements and handle them ourselves.
                _ => {
                    if !inside_txn {
                        stmts += 1;
                    }
                }
            }
        }

        stmts <= 1 && txn_stmts % 2 == 0
    }
}
