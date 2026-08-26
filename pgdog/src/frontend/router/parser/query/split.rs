use pg_raw_parse::{deparse, raw::TransactionStmtKind::*};

use super::*;

impl QueryParser {
    /// Check if the statement contains multiple queries.
    ///
    /// If that's the case, check that we can execute it safely, and if we can,
    /// either return it as-is or ask the query engine to re-execute statements separately.
    ///
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
                let check = Self::split_execution_no_transaction_safe(ast);
                // TODO(lev): We can still mis-route things here, e.g.,
                // SELECT 1; INSERT INTO [...];
                // will use the first statement for routing and send the whole
                // thing to a replica, causing an error.
                let no_sharding = context.shards == 1;
                // Postgres will abort a multi-statement extended protocol execution,
                // we don't need to worry about it.
                let extended = context.router_context.extended;

                // /* pgdog_shard */ manual override makes this a safe, direct-to-shard query
                // without splitting.
                let direct_to_shard =
                    context.shards > 1 && context.shards_calculator.shard().is_direct();

                let safe_not_to_split =
                    no_sharding || extended || direct_to_shard || check.only_ddl();

                if safe_not_to_split {
                    // Safe to use the first statement for routing.
                    Ok(None)
                } else if check.statements() <= 1 && !check.open_txn {
                    Ok(Some(Self::split(ast)?))
                } else {
                    Err(Error::MultiStatementSafety)
                }
            }
            Err(Error::MultiStatementMixedSet) => {
                let check = Self::split_execution_no_transaction_safe(ast);
                if check.statements() > 1 {
                    Err(Error::MultiStatementSafety)
                } else {
                    Ok(Some(Self::split(ast)?))
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
    fn split_execution_no_transaction_safe(ast: &Ast) -> CheckResult {
        let mut check = CheckResult::default();
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
                Node::SelectStmt(_)
                | Node::ExecuteStmt(_)
                | Node::InsertStmt(_)
                | Node::UpdateStmt(_)
                | Node::DeleteStmt(_) => {
                    if !inside_txn {
                        check.no_txn_dml += 1;
                    }
                }
                _ => {
                    if !inside_txn {
                        check.no_txn_ddl += 1;
                    }
                }
            }
        }

        check.open_txn = txn_stmts % 2 != 0;
        check.txn = txn_stmts / 2;

        check
    }
}

#[derive(Default, Debug)]
struct CheckResult {
    no_txn_dml: u32,
    no_txn_ddl: u32,
    open_txn: bool,
    txn: u32,
}

impl CheckResult {
    // Any statements executed outside an explicit
    // transaction cannot be safely replayed without us
    // starting another transaction.
    fn statements(&self) -> u32 {
        self.no_txn_dml + self.no_txn_ddl
    }

    fn no_transactions(&self) -> bool {
        self.txn == 0 && !self.open_txn
    }

    fn only_ddl(&self) -> bool {
        self.no_txn_dml == 0 && self.no_transactions()
    }
}
