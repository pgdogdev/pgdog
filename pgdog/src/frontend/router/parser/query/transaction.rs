use super::*;

impl QueryParser {
    /// Handle transaction control statements, e.g. BEGIN, ROLLBACK, COMMIT.
    ///
    /// # Arguments
    ///
    /// * `stmt`: Transaction statement from pg_query.
    /// * `context`: Query parser context.
    ///
    pub(super) fn transaction(
        &mut self,
        stmt: &TransactionStmt,
        context: &QueryParserContext,
    ) -> Result<Command, Error> {
        // Only allow to intercept transaction statements
        // if they are using the simple protocol.
        if context.query()?.simple() {
            // Send all transactions to primary.
            if context.rw_conservative() && !context.read_only {
                self.write_override = true;
            }

            match stmt.kind() {
                TransactionStmtKind::TransStmtCommit => return Ok(Command::CommitTransaction),
                TransactionStmtKind::TransStmtRollback => return Ok(Command::RollbackTransaction),
                TransactionStmtKind::TransStmtBegin | TransactionStmtKind::TransStmtStart => {
                    return Ok(Command::StartTransaction(context.query()?.clone()));
                }
                _ => Ok(Command::Query(Route::write(None))),
            }
        } else {
            Ok(Command::Query(Route::write(None)))
        }
    }
}
