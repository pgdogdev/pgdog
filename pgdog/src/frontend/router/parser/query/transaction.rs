use crate::frontend::client::TransactionType;

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
        let extended = !context.query()?.simple();

        if context.rw_conservative() && !context.read_only {
            self.write_override = true;
        }

        match stmt.kind() {
            TransactionStmtKind::TransStmtCommit => {
                return Ok(Command::CommitTransaction { extended })
            }
            TransactionStmtKind::TransStmtRollback => {
                return Ok(Command::RollbackTransaction { extended })
            }
            TransactionStmtKind::TransStmtBegin | TransactionStmtKind::TransStmtStart => {
                self.in_transaction = true;
                let transaction_type = Self::transaction_type(&stmt.options).unwrap_or_default();
                return Ok(Command::StartTransaction {
                    query: context.query()?.clone(),
                    transaction_type,
                    extended,
                });
            }
            TransactionStmtKind::TransStmtPrepare
            | TransactionStmtKind::TransStmtCommitPrepared
            | TransactionStmtKind::TransStmtRollbackPrepared => {
                if context.router_context.two_pc {
                    return Err(Error::NoTwoPc);
                }
            }
            _ => (),
        }

        Ok(Command::Query(Route::write(None)))
    }

    #[inline]
    fn transaction_type(options: &[Node]) -> Option<TransactionType> {
        for option_node in options {
            let node_enum = option_node.node.as_ref()?;
            if let NodeEnum::DefElem(def_elem) = node_enum {
                if def_elem.defname == "transaction_read_only" {
                    let arg_node = def_elem.arg.as_ref()?.node.as_ref()?;
                    if let NodeEnum::AConst(ac) = arg_node {
                        // 1 => read-only, 0 => read-write
                        if let Some(a_const::Val::Ival(i)) = ac.val.as_ref() {
                            if i.ival != 0 {
                                return Some(TransactionType::ReadOnly);
                            }
                        }
                    }
                }
            }
        }

        Some(TransactionType::ReadWrite)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_detect_transaction_type() {
        let read_write_queries = vec![
            "BEGIN",
            "BEGIN;",
            "begin",
            "bEgIn",
            "BEGIN WORK",
            "BEGIN TRANSACTION",
            "BEGIN READ WRITE",
            "BEGIN WORK READ WRITE",
            "BEGIN TRANSACTION READ WRITE",
            "START TRANSACTION",
            "START TRANSACTION;",
            "start transaction",
            "START TRANSACTION READ WRITE",
            "BEGIN ISOLATION LEVEL REPEATABLE READ READ WRITE DEFERRABLE",
        ];

        let read_only_queries = vec![
            "BEGIN READ ONLY",
            "BEGIN WORK READ ONLY",
            "BEGIN TRANSACTION READ ONLY",
            "START TRANSACTION READ ONLY",
            "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY",
            "START TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY NOT DEFERRABLE",
        ];

        for q in read_write_queries {
            let binding = pg_query::parse(q).unwrap();
            let stmt = binding
                .protobuf
                .stmts
                .first()
                .as_ref()
                .unwrap()
                .stmt
                .as_ref()
                .unwrap();

            match stmt.node {
                Some(NodeEnum::TransactionStmt(ref stmt)) => {
                    let t = QueryParser::transaction_type(&stmt.options);
                    assert_eq!(t, Some(TransactionType::ReadWrite));
                }
                _ => panic!("not a transaction"),
            }
        }

        for q in read_only_queries {
            let binding = pg_query::parse(q).unwrap();
            let stmt = binding
                .protobuf
                .stmts
                .first()
                .as_ref()
                .unwrap()
                .stmt
                .as_ref()
                .unwrap();

            match stmt.node {
                Some(NodeEnum::TransactionStmt(ref stmt)) => {
                    let t = QueryParser::transaction_type(&stmt.options);
                    assert_eq!(t, Some(TransactionType::ReadOnly));
                }
                _ => panic!("not a transaction"),
            }
        }
    }
}
