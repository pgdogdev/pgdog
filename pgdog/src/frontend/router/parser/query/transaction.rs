use crate::frontend::client::TransactionType;
#[cfg(feature = "new_parser")]
use pg_raw_parse::nodes::TransactionStmtKind::*;

use super::*;

impl QueryParser {
    /// Handle transaction control statements, e.g. BEGIN, ROLLBACK, COMMIT.
    ///
    /// # Arguments
    ///
    /// * `stmt`: Transaction statement from pg_query.
    /// * `context`: Query parser context.
    ///
    #[cfg(feature = "new_parser")]
    pub(super) fn transaction(
        &mut self,
        stmt: &nodes::TransactionStmt,
        context: &mut QueryParserContext,
    ) -> Result<Command, Error> {
        let extended = !context.query()?.simple();
        let mut rollback_savepoint = false;

        if context.rw_conservative() && !context.read_only {
            self.write_override = true;
        }

        match stmt.kind {
            TRANS_STMT_COMMIT => {
                return Ok(Command::CommitTransaction { extended });
            }
            TRANS_STMT_ROLLBACK => {
                return Ok(Command::RollbackTransaction { extended });
            }
            TRANS_STMT_BEGIN | TRANS_STMT_START => {
                let transaction_type = Self::transaction_type(stmt.options()).unwrap_or_default();
                return Ok(Command::StartTransaction {
                    query: context.query()?.clone(),
                    transaction_type,
                    extended,
                    route: Route::write(context.shards_calculator.shard())
                        .with_read(transaction_type == TransactionType::ReadOnly),
                });
            }
            TRANS_STMT_ROLLBACK_TO => rollback_savepoint = true,
            TRANS_STMT_PREPARE | TRANS_STMT_COMMIT_PREPARED | TRANS_STMT_ROLLBACK_PREPARED
                if context.router_context.two_pc =>
            {
                return Err(Error::NoTwoPc);
            }
            _ => (),
        }

        context
            .shards_calculator
            .push(ShardWithPriority::new_table(Shard::All));

        Ok(Command::Query(
            Route::write(context.shards_calculator.shard())
                .with_rollback_savepoint(rollback_savepoint),
        ))
    }

    cfg_select! {
        not(feature = "new_parser") => {
            pub(super) fn transaction(
                &mut self,
                stmt: &TransactionStmt,
                context: &mut QueryParserContext,
            ) -> Result<Command, Error> {
                let extended = !context.query()?.simple();
                let mut rollback_savepoint = false;

                if context.rw_conservative() && !context.read_only {
                    self.write_override = true;
                }

                match stmt.kind() {
                    TransactionStmtKind::TransStmtCommit => {
                        return Ok(Command::CommitTransaction { extended });
                    }
                    TransactionStmtKind::TransStmtRollback => {
                        return Ok(Command::RollbackTransaction { extended });
                    }
                    TransactionStmtKind::TransStmtBegin | TransactionStmtKind::TransStmtStart => {
                        let transaction_type = Self::transaction_type(&stmt.options).unwrap_or_default();
                        return Ok(Command::StartTransaction {
                            query: context.query()?.clone(),
                            transaction_type,
                            extended,
                            route: Route::write(context.shards_calculator.shard())
                                .with_read(transaction_type == TransactionType::ReadOnly),
                        });
                    }
                    TransactionStmtKind::TransStmtRollbackTo => rollback_savepoint = true,
                    TransactionStmtKind::TransStmtPrepare
                    | TransactionStmtKind::TransStmtCommitPrepared
                    | TransactionStmtKind::TransStmtRollbackPrepared
                        if context.router_context.two_pc =>
                    {
                        return Err(Error::NoTwoPc);
                    }
                    _ => (),
                }

                context
                    .shards_calculator
                    .push(ShardWithPriority::new_table(Shard::All));

                Ok(Command::Query(
                    Route::write(context.shards_calculator.shard())
                        .with_rollback_savepoint(rollback_savepoint),
                ))
            }
        }
        _ => {}
    }

    #[cfg(feature = "new_parser")]
    fn transaction_type<'a>(
        options: impl IntoIterator<Item = Node<'a>>,
    ) -> Option<TransactionType> {
        for option in options {
            if let Node::DefElem(def_elem) = option
                && def_elem.defname() == Some("transaction_read_only")
            {
                if let Node::A_Const(ac) = def_elem.arg()
                    && let Some(val) = ac.val()
                    && let Some(1) = val.numeric_value::<i32>()
                {
                    return Some(TransactionType::ReadOnly);
                }
            }
        }

        Some(TransactionType::ReadWrite)
    }

    cfg_select! {
        not(feature = "new_parser") => {
            fn transaction_type(options: &[PgNode]) -> Option<TransactionType> {
                for option_node in options {
                    let node_enum = option_node.node.as_ref()?;
                    if let NodeEnum::DefElem(def_elem) = node_enum
                        && def_elem.defname == "transaction_read_only"
                    {
                        let arg_node = def_elem.arg.as_ref()?.node.as_ref()?;
                        if let NodeEnum::AConst(ac) = arg_node {
                            // 1 => read-only, 0 => read-write
                            if let Some(a_const::Val::Ival(i)) = ac.val.as_ref()
                                && i.ival != 0
                            {
                                return Some(TransactionType::ReadOnly);
                            }
                        }
                    }
                }

                Some(TransactionType::ReadWrite)
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    #[cfg(feature = "new_parser")]
    fn test_detect_transaction_type() {
        let read_write_queries = [
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

        let read_only_queries = [
            "BEGIN READ ONLY",
            "BEGIN WORK READ ONLY",
            "BEGIN TRANSACTION READ ONLY",
            "START TRANSACTION READ ONLY",
            "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY",
            "START TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY NOT DEFERRABLE",
        ];

        for q in read_write_queries {
            let ast = pg_raw_parse::parse(q).unwrap();
            let Some(Node::TransactionStmt(stmt)) = ast.stmts().next() else {
                unreachable!("not a transaction")
            };

            let t = QueryParser::transaction_type(stmt.options());
            assert_eq!(t, Some(TransactionType::ReadWrite));
        }

        for q in read_only_queries {
            let ast = pg_raw_parse::parse(q).unwrap();
            let Some(Node::TransactionStmt(stmt)) = ast.stmts().next() else {
                unreachable!("not a transaction")
            };

            let t = QueryParser::transaction_type(stmt.options());
            assert_eq!(t, Some(TransactionType::ReadOnly));
        }
    }

    cfg_select! {
        not(feature = "new_parser") => {
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
        _ => {}
    }
}
