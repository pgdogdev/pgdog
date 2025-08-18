use pg_query::protobuf::{a_const, node, Node, TransactionStmtKind};

use crate::{
    frontend::client::TransactionType,
    net::{CommandComplete, Protocol, ReadyForQuery},
};

use super::*;

impl QueryEngine {
    /// BEGIN
    pub(super) async fn start_transaction(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        begin: BufferedQuery,
    ) -> Result<(), Error> {
        context.transaction = detect_transaction_type(&begin);

        let bytes_sent = context
            .stream
            .send_many(&[
                CommandComplete::new_begin().message()?.backend(),
                ReadyForQuery::in_transaction(context.in_transaction()).message()?,
            ])
            .await?;

        self.stats.sent(bytes_sent);
        self.begin_stmt = Some(begin);

        Ok(())
    }
}

#[inline]
pub fn detect_transaction_type(buffered_query: &BufferedQuery) -> Option<TransactionType> {
    let simple_query = match buffered_query {
        BufferedQuery::Query(q) => q,
        _ => return None,
    };

    let parsed = pg_query::parse(simple_query.query()).ok()?;
    for raw_stmt in parsed.protobuf.stmts {
        let node_enum = raw_stmt.stmt?.node?;
        if let node::Node::TransactionStmt(txn) = node_enum {
            if is_txn_begin_kind(txn.kind) {
                return match read_only_flag(&txn.options) {
                    Some(true) => Some(TransactionType::ReadOnly),
                    Some(false) => Some(TransactionType::ReadWrite),
                    None => Some(TransactionType::ReadWrite),
                };
            }
        }
    }

    None
}

#[inline]
fn is_txn_begin_kind(kind_i32: i32) -> bool {
    let k = kind_i32;

    let is_begin = k == TransactionStmtKind::TransStmtBegin as i32;
    let is_start = k == TransactionStmtKind::TransStmtStart as i32;

    is_begin || is_start
}

#[inline]
fn read_only_flag(options: &[Node]) -> Option<bool> {
    for option_node in options {
        let node_enum = option_node.node.as_ref()?;
        if let node::Node::DefElem(def_elem) = node_enum {
            if def_elem.defname == "transaction_read_only" {
                let arg_node = def_elem.arg.as_ref()?.node.as_ref()?;
                if let node::Node::AConst(ac) = arg_node {
                    // 1 => read-only, 0 => read-write
                    if let Some(a_const::Val::Ival(i)) = ac.val.as_ref() {
                        return Some(i.ival != 0);
                    }
                }
            }
        }
    }

    None
}

#[test]
fn test_detect_transaction_type() {
    use super::*;

    use crate::frontend::client::TransactionType;
    use crate::net::Query;

    // Helper to create BufferedQuery::Query
    fn query(q: &str) -> BufferedQuery {
        BufferedQuery::Query(Query::new(q))
    }

    let none_queries = vec![
        "COMMIT",
        "ROLLBACK",
        "SET TRANSACTION READ ONLY",
        "SELECT 1",
        "INSERT INTO table VALUES (1)",
        "BEGINS",
        "START", // not START TRANSACTION
        "BEGIN INVALID OPTION",
        "",
        "INVALID",
    ];

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

    for q in none_queries {
        assert_eq!(
            detect_transaction_type(&query(q)),
            None,
            "Failed for query: {}",
            q
        );
    }

    for q in read_write_queries {
        assert_eq!(
            detect_transaction_type(&query(q)),
            Some(TransactionType::ReadWrite),
            "Failed for query: {}",
            q
        );
    }

    for q in read_only_queries {
        assert_eq!(
            detect_transaction_type(&query(q)),
            Some(TransactionType::ReadOnly),
            "Failed for query: {}",
            q
        );
    }
}
