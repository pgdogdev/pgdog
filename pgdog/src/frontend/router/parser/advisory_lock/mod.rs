//! Advisory lock extraction and result positions.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use pg_raw_parse::{ConstValue, Node, nodes};

use super::StatementParameters;
use crate::frontend::router::sharding::{varchar_extended, varchar_not_extended};

#[cfg(test)]
mod test;

/// Lifetime of an advisory lock.
///
/// Used by the query engine to decide whether the lock should survive
/// COMMIT/ROLLBACK (`Session`) or be dropped along with the transaction
/// (`Transaction` — the `pg_advisory_xact_lock*` family).
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub(crate) enum LockScope {
    Session,
    Transaction,
}

/// A pg_advisory_lock / pg_advisory_unlock call observed in a statement.
/// if `unlock_all`, it's a pg_advisory_unlock_all call
///
///  `id` is `None` when the key isn't a literal we can resolve (parameter placeholder,
/// subquery, etc.) or when the call takes no key at all (`pg_advisory_unlock_all()`).
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub(crate) struct AdvisoryLock {
    pub(crate) id: Option<AdvisoryLockId>,
    pub(crate) unlock: bool,
    pub(crate) unlock_all: bool,
    /// Whether this is a nonblocking `pg_try_advisory_*` lock attempt.
    pub(crate) try_lock: bool,
    /// Zero-based output column when this call is directly selected.
    /// None for indirect results or positions after an unresolved star expansion.
    pub(crate) column_index: Option<usize>,
    /// Zero-based source VALUES row; zero for calls that are not expanded.
    /// Source order may differ from response order after sorting or filtering.
    pub(crate) row_index: usize,
    pub(crate) scope: LockScope,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub(crate) enum AdvisoryLockId {
    /// pg_advisory_lock(ID)
    OneParameter(i64),
    /// pg_advisory_lock(ID_1, ID_2)
    TwoParameters(i32, i32),
}

impl AdvisoryLockId {
    /// Return the first parameter of the ID
    /// OneParameter(x) => x
    /// TwoParameters(x, y) => x
    pub(crate) fn get_first_parameter(self) -> i64 {
        match self {
            Self::OneParameter(x) => x,
            Self::TwoParameters(x, _) => x as i64,
        }
    }
}

/// Set of advisory locks discovered while walking a statement.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct AdvisoryLocks {
    locks: HashSet<AdvisoryLock>,
}

impl AdvisoryLocks {
    pub(crate) fn iter(&self) -> impl Iterator<Item = &AdvisoryLock> {
        self.locks.iter()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.locks.is_empty()
    }
}

impl Extend<AdvisoryLock> for AdvisoryLocks {
    fn extend<T: IntoIterator<Item = AdvisoryLock>>(&mut self, locks: T) {
        self.locks.extend(locks);
    }
}

/// Extract lock calls, including resolved keys and their result positions.
pub(crate) fn advisory_locks_from_func_call(
    func: &nodes::FuncCall,
    bind: Option<StatementParameters<'_>>,
    values_columns: Option<&ValuesColumns<'_>>,
    column_index: Option<usize>,
) -> Vec<AdvisoryLock> {
    let mut name_parts = func.funcname().into_iter().filter_map(Node::as_str);

    let name = match (name_parts.next(), name_parts.next(), name_parts.next()) {
        (Some(name), None, None) | (Some("pg_catalog"), Some(name), None) => name,
        _ => return Vec::new(),
    };

    let (unlock, scope, try_lock) = match name {
        "pg_advisory_lock" | "pg_advisory_lock_shared" => (false, LockScope::Session, false),
        "pg_try_advisory_lock" | "pg_try_advisory_lock_shared" => (false, LockScope::Session, true),
        "pg_advisory_xact_lock" | "pg_advisory_xact_lock_shared" => {
            (false, LockScope::Transaction, false)
        }
        "pg_try_advisory_xact_lock" | "pg_try_advisory_xact_lock_shared" => {
            (false, LockScope::Transaction, true)
        }
        // Session-scoped unlocks. xact locks can't be released by name;
        // Postgres drops them automatically at COMMIT/ROLLBACK.
        "pg_advisory_unlock" => (true, LockScope::Session, false),
        "pg_advisory_unlock_all" => {
            return vec![AdvisoryLock {
                id: None,
                unlock: true,
                unlock_all: true,
                try_lock: false,
                column_index,
                row_index: 0,
                scope: LockScope::Session,
            }];
        }
        _ => return Vec::new(),
    };

    // TODO: I came across this as another kind of pg_advisory_lock arg: 'users'::regclass::integer
    // that we don't handle right now (resolving to None) here as the id
    let mut arg_iterator = func.args().iter();

    let Some(arg) = arg_iterator.next() else {
        return vec![AdvisoryLock {
            id: None,
            unlock,
            unlock_all: false,
            try_lock,
            column_index,
            row_index: 0,
            scope,
        }];
    };

    let second_arg = arg_iterator.next();

    // Fast path: the key is a literal / param / cast we can resolve directly.
    if let Some(id) = integer_arg(arg, bind)
        && second_arg.is_none()
    {
        return vec![AdvisoryLock {
            id: Some(AdvisoryLockId::OneParameter(id)),
            unlock,
            unlock_all: false,
            try_lock,
            column_index,
            row_index: 0,
            scope,
        }];
    } else if let Some(first_id) = integer_arg(arg, bind)
        && let Some(second_arg) = second_arg
        && let Some(second_id) = integer_arg(second_arg, bind)
    {
        return vec![AdvisoryLock {
            id: Some(AdvisoryLockId::TwoParameters(
                first_id as i32,
                second_id as i32,
            )),
            unlock,
            unlock_all: false,
            try_lock,
            column_index,
            row_index: 0,
            scope,
        }];
    }

    // If the argument is a parameter placeholder ($1) and we have no Bind message,
    // this is just a prepared statement being parsed — the lock isn't actually
    // being taken yet. Return empty so we don't route as if a lock is held.
    if bind.is_none()
        && (is_param_ref(arg) || (second_arg.map(|s_arg| is_param_ref(s_arg)).unwrap_or(false)))
    {
        return Vec::new();
    }

    // TODO: If we have a second arg that isn't numeric,
    //  e.g., two functions, this isn't handled yet.
    if second_arg.is_some() {
        return vec![AdvisoryLock {
            id: None,
            unlock,
            unlock_all: false,
            try_lock,
            column_index,
            row_index: 0,
            scope,
        }];
    }

    // SELECT pg_advisory_lock(hashtext('some text!'))
    // Parse & evaluate a hashtext / hashtextended function within a pg_advisory_lock query.
    // The purpose is to understand what ID the func resolves to, so we can set on `AdvisoryLock`
    if let Node::FuncCall(call) = arg
        && let Some(Node::String(name)) = call.funcname().first()
        && let Some(function_name) = name.sval()
    {
        let args = call.args();
        let hash_func_evaluated_to_num = if function_name.eq("hashtext")
            && args.len() == 1
            && let Some(Node::A_Const(arg1)) = args.first()
            && let Some(ConstValue::String(hash_text)) = arg1.val()
        {
            Some(varchar_not_extended(hash_text.as_bytes()) as i64)
        } else if function_name.eq("hashtextextended")
            && args.len() == 2
            && let Some(Node::A_Const(arg1)) = args.first()
            && let Some(Node::A_Const(arg2)) = args.get(1)
            && let Some(ConstValue::String(hash_text)) = arg1.val()
            && let Some(ConstValue::Integer(seed)) = arg2.val()
        {
            // This is a u64 -> i64 cast (bitwise reinterpretation wrap-around)
            // Postgres does this same thing.
            Some(varchar_extended(hash_text.as_bytes(), seed as u64) as i64)
        } else {
            // TODO: There's likely some other funcs that are used;
            // however, hashtext and hashtextended are the most common

            // I'm really not a fan of silently routing everything else to 0.
            // I tried re-working all this to return an Error, and it was like
            // 200 LOC of changes though
            None
        };

        if hash_func_evaluated_to_num.is_some() {
            return vec![AdvisoryLock {
                id: hash_func_evaluated_to_num.map(AdvisoryLockId::OneParameter),
                unlock,
                unlock_all: false,
                try_lock,
                column_index,
                row_index: 0,
                scope,
            }];
        }
    }

    // Slow path: `SELECT pg_advisory_lock(value) FROM (VALUES (1),(2)) AS t(value)`.
    // The function is called once per row, so we emit one lock per resolved value.
    if let Node::ColumnRef(cref) = arg
        // FIXME: Don't assume the name is unqualified
        && let Some(col) = last_column_name(cref.fields())
        && let Some(rows) = values_columns.and_then(|m| m.get(col))
    {
        return rows
            .iter()
            .enumerate()
            // Skip unresolvable param refs when there is no Bind.
            .filter(|(_, v)| bind.is_some() || !is_param_ref(**v))
            .map(|(row_index, v)| AdvisoryLock {
                id: integer_arg(*v, bind).map(AdvisoryLockId::OneParameter),
                unlock,
                unlock_all: false,
                try_lock,
                column_index,
                row_index,
                scope,
            })
            .collect();
    }

    vec![AdvisoryLock {
        id: None,
        unlock,
        unlock_all: false,
        try_lock,
        column_index,
        row_index: 0,
        scope,
    }]
}

/// Return the final name in a possibly qualified column reference.
pub(crate) fn last_column_name<'a>(fields: impl IntoIterator<Item = Node<'a>>) -> Option<&'a str> {
    fields.into_iter().last().and_then(Node::as_str)
}

/// Map from unqualified VALUES column alias to the list of value nodes — one
/// per row — introduced by a `FROM (VALUES (...), ...) AS t(col, ...)` in the
/// current SELECT's FROM clause.
pub(crate) type ValuesColumns<'a> = HashMap<Cow<'a, str>, Vec<Node<'a>>>;

/// Collect source values by column name from a single VALUES subquery.
pub(crate) fn collect_values_columns(stmt: &nodes::SelectStmt) -> Option<ValuesColumns<'_>> {
    let Node::RangeSubselect(rs) = stmt.from_clause().into_iter().exactly_one().ok()? else {
        return None;
    };
    let alias = rs.alias();
    let Node::SelectStmt(s) = rs.subquery() else {
        return None;
    };
    if s.values_lists().is_empty() {
        return None;
    }
    let colnames = alias
        .map(|a| {
            a.colnames()
                .into_iter()
                .filter_map(Node::as_str)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let values = s
        .values_lists()
        .into_iter()
        .map(|values| values.expect_node_list())
        .flat_map(|row| row.into_iter().enumerate())
        .map(|(i, v)| {
            let colname = colnames
                .get(i)
                .map(|&s| Cow::Borrowed(s))
                .unwrap_or_else(|| format!("column{}", i + 1).into());
            (colname, v)
        })
        .into_group_map();
    Some(values)
}

/// Resolve an integer literal, bound parameter, or cast of either.
pub(crate) fn integer_arg(node: Node<'_>, bind: Option<StatementParameters<'_>>) -> Option<i64> {
    match node {
        Node::A_Const(a) => a.val()?.numeric_value(),
        Node::TypeCast(c) => integer_arg(c.arg(), bind),
        Node::ParamRef(param_ref) => {
            let index = (param_ref.number as usize).checked_sub(1)?;
            let param = bind?.parameter(index).ok()??;
            param.decode::<i64>()
        }
        _ => None,
    }
}

/// Check whether a node is (or wraps) a parameter placeholder (`$N`).
pub(crate) fn is_param_ref(node: Node<'_>) -> bool {
    match node {
        Node::ParamRef(_) => true,
        Node::TypeCast(cast) => is_param_ref(cast.arg()),
        _ => false,
    }
}
