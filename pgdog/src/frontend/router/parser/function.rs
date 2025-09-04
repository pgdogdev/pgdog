use std::collections::HashMap;

use once_cell::sync::Lazy;
use pg_query::{protobuf, Node, NodeEnum};

static WRITE_ONLY: Lazy<HashMap<&'static str, LockingBehavior>> = Lazy::new(|| {
    HashMap::from([
        ("pg_advisory_lock", LockingBehavior::Lock),
        ("pg_advisory_xact_lock", LockingBehavior::None),
        ("pg_advisory_lock_shared", LockingBehavior::Lock),
        ("pg_advisory_xact_lock_shared", LockingBehavior::None),
        ("pg_try_advisory_lock", LockingBehavior::Lock),
        ("pg_try_advisory_xact_lock", LockingBehavior::None),
        ("pg_try_advisory_lock_shared", LockingBehavior::Lock),
        ("pg_try_advisory_xact_lock_shared", LockingBehavior::None),
        ("pg_advisory_unlock_all", LockingBehavior::Unlock),
        ("nextval", LockingBehavior::None),
        ("setval", LockingBehavior::None),
    ])
});

#[derive(Debug, Copy, Clone, PartialEq, Default)]
pub(crate) enum LockingBehavior {
    Lock,
    Unlock,
    #[default]
    None,
}

#[derive(Default, Debug, Copy, Clone)]
pub(crate) struct FunctionBehavior {
    pub(crate) writes: bool,
    pub(crate) locking_behavior: LockingBehavior,
}

impl FunctionBehavior {
    pub(crate) fn writes_only() -> FunctionBehavior {
        FunctionBehavior {
            writes: true,
            ..Default::default()
        }
    }
}

pub(crate) struct Function<'a> {
    pub(crate) name: &'a str,
}

impl<'a> Function<'a> {
    fn from_string(node: &'a Option<NodeEnum>) -> Result<Self, ()> {
        match node {
            Some(NodeEnum::String(protobuf::String { sval })) => Ok(Self {
                name: sval.as_str(),
            }),

            _ => Err(()),
        }
    }

    /// This function likely writes.
    pub(crate) fn behavior(&self) -> FunctionBehavior {
        if let Some(locks) = WRITE_ONLY.get(&self.name) {
            FunctionBehavior {
                writes: true,
                locking_behavior: *locks,
            }
        } else {
            FunctionBehavior::default()
        }
    }
}

impl<'a> TryFrom<&'a Node> for Function<'a> {
    type Error = ();
    fn try_from(value: &'a Node) -> Result<Self, Self::Error> {
        match &value.node {
            Some(NodeEnum::FuncCall(func)) => {
                if let Some(node) = func.funcname.last() {
                    return Self::from_string(&node.node);
                }
            }

            Some(NodeEnum::TypeCast(cast)) => {
                if let Some(node) = cast.arg.as_ref() {
                    return Self::try_from(node.as_ref());
                }
            }

            Some(NodeEnum::ResTarget(res)) => {
                if let Some(val) = &res.val {
                    return Self::try_from(val.as_ref());
                }
            }

            _ => (),
        }

        Err(())
    }
}

#[cfg(test)]
mod test {
    use pg_query::parse;

    use super::*;

    #[test]
    fn test_function() {
        let ast =
            parse("SELECT pg_advisory_lock(234234), pg_try_advisory_lock(23234)::bool").unwrap();
        let root = ast.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();

        match root.node.as_ref() {
            Some(NodeEnum::SelectStmt(stmt)) => {
                for node in &stmt.target_list {
                    let func = Function::try_from(node).unwrap();
                    assert!(func.name.contains("advisory_lock"));
                }
            }

            _ => panic!("not a select"),
        }
    }
}
