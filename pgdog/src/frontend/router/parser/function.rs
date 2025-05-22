use std::collections::HashSet;

use once_cell::sync::Lazy;
use pg_query::{protobuf, Node, NodeEnum};

static WRITE_ONLY: Lazy<HashSet<&'static str>> = Lazy::new(|| {
    HashSet::from([
        "pg_advisory_lock",
        "pg_advisory_xact_lock",
        "pg_advisory_lock_shared",
        "pg_advisory_xact_lock_shared",
        "pg_try_advisory_lock",
        "pg_try_advisory_xact_lock",
        "pg_try_advisory_lock_shared",
        "pg_try_advisory_xact_lock_shared",
        "nextval",
        "setval",
    ])
});

pub struct Function<'a> {
    pub name: &'a str,
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
    pub fn writes(&self) -> bool {
        WRITE_ONLY.contains(&self.name)
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
                if let Some(node) = cast.arg.as_ref().map(|arg| arg) {
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
