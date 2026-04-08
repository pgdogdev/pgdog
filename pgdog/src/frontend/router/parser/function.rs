use std::collections::{HashMap, HashSet};

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
        ("pg_advisory_unlock", LockingBehavior::Unlock), // TODO: we don't track multiple advisory locks.
        ("nextval", LockingBehavior::None),
        ("setval", LockingBehavior::None),
    ])
});

static CROSS_SHARD: Lazy<HashSet<(&'static str, &'static str)>> =
    Lazy::new(|| HashSet::from([("pgdog", "install_sharded_sequence")]));

#[derive(Debug, Copy, Clone, PartialEq, Default)]
pub enum LockingBehavior {
    Lock,
    Unlock,
    #[default]
    None,
}

#[derive(Default, Debug, Copy, Clone)]
pub struct FunctionBehavior {
    pub writes: bool,
    pub locking_behavior: LockingBehavior,
    pub cross_shard: bool,
}

impl FunctionBehavior {
    pub fn writes_only() -> FunctionBehavior {
        FunctionBehavior {
            writes: true,
            ..Default::default()
        }
    }
}

pub struct Function<'a> {
    pub name: &'a str,
    pub schema: Option<&'a str>,
}

impl<'a> Function<'a> {
    /// Build a Function from a qualified name list (as found in `FuncCall.funcname`).
    /// The last element is the function name; the preceding element (if any) is the
    /// schema.
    fn from_strings(parts: &'a [Node]) -> Result<Self, ()> {
        let str_of = |node: &'a Node| match &node.node {
            Some(NodeEnum::String(protobuf::String { sval })) => Ok(sval.as_str()),
            _ => Err(()),
        };
        match parts {
            [name] => Ok(Self {
                name: str_of(name)?,
                schema: None,
            }),
            [.., schema, name] => Ok(Self {
                name: str_of(name)?,
                schema: Some(str_of(schema)?),
            }),
            _ => Err(()),
        }
    }

    /// This function likely writes.
    pub fn behavior(&self) -> FunctionBehavior {
        let cross_shard = self
            .schema
            .map(|schema| CROSS_SHARD.contains(&(schema, self.name)))
            .unwrap_or(false);

        if let Some(locks) = WRITE_ONLY.get(&self.name) {
            FunctionBehavior {
                writes: true,
                locking_behavior: *locks,
                cross_shard,
            }
        } else {
            FunctionBehavior {
                cross_shard,
                ..FunctionBehavior::default()
            }
        }
    }
}

impl<'a> TryFrom<&'a Node> for Function<'a> {
    type Error = ();
    fn try_from(value: &'a Node) -> Result<Self, Self::Error> {
        match &value.node {
            Some(NodeEnum::FuncCall(func)) => {
                return Self::from_strings(&func.funcname);
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

            Some(NodeEnum::NullTest(test)) => {
                if let Some(node) = test.arg.as_ref() {
                    return Self::try_from(node.as_ref());
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
                    assert!(func.schema.is_none());
                    assert!(!func.behavior().cross_shard);
                }
            }

            _ => panic!("not a select"),
        }
    }

    fn first_func<R>(query: &str, check: impl FnOnce(Function<'_>) -> R) -> R {
        let ast = parse(query).unwrap();
        let root = ast.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        match root.node.as_ref() {
            Some(NodeEnum::SelectStmt(stmt)) => {
                let target = stmt.target_list.first().unwrap();
                check(Function::try_from(target).unwrap())
            }
            _ => panic!("not a select"),
        }
    }

    #[test]
    fn test_cross_shard_function() {
        first_func(
            "SELECT pgdog.install_sharded_sequence('foo', 'id')",
            |func| {
                assert_eq!(func.name, "install_sharded_sequence");
                assert_eq!(func.schema, Some("pgdog"));
                assert!(func.behavior().cross_shard);
            },
        );

        // Same function name without the schema should not be flagged.
        first_func("SELECT install_sharded_sequence('foo', 'id')", |func| {
            assert_eq!(func.name, "install_sharded_sequence");
            assert!(func.schema.is_none());
            assert!(!func.behavior().cross_shard);
        });

        // Different schema should not be flagged.
        first_func(
            "SELECT other.install_sharded_sequence('foo', 'id')",
            |func| {
                assert_eq!(func.schema, Some("other"));
                assert!(!func.behavior().cross_shard);
            },
        );
    }
}
