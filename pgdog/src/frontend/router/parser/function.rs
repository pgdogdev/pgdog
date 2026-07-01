use pg_query::{Node, NodeEnum, protobuf};

const WRITE_ONLY: &[&str] = &["nextval", "setval"];

const CROSS_SHARD: &[(Option<&str>, &str)] = &[(Some("pgdog"), "install_sharded_sequence")];

#[derive(Default, Debug, Copy, Clone)]
pub(crate) struct FunctionBehavior {
    pub(crate) writes: bool,
    pub(crate) cross_shard: bool,
}

pub(crate) struct Function<'a> {
    pub(crate) name: &'a str,
    pub(crate) schema: Option<&'a str>,
}

impl<'a> Function<'a> {
    /// Build a Function from a qualified name list (as found in `FuncCall.funcname`).
    /// The last element is the function name; the preceding element (if any) is the
    /// schema.
    pub(crate) fn from_strings(
        mut parts: impl DoubleEndedIterator<Item = &'a str>,
    ) -> Option<Self> {
        Some(Self {
            name: parts.next_back()?,
            schema: parts.next_back(),
        })
    }

    /// This function likely writes.
    pub(crate) fn behavior(&self) -> FunctionBehavior {
        FunctionBehavior {
            writes: WRITE_ONLY.contains(&self.name),
            cross_shard: CROSS_SHARD.contains(&(self.schema, self.name)),
        }
    }
}

impl<'a> TryFrom<&'a Node> for Function<'a> {
    type Error = ();
    fn try_from(value: &'a Node) -> Result<Self, Self::Error> {
        match &value.node {
            Some(NodeEnum::FuncCall(func)) => {
                let strings = func.funcname.iter().filter_map(|s| match &s.node {
                    Some(NodeEnum::String(protobuf::String { sval })) => Some(sval.as_str()),
                    _ => None,
                });
                Self::from_strings(strings).ok_or(())
            }

            Some(NodeEnum::TypeCast(cast)) if let Some(node) = cast.arg.as_ref() => {
                Self::try_from(node.as_ref())
            }

            Some(NodeEnum::ResTarget(res)) if let Some(val) = &res.val => {
                Self::try_from(val.as_ref())
            }

            Some(NodeEnum::NullTest(test)) if let Some(node) = test.arg.as_ref() => {
                Self::try_from(node.as_ref())
            }

            _ => Err(()),
        }
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
