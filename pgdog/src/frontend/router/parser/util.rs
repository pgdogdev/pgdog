use pg_query::NodeEnum;
use pg_query::protobuf::{Node, String as PgString};
use std::cmp::PartialEq;

pub(crate) fn pg_string(s: impl Into<String>) -> Node {
    node(NodeEnum::String(PgString { sval: s.into() }))
}

pub(crate) fn node(n: NodeEnum) -> Node {
    Node { node: Some(n) }
}

/// A const type that can be compared with Node for equality
pub(crate) const fn pg_str(s: &str) -> PgStr<'_> {
    PgStr(s)
}

#[derive(Debug)]
pub(crate) struct PgStr<'a>(&'a str);

impl PartialEq<Node> for PgStr<'_> {
    fn eq(&self, rhs: &Node) -> bool {
        match &rhs.node {
            Some(NodeEnum::String(PgString { sval })) => sval == self.0,
            _ => false,
        }
    }
}
