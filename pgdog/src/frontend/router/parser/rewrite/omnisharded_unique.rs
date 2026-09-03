//! Perform a rewrite of pgdog.omnisharded_id().

use pg_raw_parse::{Node, walk};

use crate::backend::{Cluster, pool::Request};

use super::super::{Ast, Error};

pub(crate) struct RewriteOmnishardedUnique<'a> {
    cluster: &'a Cluster,
    ast: &'a Ast,
}

impl RewriteOmnishardedUnique<'_> {
    pub(crate) async fn rewrite_simple(&self) -> Result<String, Error> {
        let mut count = 0;

        for stmt in self.ast.ast.stmts() {
            walk::walk(stmt, |node| {
                let Node::FuncCall(function) = node else {
                    return;
                };

                let _args = function
                    .args()
                    .iter()
                    .filter_map(|arg| {
                        let Node::A_Const(value) = arg else {
                            return None;
                        };

                        value.val()?.string_value().map(str::to_owned)
                    })
                    .collect::<Vec<_>>();

                if function
                    .funcname()
                    .iter()
                    .filter_map(Node::as_str)
                    .eq(["pgdog", "omnisharded_id"])
                {
                    count += 1;
                }
            });
        }

        Ok(count.to_string())
    }

    async fn get_next_id(&self, name: &str) -> Result<i64, crate::backend::Error> {
        let mut shard_0 = self.cluster.primary(0, &Request::default()).await?;
        let val: Vec<i64> = shard_0
            .fetch_all(format!("SELECT nextval('{}')", name))
            .await?;

        Ok(val.get(0).expect("sequence to return a value").clone())
    }
}
