use crate::{
    frontend::ClientRequest,
    net::{
        Describe, Execute, Parse, ProtocolMessage, Sync,
        bind::{Bind, Parameter},
    },
};
use pg_raw_parse::{
    ConstValue, Node, NodeMut,
    make::{MemoryToken, Unique},
    nodes,
    transform::{self, Assignable, Transform},
};

use super::*;

#[derive(Default, Clone, Debug)]
pub(crate) struct SimpleToPreparedPlan {
    /// Parameters using text encoding.
    pub(crate) params: Vec<Parameter>,

    pub(crate) step_two: Option<SimpleToPreparedPlanStepTwo>,
}

#[derive(Default, Clone, Debug)]
pub(crate) struct SimpleToPreparedPlanStepTwo {
    pub(super) bind: Bind,
    pub(super) parse: Parse,
}

impl SimpleToPreparedPlan {
    /// This step runs after all other rewriters are done.
    ///
    /// This is to ensure we cache the prepared statement after all rewrites are complete.
    ///
    pub(super) fn step_two(
        &mut self,
        prepared_statements: &mut PreparedStatements,
        stmt: &str,
    ) -> Result<(), Error> {
        if self.params.is_empty() {
            return Ok(());
        }

        let mut parse = Parse::new_anonymous(stmt);
        let name = prepared_statements.insert_rewritten_simple_to_prepared(&parse);
        parse.rename(&name);

        let parse = Parse::named(&name, stmt);
        let bind = Bind::new_params(&name, &self.params);

        self.step_two = Some(SimpleToPreparedPlanStepTwo { parse, bind });

        Ok(())
    }

    /// Rewrite the request from simple protocol to prepared.
    ///
    /// INVARIANT: the request contains one single [`crate::net::Query`] message.
    /// This is enforced by [`crate::frontend::client::Client::buffer`] and [`ClientRequest::is_complete`].
    ///
    pub(crate) fn apply(&self, request: &mut ClientRequest) -> bool {
        if let Some(ref step_two) = self.step_two {
            request.clear();
            request.push(ProtocolMessage::Parse(step_two.parse.clone()));
            request.push(ProtocolMessage::Describe(Describe::new_statement(
                step_two.parse.name(),
            )));
            request.push(ProtocolMessage::Bind(step_two.bind.clone()));
            request.push(ProtocolMessage::Execute(Execute::new()));
            request.push(ProtocolMessage::Sync(Sync));
            true
        } else {
            false
        }
    }
}

impl StatementRewrite<'_> {
    /// Rewrite a simple query protocol request to a prepared one.
    ///
    /// # Example
    ///
    /// ```sql
    /// SELECT * FROM users WHERE id = 1;
    /// ```
    ///
    /// becomes
    ///
    /// ```sql
    /// SELECT * FROM users WHERE id = $1;
    /// ```
    ///
    /// This also returns parameters extracted from the query, e.g.:
    ///
    /// ```no_compile
    /// vec![
    ///         Parameter {
    ///             data: b"1",
    ///             len: 1,
    ///         }
    /// ]
    /// ```
    ///
    pub(super) fn rewrite_simple_to_prepared<'a>(
        &mut self,
        node: NodeMut<'a, '_>,
        mem: MemoryToken<'a>,
        plan: &mut RewritePlan,
    ) -> Result<(), Error> {
        // Only rewrite simple statements.
        if self.extended || self.prepared || !self.schema.rewrite.simple_to_prepared {
            return Ok(());
        }

        let simple_plan = rewrite_literals(node, mem);

        if !simple_plan.params.is_empty() {
            self.rewritten = true;
            plan.simple_to_prepared = simple_plan;
        }

        Ok(())
    }
}

/// Replaces constants in executable expressions while leaving constants which
/// are part of SQL syntax (such as the precision and scale in `numeric(5, 2)`)
/// untouched.
fn rewrite_literals<'a>(node: NodeMut<'a, '_>, mem: MemoryToken<'a>) -> SimpleToPreparedPlan {
    if !matches!(
        &node,
        NodeMut::SelectStmt(_)
            | NodeMut::InsertStmt(_)
            | NodeMut::UpdateStmt(_)
            | NodeMut::DeleteStmt(_)
    ) {
        return SimpleToPreparedPlan::default();
    }

    let mut rewriter = LiteralRewriter {
        mem,
        params: Vec::new(),
        next_param: 1,
    };
    transform::transform_node(node, &mut rewriter);

    SimpleToPreparedPlan {
        params: rewriter.params,
        ..Default::default()
    }
}

struct LiteralRewriter<'mem> {
    mem: MemoryToken<'mem>,
    params: Vec<Parameter>,
    next_param: i32,
}

impl<'mem> LiteralRewriter<'mem> {
    fn parameter(value: Option<ConstValue<'_>>) -> Option<(Parameter, &'static str)> {
        match value {
            // An untyped NULL gets its type from the surrounding expression.
            // Casting it to an arbitrary type can make otherwise valid
            // expressions fail (for example, `integer IS DISTINCT FROM NULL`).
            None => None,
            Some(ConstValue::Integer(value)) => Some((
                Parameter::new(itoa::Buffer::new().format(value).as_bytes()),
                "int8",
            )),
            Some(ConstValue::Float(value)) => Some((
                Parameter::new(value.as_bytes()),
                if value.parse::<i64>().is_ok() {
                    "int8"
                } else {
                    "numeric"
                },
            )),
            Some(ConstValue::String(value)) => Some((
                Parameter::new(value.as_bytes()),
                if value.parse::<uuid::Uuid>().is_ok() {
                    "uuid"
                } else {
                    "text"
                },
            )),
            Some(ConstValue::BitString(value)) => Some((Parameter::new(value.as_bytes()), "bit")),
            Some(ConstValue::Boolean(value)) => Some((
                Parameter::new(if value { b"true" } else { b"false" }),
                "bool",
            )),
            Some(_) => None,
        }
    }

    fn replacement(
        &mut self,
        value: Option<ConstValue<'_>>,
        explicit_type: bool,
    ) -> Option<Unique<'mem, Node<'mem>>> {
        let (parameter, parameter_type) = Self::parameter(value)?;

        self.params.push(parameter);
        let parameter = self.mem.make_param_ref(self.next_param).uncast();
        let replacement = if explicit_type {
            parameter
        } else {
            let type_name = if parameter_type == "text" {
                self.mem
                    .make_list(&[self.mem.make_string(Some(parameter_type))])
            } else {
                self.mem.make_list(&[
                    self.mem.make_string(Some("pg_catalog")),
                    self.mem.make_string(Some(parameter_type)),
                ])
            };
            self.mem.make_type_cast(parameter, type_name).uncast()
        };
        self.next_param += 1;
        Some(replacement)
    }
}

impl<'mem> Transform<'mem> for LiteralRewriter<'mem> {
    fn transform_node<'mutref>(&mut self, node: Assignable<'mem, 'mutref>) {
        let replacement = match &*node {
            NodeMut::A_Const(constant) => self.replacement(constant.val(), false),
            _ => None,
        };

        if let Some(replacement) = replacement {
            node.replace(replacement);
        } else {
            transform::transform_node(node.into_inner(), self);
        }
    }

    fn transform_type_cast<'mutref>(&mut self, mut node: nodes::TypeCastMut<'mem, 'mutref>) {
        let replacement = match node.arg() {
            Node::A_Const(constant) => self.replacement(constant.val(), true),
            _ => None,
        };

        if let Some(replacement) = replacement {
            node.set_arg(replacement);
        } else {
            transform::transform_type_cast(node, self);
        }
    }

    // Type modifiers are represented as A_Const nodes too, but replacing them
    // would produce invalid SQL (`numeric($1, $2)`). The value being cast is
    // reached through TypeCast.arg and is still rewritten normally.
    fn transform_type_name<'mutref>(&mut self, _node: nodes::TypeNameMut<'mem, 'mutref>) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rewrite(sql: &str) -> (String, Vec<Parameter>) {
        let parsed = pg_raw_parse::parse(sql).expect("test query should parse");
        let mut params = Vec::new();
        let rewritten = pg_raw_parse::make::owned(|mem| {
            let mut copy = mem.make_unique(&*parsed.into_inner());
            let mut stmt = copy
                .as_mut()
                .into_iter()
                .next()
                .expect("test query should contain a statement");
            let plan = rewrite_literals(stmt.stmt_mut(), mem);
            params = plan.params;
            copy
        });
        let sql = pg_raw_parse::deparse_stmts(&*rewritten)
            .expect("rewritten query should deparse")
            .as_str()
            .to_owned();
        (sql, params)
    }

    #[test]
    fn rewrites_constants_in_parameter_order() {
        let (sql, params) = rewrite("SELECT 42, 'hello', true, NULL, 1.25");

        assert_eq!(
            sql,
            "SELECT $1::bigint, $2::text, $3::boolean, NULL, $4::numeric"
        );
        assert_eq!(params.len(), 4);
        assert_eq!(params[0].data.as_ref(), b"42");
        assert_eq!(params[1].data.as_ref(), b"hello");
        assert_eq!(params[2].data.as_ref(), b"true");
        assert_eq!(params[3].data.as_ref(), b"1.25");
    }

    #[test]
    fn leaves_untyped_nulls_in_place() {
        let (sql, params) = rewrite("SELECT 1 IS DISTINCT FROM NULL, NULL::integer");

        assert_eq!(sql, "SELECT $1::bigint IS DISTINCT FROM NULL, NULL::int");
        assert_eq!(params.len(), 1);
        assert_eq!(params[0].data.as_ref(), b"1");
    }

    #[test]
    fn leaves_cast_type_modifiers_in_place() {
        let (sql, params) = rewrite("SELECT 5::numeric(10, 2)");

        assert_eq!(sql, "SELECT $1::numeric(10, 2)");
        assert_eq!(params.len(), 1);
        assert_eq!(params[0].data.as_ref(), b"5");
    }

    #[test]
    fn rewrites_nested_expressions() {
        let (sql, params) = rewrite(
            "SELECT * FROM users WHERE id = 7 AND name IN ('alice', 'bob') LIMIT 10 OFFSET 2",
        );

        assert_eq!(
            sql,
            "SELECT * FROM users WHERE id = $1::bigint AND name IN ($2::text, $3::text) LIMIT $5::bigint OFFSET $4::bigint"
        );
        let params: Vec<_> = params
            .iter()
            .map(|parameter| parameter.data.as_ref())
            .collect();
        assert_eq!(
            params,
            [
                b"7" as &[u8],
                b"alice" as &[u8],
                b"bob" as &[u8],
                b"2" as &[u8],
                b"10" as &[u8]
            ]
        );
    }

    #[test]
    fn does_not_rewrite_non_dml_statements() {
        let (sql, params) = rewrite("CREATE TABLE measurements (value numeric(10, 2) DEFAULT 5)");

        assert_eq!(
            sql,
            "CREATE TABLE measurements (value numeric(10, 2) DEFAULT 5)"
        );
        assert!(params.is_empty());

        let (sql, params) = rewrite("EXPLAIN SELECT 5");

        assert_eq!(sql, "EXPLAIN SELECT 5");
        assert!(params.is_empty());
    }
}
