use crate::{
    frontend::ClientRequest,
    net::{
        Parse, ProtocolMessage,
        bind::{Bind, Parameter},
    },
    util::random_string,
};
use pg_raw_parse::{
    ConstValue, NodeMut,
    make::MemoryToken,
    nodes,
    transform::{self, Assignable, Transform},
};

use super::*;

#[derive(Default, Clone, Debug)]
pub(crate) struct SimpleToPreparedPlan {
    /// Parameters using text encoding.
    pub(crate) params: Vec<Parameter>,

    pub(crate) step_two: SimpleToPreparedPlanStepTwo,
}

#[derive(Default, Clone, Debug)]
pub(crate) struct SimpleToPreparedPlanStepTwo {
    pub(crate) bind: Bind,
    pub(crate) parse: Parse,
}

impl SimpleToPreparedPlan {
    pub(super) fn step_two(
        &mut self,
        prepared_statements: &mut PreparedStatements,
        stmt: &str,
    ) -> Result<(), Error> {
        if self.params.is_empty() {
            return Ok(());
        }

        let mut parse = Parse::new_anonymous(stmt);

        let (_, name) = PreparedStatements::global().write().insert(&parse);
        parse.rename(&name);

        let parse = Parse::named(&name, stmt);
        let bind = Bind::new_params(&name, &self.params);

        // This will ensure the global counter for this prepared statement
        // is correctly decreased when this client disconnects.
        //
        // We are using the global name for the local cache because
        // the client doesn't know it's using prepared statements and will never
        // manually close it.
        prepared_statements.insert_local_mapping(parse.name(), parse.name());

        self.step_two = SimpleToPreparedPlanStepTwo { parse, bind };

        Ok(())
    }

    pub(crate) fn apply(&self, request: &mut ClientRequest) {}
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
        if self.extended || self.prepared {
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

impl LiteralRewriter<'_> {
    fn parameter(value: Option<ConstValue<'_>>) -> Option<Parameter> {
        match value {
            None => Some(Parameter::new_null()),
            Some(ConstValue::Integer(value)) => {
                Some(Parameter::new(itoa::Buffer::new().format(value).as_bytes()))
            }
            Some(ConstValue::Float(value))
            | Some(ConstValue::String(value))
            | Some(ConstValue::BitString(value)) => Some(Parameter::new(value.as_bytes())),
            Some(ConstValue::Boolean(value)) => {
                Some(Parameter::new(if value { b"true" } else { b"false" }))
            }
            Some(_) => None,
        }
    }
}

impl<'mem> Transform<'mem> for LiteralRewriter<'mem> {
    fn transform_node<'mutref>(&mut self, node: Assignable<'mem, 'mutref>) {
        let parameter = match &*node {
            NodeMut::A_Const(constant) => Self::parameter(constant.val()),
            _ => None,
        };

        if let Some(parameter) = parameter {
            self.params.push(parameter);
            node.replace(self.mem.make_param_ref(self.next_param).uncast());
            self.next_param += 1;
        } else {
            transform::transform_node(node.into_inner(), self);
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

        assert_eq!(sql, "SELECT $1, $2, $3, $4, $5");
        assert_eq!(params.len(), 5);
        assert_eq!(params[0].data.as_ref(), b"42");
        assert_eq!(params[1].data.as_ref(), b"hello");
        assert_eq!(params[2].data.as_ref(), b"true");
        assert_eq!(params[3].len, -1);
        assert_eq!(params[4].data.as_ref(), b"1.25");
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
            "SELECT * FROM users WHERE id = $1 AND name IN ($2, $3) LIMIT $5 OFFSET $4"
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
