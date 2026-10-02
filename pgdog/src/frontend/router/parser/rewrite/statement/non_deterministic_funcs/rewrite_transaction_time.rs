use std::ops::Deref;

use pg_raw_parse::{
    Node,
    transform::{self, Transform},
};

use crate::frontend::router::parser::rewrite::statement::{
    Error,
    non_deterministic_funcs::{NDFunction, NDFunctionType, NDRewrite, RewriteCase},
};

/// transform_node doesn't let us work with ResTargets, so we have to implement transform ourselves
/// see <https://github.com/pgdogdev/pg_raw_parse/blob/f63e7f49d85612e4507081e52fc8f40349b70580/src/transform.rs#L107>
pub(super) struct ReplaceTransactionTime<'mutr, 'mem, 'a, 's> {
    /// Ability to reference `self` within the Transform impl.
    pub(super) nd_rewrite: &'mutr mut NDRewrite<'mem, 'a, 's>,
    /// Replaced with an Error if we come across one, so we can return an Error from this function
    /// to the client.
    pub(super) outer_error: Option<Error>,
}

impl<'mutr, 'mem, 'a, 's> Transform<'mem> for ReplaceTransactionTime<'mutr, 'mem, 'a, 's> {
    /// Case, basic: SELECT now()
    ///
    /// This means now is a ResTarget, and Postgres will output the timestamptz w/ **a now column**
    /// If we replace all FunctionCall nodes with Strings, a ?col? will be returned as Postgres doesn't know
    /// that the client called now().
    ///
    /// This is handled by naming the ResTarget below.
    fn transform_res_target<'mutref>(
        &mut self,
        mut node: pg_raw_parse::nodes::ResTargetMut<'mem, 'mutref>,
    ) {
        if node.name().is_none()
            && matches!(node.val(), Node::FuncCall(_) | Node::SQLValueFunction(_))
            && let Some(nd_function_type) = match NDFunctionType::from_node(
                node.val(),
                None,
                &RewriteCase::TransactionTimeFunction,
            ) {
                Ok(nd_function_type) => nd_function_type,
                Err(e) => {
                    self.outer_error.get_or_insert(e);
                    return;
                }
            }
        {
            node.set_name(Some(
                self.nd_rewrite.mem.copy_string(nd_function_type.name()),
            ));
        }

        // Continue to traverse.
        transform::transform_res_target(node, self);
    }

    /// This covers both `FuncCall` and `SQLValueFunction` replacement.
    fn transform_node<'mutref>(
        &mut self,
        node: pg_raw_parse::transform::Assignable<'mem, 'mutref>,
    ) {
        if let Some(nd_function_type) = match NDFunctionType::from_node(
            node.deref().as_ref(),
            None,
            &RewriteCase::TransactionTimeFunction,
        ) {
            Ok(nd_function_type) => nd_function_type,
            Err(e) => {
                self.outer_error.get_or_insert(e);
                return;
            }
        } {
            let nd_func = NDFunction {
                nd_function_type,
                column_type: nd_function_type.output_type_as_str().to_string(),
            };

            node.replace(match self.nd_rewrite.make_node(&nd_func) {
                Ok(node) => node,
                Err(e) => {
                    self.outer_error.get_or_insert(e);
                    return;
                }
            });

            return;
        }

        // Continue to traverse.
        transform::transform_node(node.into_inner(), self);
    }
}
