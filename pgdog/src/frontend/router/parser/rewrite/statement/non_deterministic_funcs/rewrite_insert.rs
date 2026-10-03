use std::ops::Deref;

use pg_raw_parse::{
    Node, NodeMut,
    transform::{TransformClosure, transform_node},
};

use crate::frontend::router::parser::rewrite::statement::{
    Error,
    non_deterministic_funcs::{InsertContext, NDFunction, NDFunctionType, NDRewrite},
};

impl<'mem, 'a, 's> NDRewrite<'mem, 'a, 's> {
    /// Replaces all non-deterministic function calls (ParamRef or String)
    /// Used by all to handle re-writes.
    pub(super) fn transform_func_calls_in_insert(
        &mut self,
        stmt: NodeMut<'mem, '_>,
        insert_context: &InsertContext,
    ) -> Result<(), Error> {
        // If any Error is caught during transform_node, update this, and it'll be returned when the transform is done.
        // Have this workaround because it's within a closure.
        let mut err: Option<Error> = None;
        transform_node(
            stmt,
            &mut TransformClosure::new(|node| match &*node {
                // TODO: Is this guaranteed to be a VALUES list?
                //       What if it's something unrelated in the statement?
                NodeMut::NodeList(list_of_values) => {
                    // VALUES (...), (...) where (...) is what we're inspecting (one NodeList)

                    let mut cloned_values = self.mem.make_unique(list_of_values.deref());
                    let mut changed = false;

                    // The reason this is iterating over the NodeList instead of individual
                    // FuncCalls is that we must know where we are within a VALUES, as that
                    // allows us to know the present column's data type (for potential later coersion)
                    for (i, value) in list_of_values.iter().enumerate() {
                        let col_relation = if insert_context.cols.is_empty() {
                            insert_context
                                .relation
                                .columns
                                .get_index(i)
                                .map(|(_, column)| column)
                        } else {
                            match insert_context.cols.get(i) {
                                Some(Node::ResTarget(target)) => target
                                    .name()
                                    .and_then(|name| insert_context.relation.columns.get(name)),
                                _ => None,
                            }
                        };

                        match NDFunctionType::from_node(
                            value,
                            col_relation,
                            &insert_context.rewrite_case,
                        ) {
                            Ok(Some(nd_function_type)) => {
                                let Some(col_relation) = col_relation else {
                                    continue;
                                };

                                let nd_function = NDFunction {
                                    nd_function_type,
                                    column_type: col_relation.data_type.clone(),
                                };

                                let node = self.make_node(&nd_function);
                                match node {
                                    Ok(node) => {
                                        // Replace the specific node within the list.
                                        cloned_values.as_mut().set(i, node);
                                        changed = true;
                                    }
                                    Err(e) => {
                                        err.get_or_insert(e);
                                        break;
                                    }
                                }
                            }

                            Ok(None) => continue,
                            Err(e) => {
                                err.get_or_insert(e);
                                break;
                            }
                        }
                    }

                    // Replaces the entire VALUES list at once with the one we cloned and re-wrote.
                    if changed {
                        node.replace(cloned_values.uncast());

                        // Do not continue to traverse.
                        return None;
                    }

                    Some(node)
                }
                _ => Some(node),
            }),
        );

        err.map(Err).unwrap_or(Ok(()))
    }

    /// Iterates through Schema to find DEFAULT columns
    /// Adds the column to target list & all the values lists (ParamRef or String)
    pub(super) fn handle_adding_defaults(
        &mut self,
        mut stmt: &mut NodeMut<'mem, '_>,
        not_covered_cols: &Vec<String>,
        insert_context: &InsertContext<'mem>,
    ) -> Result<(), Error> {
        let NodeMut::InsertStmt(insert_stmt) = &mut stmt else {
            return Ok(());
        };

        for col in not_covered_cols {
            let col_relation = insert_context.relation.columns.get(col.as_str()).unwrap();

            let nd_function_type = NDFunctionType::from_func_call(
                &col_relation.column_default,
                None,
                &insert_context.rewrite_case,
            )?;

            let Some(nd_function_type) = nd_function_type else {
                continue;
            };

            let nd_function = NDFunction {
                nd_function_type,
                column_type: col_relation.data_type.clone(),
            };

            // Add to the list of cols in the INSERT.
            insert_stmt.cols_mut().push(
                self.mem,
                self.mem
                    .make_res_target(Some(col), self.mem.empty(), self.mem.none())
                    .uncast(),
            );

            let NodeMut::SelectStmt(select_stmt) = &mut insert_stmt.select_stmt_mut() else {
                return Ok(());
            };

            // Have to add the now() to every single select VALUES list now.
            // VALUES (...), (....)
            for values_list in select_stmt.values_lists_mut() {
                let mut node_list_mut = values_list.expect_node_list();

                node_list_mut.push(self.mem, self.make_node(&nd_function)?);
            }
        }

        Ok(())
    }
}
