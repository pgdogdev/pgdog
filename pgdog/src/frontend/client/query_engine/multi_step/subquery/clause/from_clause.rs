use std::ops::Deref;

use pg_raw_parse::{
    Node, NodeMut,
    make::{MemoryToken, Unique},
    nodes::{self, Alias, RangeFunction, SelectStmt},
    transform,
};
use pgdog_postgres_types::{Array, DataType, Format};

use crate::{
    frontend::client::query_engine::multi_step::SubqueryMulti,
    frontend::router::parser::rewrite::statement::insert::is_constant,
    net::{Bind, DataRow, RowDescription, bind::Parameter},
};

impl<'a> SubqueryMulti<'a> {
    pub fn handle_from_clause<'mem>(
        &self,
        mem: MemoryToken<'mem>,
        mut statement: Unique<'mem, Node<'mem>>,
        row_description: Option<&RowDescription>,
        data_rows: &[DataRow],
        new_bind: &mut Bind,
    ) -> Unique<'mem, Node<'mem>> {
        let Some(row_description) = row_description else {
            return statement;
        };

        let new_from_clause = mem.make_unique(match statement.as_ref() {
            Node::SelectStmt(stmt) => stmt.from_clause(),
            Node::UpdateStmt(stmt) => stmt.from_clause(),
            _ => unreachable!("The parser only allows through SELECT and UPDATE"),
        });

        let mut new_from_clause = mem.make_unique(new_from_clause.first().unwrap());
        transform::transform(&mut new_from_clause, |node| match &*node {
            NodeMut::RangeSubselect(range_subselect) => {
                let mut range_subselect = mem.make_unique(range_subselect.deref());
                // Replace the subquery in the statement with the DataRows (via VALUES)
                {
                    let new_subselect = Self::new_select_with_unnest(
                        new_bind,
                        mem,
                        range_subselect.subquery(),
                        data_rows,
                        row_description,
                    );

                    range_subselect
                        .as_mut()
                        .set_subquery(new_subselect.uncast());
                }

                {
                    let existing_alias = range_subselect
                        .alias()
                        .expect("alias was checked in the parser");

                    if existing_alias.colnames().is_empty() {
                        // If no explicit columns are provided in the alias, then we added them
                        // to correctly resolve against alias columns referenced in the outer query.

                        let new_alias =
                            Self::make_alias(mem, existing_alias, row_description.clone());

                        range_subselect.as_mut().set_alias(new_alias.as_option());
                    }
                }

                node.replace(range_subselect.uncast());
                None
            }
            _ => Some(node),
        });

        let new_from_clause = mem.make_list(&[new_from_clause]);

        match statement.as_mut() {
            NodeMut::SelectStmt(mut stmt) => stmt.set_from_clause(new_from_clause),
            NodeMut::UpdateStmt(mut stmt) => stmt.set_from_clause(new_from_clause),
            _ => unreachable!("The parser only allows through SELECT and UPDATE"),
        };

        if data_rows.is_empty() {
            // If rows are empty, now set WHERE to false to cancel out the NULL row we created
            // with `explicit_values_list` to properly return type OIDs in the RowDescription
            // when we have an empty derived table response.

            match statement.as_mut() {
                NodeMut::SelectStmt(mut stmt) => stmt.set_where_clause(
                    mem.make_a_const(pg_raw_parse::ConstValue::Boolean(false))
                        .uncast(),
                ),
                NodeMut::UpdateStmt(mut stmt) => stmt.set_where_clause(
                    mem.make_a_const(pg_raw_parse::ConstValue::Boolean(false))
                        .uncast(),
                ),
                _ => unreachable!("The parser only allows through SELECT and UPDATE"),
            }
        }

        statement
    }

    pub(crate) fn new_select_with_unnest<'mem>(
        old_bind: &mut Bind,
        mem: MemoryToken<'mem>,
        original_ast: Node<'_>,
        data_rows: &[DataRow],
        row_description: &RowDescription,
    ) -> Unique<'mem, &'mem SelectStmt> {
        let base_param_len = old_bind.params_raw().len() as i32;

        let original_targets = match original_ast {
            Node::SelectStmt(original_select)
                if original_select.target_list().len() == row_description.fields.len() =>
            {
                original_select
                    .target_list()
                    .into_iter()
                    .map(|target| Some(target.val()))
                    .collect()
            }
            _ => vec![None; row_description.fields.len()],
        };

        // Form the bind params
        let mut param_refs = Vec::new();
        let mut select_targets = Vec::new();
        let mut unnest_column_names = Vec::new();
        for (idx, field) in row_description.fields.iter().enumerate() {
            if let Some(constant) = original_targets[idx].filter(|target| is_constant(*target)) {
                select_targets.push(mem.make_res_target(
                    None,
                    mem.empty(),
                    mem.make_unique(constant),
                ));
                continue;
            }

            let unnest_column_name = format!("c{}", param_refs.len() + 1);
            select_targets.push(
                mem.make_res_target(
                    None,
                    mem.empty(),
                    mem.make_column_ref(mem.make_list(&[
                        mem.make_string(Some("u")).uncast(),
                        mem.make_string(Some(unnest_column_name.as_str())).uncast(),
                    ]))
                    .uncast(),
                ),
            );

            unnest_column_names.push(mem.make_string(Some(unnest_column_name.as_str())).uncast());

            let array_type_name = DataType::array_type_oid(field.type_oid)
                .and_then(DataType::array_type_name)
                .expect("evaluate_and_rewrite_outer_statement checked the column types");

            let node = {
                let mut node = mem.make_node::<nodes::ParamRef>();

                node.as_mut()
                    .set_number(base_param_len + param_refs.len() as i32 + 1);

                node
            };

            // We build one array for each column that the subquery returned.
            // E.g., if SELECT id, status FROM users returns the rows (1, 'VIP'), (2, NULL) and (3, 'VIP'), we build
            // - {1,2,3} for id
            // - {VIP,NULL,VIP} for status
            let elements = data_rows.iter().map(|row| {
                row.get_raw(idx)
                    .filter(|value| !value.is_null)
                    .map(|value| value.data.as_ref())
            });

            let array = Array::binary_from_encoded_elements(field.type_oid, elements);

            old_bind.push_param(Parameter::from_bytes(array), Format::Binary);

            let node = mem
                .make_type_cast(
                    node.uncast(),
                    mem.make_list(&[
                        mem.make_string(Some("pg_catalog")),
                        mem.make_string(Some(array_type_name)),
                    ]),
                )
                .uncast();

            param_refs.push(node);
        }

        let list_of_param_refs = mem.make_list(param_refs.as_slice());
        let unnest_func_call = mem.make_func_call(
            mem.make_list(&[mem.make_string(Some("unnest")).uncast()]),
            list_of_param_refs,
            0,
        );

        let unnest_alias = {
            let mut unnest_alias = mem.make_node::<Alias>();

            unnest_alias
                .as_mut()
                .set_aliasname(Some(mem.copy_string("u")));

            unnest_alias
                .as_mut()
                .set_colnames(mem.make_list(unnest_column_names.as_slice()));

            unnest_alias
        };

        let unnest_func_wrapper = {
            let mut unnest_func_wrapper = mem.make_node::<RangeFunction>();

            unnest_func_wrapper
                .as_mut()
                .set_functions(mem.make_list(&[mem.make_list(&[unnest_func_call.uncast()])]));

            unnest_func_wrapper
                .as_mut()
                .set_alias(unnest_alias.as_option());

            unnest_func_wrapper
        };

        {
            let mut new_select = mem.make_node::<SelectStmt>();

            new_select
                .as_mut()
                .set_from_clause(mem.make_list(&[unnest_func_wrapper.uncast()]));

            new_select
                .as_mut()
                .set_target_list(mem.make_list(select_targets.as_slice()));

            new_select
        }
    }

    /// Assemble a new alias to be used within the `RangeSubselect` node we're re-writing,
    /// to include the colnames if they're not already present. This allows for explicit
    /// <ALIAS>.<col> use within the outer-query (whom references the `RangeSubselect` results)
    pub(crate) fn make_alias<'mem>(
        mem: MemoryToken<'mem>,
        existing_alias: &Alias,
        row_description: RowDescription,
    ) -> Unique<'mem, &'mem Alias> {
        let mut new_alias = mem.make_unique(existing_alias);

        let mut col_name_list = Vec::new();
        for i in 0..row_description.len() {
            col_name_list.push(
                mem.make_string(Some(row_description.fields.get(i).unwrap().name.as_str()))
                    .uncast(),
            );
        }

        new_alias
            .as_mut()
            .set_colnames(mem.make_list(col_name_list.as_ref()));

        new_alias
    }
}
