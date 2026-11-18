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

            // Why can't we send an array column (e.g., tags text[]) as an unnest(..) argument like the other columns?
            // Because that argument would be a 2D array (e.g., {{a,b},NULL,{c}}), but the inner arrays of 2D arrays are **required to be the same length**
            // Also, unnest(..) on 2D array returns the elements of the array flattened, not one row per inner array
            //
            // So, instead,
            // - we send the elements of all rows in a singular flat array
            // - for each row, we also send where the elements of the row start in the flat array (position), and how many elements the row has (length)
            // E.g., for rows (1, {a,b}), (2, NULL) and (3, {c}), we send the flat array {a,b,c} as a parameter, the positions {0,2,2} and the lengths {2,NULL,1} as unnest(..) arguments.
            //
            // The SELECT that we're building here then reads the rows of unnest(..) u.
            // For each row, the SELECT slices the flat array from position + 1 to position + length (we add 1, since Postgres arrays start at index 1):
            // {a,b,c}[0 + 1:0 + 2] = {a,b} for row 1, NULL for row 2 (slice with a NULL bound is NULL), and {a,b,c}[2 + 1:2 + 1] = {c} for row 3
            if let Some(element_oid) = DataType::array_element_oid(field.type_oid) {
                let array_type_name = DataType::array_type_name(field.type_oid)
                    .expect("array_element_oid only returns elements of built-in array types");

                let (flat, positions, lengths) = {
                    let mut flat = Vec::new();
                    let mut positions = Vec::with_capacity(data_rows.len());
                    let mut lengths = Vec::with_capacity(data_rows.len());

                    for row in data_rows {
                        positions.push(Some((flat.len() as i32).to_be_bytes()));

                        match row.get_raw(idx).filter(|value| !value.is_null) {
                            Some(value) => {
                                let elements = Array::binary_elements(value.data.as_ref()).expect(
                                    "evaluate_and_rewrite_outer_statement checked the arrays",
                                );
                                lengths.push(Some((elements.len() as i32).to_be_bytes()));
                                flat.extend(elements);
                            }
                            None => lengths.push(None),
                        }
                    }

                    (flat, positions, lengths)
                };

                let flat_param = push_array_param(old_bind, element_oid, flat.into_iter());

                let position_column = {
                    let position_column = format!("c{}", param_refs.len() + 1);
                    param_refs.push(push_unnest_argument(
                        mem,
                        old_bind,
                        INT4_OID,
                        "_int4",
                        positions
                            .iter()
                            .map(|position| position.as_ref().map(|bytes| bytes.as_slice())),
                    ));
                    unnest_column_names
                        .push(mem.make_string(Some(position_column.as_str())).uncast());
                    position_column
                };

                let length_column = {
                    let length_column = format!("c{}", param_refs.len() + 1);
                    param_refs.push(push_unnest_argument(
                        mem,
                        old_bind,
                        INT4_OID,
                        "_int4",
                        lengths
                            .iter()
                            .map(|length| length.as_ref().map(|bytes| bytes.as_slice())),
                    ));
                    unnest_column_names
                        .push(mem.make_string(Some(length_column.as_str())).uncast());
                    length_column
                };

                let slice_target = {
                    let sql = format!(
                        "SELECT (${flat_param}::pg_catalog.{array_type_name})[u.{position_column} + 1 : u.{position_column} + u.{length_column}]"
                    );
                    let parsed = pg_raw_parse::parse(&sql).expect("we build valid SQL");
                    let Some(Node::SelectStmt(select)) = parsed.stmts().next() else {
                        unreachable!("we parse a SELECT");
                    };
                    let slice = select
                        .target_list()
                        .into_iter()
                        .next()
                        .expect("the SELECT has one target")
                        .val();
                    mem.make_res_target(None, mem.empty(), mem.make_unique(slice))
                };

                select_targets.push(slice_target);
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

            // We build one array for each column that the subquery returned.
            // E.g., if SELECT id, status FROM users returns the rows (1, 'VIP'), (2, NULL) and (3, 'VIP'), we build
            // - {1,2,3} for id
            // - {VIP,NULL,VIP} for status
            let elements = data_rows.iter().map(|row| {
                row.get_raw(idx)
                    .filter(|value| !value.is_null)
                    .map(|value| value.data.as_ref())
            });

            param_refs.push(push_unnest_argument(
                mem,
                old_bind,
                field.type_oid,
                array_type_name,
                elements,
            ));
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

const INT4_OID: i32 = 23;

/// Adds the values `elements` to `bind` as an array parameter, and returns the number of that parameter
/// E.g., for bigint values 1, 2 and 3 (`element_oid` 20), we add the array {1,2,3} as $2, and return 2
fn push_array_param<'e>(
    bind: &mut Bind,
    element_oid: i32,
    elements: impl ExactSizeIterator<Item = Option<&'e [u8]>> + Clone,
) -> i32 {
    bind.push_param(
        Parameter::from_bytes(Array::binary_from_encoded_elements(element_oid, elements)),
        Format::Binary,
    );
    bind.params_raw().len() as i32
}

/// Adds the values `elements` to `bind` as an array parameter (with `push_array_param`), and returns the argument of unnest(..) that reads that parameter
/// E.g., for bigint values 1, 2 and 3, we add the array {1,2,3} as $2, and return $2::pg_catalog._int8
fn push_unnest_argument<'mem, 'e>(
    mem: MemoryToken<'mem>,
    bind: &mut Bind,
    element_oid: i32,
    array_type_name: &str,
    elements: impl ExactSizeIterator<Item = Option<&'e [u8]>> + Clone,
) -> Unique<'mem, Node<'mem>> {
    let param_ref = {
        let mut param_ref = mem.make_node::<nodes::ParamRef>();
        param_ref
            .as_mut()
            .set_number(push_array_param(bind, element_oid, elements));
        param_ref
    };

    mem.make_type_cast(
        param_ref.uncast(),
        mem.make_list(&[
            mem.make_string(Some("pg_catalog")),
            mem.make_string(Some(array_type_name)),
        ]),
    )
    .uncast()
}
