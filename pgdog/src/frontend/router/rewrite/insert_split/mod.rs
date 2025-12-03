use pg_query::{
    protobuf::{ParseResult, RawStmt},
    Node, NodeEnum,
};

use super::*;

#[derive(Default)]
pub struct InsertSplitRewrite;

impl RewriteModule for InsertSplitRewrite {
    fn rewrite(&mut self, input: &mut Context<'_>) -> Result<(), Error> {
        let mut inserts = vec![];
        if let Some(NodeEnum::InsertStmt(insert)) = input
            .stmt()?
            .stmt
            .as_ref()
            .and_then(|stmt| stmt.node.as_ref())
        {
            if let Some(NodeEnum::SelectStmt(ref select)) = insert
                .select_stmt
                .as_ref()
                .and_then(|node| node.node.as_ref())
            {
                if select.values_lists.len() <= 1 {
                    return Ok(());
                }

                // Clone the original statement only once.
                let mut proto_select = select.clone();
                let mut proto_insert = insert.clone();
                proto_select.values_lists.clear();
                proto_insert.select_stmt = None;

                // Generate new INSERT statements, with one VALUES tuple each.
                for values in &select.values_lists {
                    let mut new_insert = proto_insert.clone();
                    let mut new_select = proto_select.clone();
                    let mut new_values = values.clone();

                    // Rewrite the parameter references
                    // and create new Bind message for each INSERT statement.
                    if let Some(NodeEnum::List(list)) = new_values.node.as_mut() {
                        for value in list.items.iter_mut() {
                            if let Some(NodeEnum::ParamRef(_)) = value.node.as_mut() {
                                // let parameter = input
                                //     .bind()
                                //     .and_then(|bind| bind.parameter(param.number as usize - 1).ok())
                                //     .flatten();
                                // if let Some(parameter) = parameter {
                                //     param.number = new_bind.add_existing(parameter)?;
                                // }
                            }
                        }
                    }
                    new_select.values_lists.push(new_values);
                    new_insert.select_stmt = Some(Box::new(Node {
                        node: Some(NodeEnum::SelectStmt(new_select)),
                    }));
                    let result = ParseResult {
                        version: input.proto_version(),
                        stmts: vec![RawStmt {
                            stmt: Some(Box::new(Node {
                                node: Some(NodeEnum::InsertStmt(new_insert)),
                            })),
                            ..Default::default()
                        }],
                    };
                    inserts.push(result);
                }
            }
        }

        drop(inserts);

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::net::bind::Parameter;

    use super::*;

    #[test]
    fn test_insert_split() {
        let stmt = pg_query::parse(
            "INSERT INTO users (id, email, created_at)
            VALUES ($1, 'test@test.com', NOW()), (123, $2, '2025-01-01') RETURNING *",
        )
        .unwrap();
        let bind = Bind::new_params(
            "",
            &[
                Parameter {
                    len: 4,
                    data: "1234".into(),
                },
                Parameter {
                    len: 14,
                    data: "hello@test.com".into(),
                },
            ],
        );
        let mut context = Context::new(&stmt.protobuf, Some(&bind), None);
        let mut module = InsertSplitRewrite::default();
        module.rewrite(&mut context).unwrap();
    }
}
