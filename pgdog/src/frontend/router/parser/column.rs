//! Column name reference.

use pg_query::{Node, NodeEnum};

/// Column name extracted from a query.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Column<'a> {
    /// Column name.
    pub name: &'a str,
}

impl<'a> TryFrom<&'a Node> for Column<'a> {
    type Error = ();

    fn try_from(value: &'a Node) -> Result<Self, Self::Error> {
        match &value.node {
            Some(NodeEnum::ResTarget(res_target)) => {
                return Ok(Self {
                    name: res_target.name.as_str(),
                })
            }

            _ => (),
        }

        Err(())
    }
}

impl<'a> TryFrom<&Option<&'a Node>> for Column<'a> {
    type Error = ();

    fn try_from(value: &Option<&'a Node>) -> Result<Self, Self::Error> {
        if let Some(value) = value {
            (*value).try_into()
        } else {
            Err(())
        }
    }
}

#[cfg(test)]
mod test {
    use pg_query::{parse, NodeEnum};

    use super::Column;

    #[test]
    fn test_column() {
        let query = parse("INSERT INTO my_table (id, email) VALUES (1, 'test@test.com')").unwrap();
        let select = query.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        match select.node {
            Some(NodeEnum::InsertStmt(ref insert)) => {
                let columns = insert
                    .cols
                    .iter()
                    .map(|col| Column::try_from(col))
                    .collect::<Result<Vec<Column>, ()>>()
                    .unwrap();
                assert_eq!(
                    columns,
                    vec![Column { name: "id" }, Column { name: "email" }]
                );
            }

            _ => panic!("not a select"),
        }
    }
}
