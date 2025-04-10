use super::Error;
use pg_query::protobuf::PrepareStmt;

#[derive(Debug, Clone, PartialEq)]
pub struct Prepare {
    name: String,
    statement: String,
}

impl TryFrom<&PrepareStmt> for Prepare {
    type Error = super::Error;

    fn try_from(value: &PrepareStmt) -> Result<Self, Self::Error> {
        let statement = value
            .query
            .as_ref()
            .ok_or(Error::EmptyQuery)?
            .deparse()
            .map_err(|_| Error::EmptyQuery)?;

        Ok(Self {
            name: value.name.to_string(),
            statement,
        })
    }
}

#[cfg(test)]
mod test {
    use pg_query::{parse, NodeEnum};

    use super::*;

    #[test]
    fn test_prepare() {
        let ast = parse("PREPARE test AS SELECT $1, $2")
            .unwrap()
            .protobuf
            .stmts
            .first()
            .unwrap()
            .stmt
            .clone()
            .unwrap();
        match ast.node.unwrap() {
            NodeEnum::PrepareStmt(stmt) => {
                let prepare = Prepare::try_from(stmt.as_ref()).unwrap();
                assert_eq!(prepare.name, "test");
                assert_eq!(prepare.statement, "SELECT $1, $2");
            }
            _ => panic!("Not a prepare"),
        }
    }
}
