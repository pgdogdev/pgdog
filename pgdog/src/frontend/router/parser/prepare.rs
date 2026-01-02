use pg_query::protobuf::PrepareStmt;
use pgdog_config::QueryParserEngine;

use super::Error;

#[derive(Debug, Clone, PartialEq)]
pub struct Prepare {
    name: String,
    statement: String,
}

impl Prepare {
    pub fn from_stmt(
        value: &PrepareStmt,
        query_parser_engine: QueryParserEngine,
    ) -> Result<Self, Error> {
        let query = value.query.as_ref().ok_or(Error::EmptyQuery)?;
        let statement = match query_parser_engine {
            QueryParserEngine::PgQueryProtobuf => query.deparse(),
            QueryParserEngine::PgQueryRaw => query.deparse_raw(),
        }
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
                let prepare =
                    Prepare::from_stmt(stmt.as_ref(), QueryParserEngine::PgQueryProtobuf).unwrap();
                assert_eq!(prepare.name, "test");
                assert_eq!(prepare.statement, "SELECT $1, $2");
            }
            _ => panic!("Not a prepare"),
        }
    }
}
