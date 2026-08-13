use crate::{
    net::{DataRow, Field, Protocol, RowDescription},
    util::pgdog_version,
};

use super::*;
use pg_raw_parse::Node;

pub(super) fn get_show_variable(sql: &str) -> Result<String, Error> {
    let stmt = pg_raw_parse::parse(sql).map_err(|_| Error::Syntax)?;
    let root = stmt.stmts().next().ok_or(Error::Syntax)?;

    if let Node::VariableShowStmt(stmt) = root {
        Ok(stmt.name().unwrap_or_default().to_owned())
    } else {
        Err(Error::Syntax)
    }
}

pub struct ShowGuc {
    pub(super) variable: String,
}

#[async_trait]
impl Command for ShowGuc {
    fn name(&self) -> String {
        "SHOW".into()
    }

    fn parse(_sql: &str) -> Result<Self, Error> {
        unreachable!("ShowGuc is initialized manually")
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let value = match self.variable.as_str() {
            "version" => pgdog_version(),
            "server_version" => pg_raw_parse::postgres_version().to_string(),
            _ => String::new(),
        };

        let rd = RowDescription::new(&[Field::text(&self.variable)]);
        let mut dr = DataRow::new();
        dr.add(&value);

        Ok(vec![rd.message()?, dr.message()?])
    }
}
