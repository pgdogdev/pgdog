use crate::{net::Query, util::escape_identifier};

use super::ParameterValue;

pub(super) fn param_to_sql(name: &str, value: &ParameterValue, local: bool) -> Query {
    Query::new(format!(
        r#"{} "{}" TO {}"#,
        if local { "SET LOCAL" } else { "SET" },
        escape_identifier(name),
        value
    ))
}

pub(super) fn reset_to_sql(name: &str) -> Query {
    Query::new(format!(r#"RESET "{}""#, escape_identifier(name)))
}
