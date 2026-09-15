use super::*;

impl QueryParser {
    /// Handle SELECT set_config('key', 'value', is_local)
    ///
    /// If the function arguments are a form we cannot handle, we warn and
    /// pass through
    pub(super) fn set_config(
        &mut self,
        fcall: &nodes::FuncCall,
        context: &QueryParserContext,
    ) -> Command {
        if let Some(param) = parse_args(fcall) {
            Command::Set {
                params: vec![param],
                route: Route::write(context.shards_calculator.shard()),
                set_config: true,
            }
        } else {
            Command::Query(
                Route::write(context.shards_calculator.shard()).with_read(context.read_only),
            )
        }
    }
}

const ROLE_ESCAPE_PARAMS: [&str; 2] = ["role", "session_authorization"];

/// Canonical name of a role-changing variable, matched case-insensitively.
fn role_escape_param(name: &str) -> Option<&'static str> {
    ROLE_ESCAPE_PARAMS
        .iter()
        .copied()
        .find(|param| name.eq_ignore_ascii_case(param))
}

/// Name of the first role-changing variable (`role`, `session_authorization`)
/// a statement list sets or resets, via `SET`/`RESET` or `set_config(...)`.
pub(super) fn role_escape_target(stmts: &pg_raw_parse::StmtList) -> Option<String> {
    for node in stmts.stmts() {
        let name = match node {
            Node::VariableSetStmt(stmt) => stmt.name().and_then(role_escape_param),
            Node::SelectStmt(stmt) => extract_set_config(stmt)
                .and_then(|fcall| fcall.args().first())
                .and_then(parse_config_name)
                .as_deref()
                .and_then(role_escape_param),
            _ => None,
        };

        if let Some(name) = name {
            return Some(name.to_string());
        }
    }

    None
}

/// Returns None if the arguments could not be parsed
fn parse_args(fcall: &nodes::FuncCall) -> Option<SetParam> {
    let name = parse_config_name(fcall.args().first()?)?;
    let value = parse_config_value(fcall.args().get(1)?)?;
    let local = parse_is_local(fcall.args().get(2)?)?;
    Some(SetParam { name, value, local })
}

/// Returns None if the name could not be parsed
fn parse_config_name(arg: Node<'_>) -> Option<String> {
    match arg {
        Node::A_Const(c) => c.val()?.string_value().map(ToOwned::to_owned),
        // Only constant strings can be handled for now
        _ => None,
    }
}

/// Returns None if the value could not be parsed, Some(None) if the value
/// is NULL, and Some if the value was successfully parsed
fn parse_config_value(arg: Node<'_>) -> Option<Option<ParameterValue>> {
    match arg {
        Node::A_Const(c) => match c.val() {
            Some(value) => Some(Some(ParameterValue::String(
                value.string_value()?.to_owned(),
            ))),
            None => Some(None),
        },
        _ => None,
    }
}

/// Returns None if the node was not a constant boolean
fn parse_is_local(arg: Node<'_>) -> Option<bool> {
    match arg {
        Node::A_Const(c) => c.val()?.bool_value(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn role_escape(query: &str) -> Option<String> {
        let statements = pg_raw_parse::parse(query).expect("parse query");
        role_escape_target(&statements)
    }

    #[test]
    fn detects_role_escape_statements() {
        for (query, expected) in [
            ("SET ROLE reporting", "role"),
            ("SET LOCAL ROLE reporting", "role"),
            ("RESET ROLE", "role"),
            (
                "SET SESSION AUTHORIZATION reporting",
                "session_authorization",
            ),
            ("RESET SESSION AUTHORIZATION", "session_authorization"),
            ("SELECT 1; SET ROLE reporting", "role"),
            (
                "SELECT set_config('role', 'report' || 'ing', false)",
                "role",
            ),
            (
                "SELECT pg_catalog.set_config('ROLE', 'reporting', true)",
                "role",
            ),
            (
                "SELECT set_config('session_authorization', current_user, false)",
                "session_authorization",
            ),
        ] {
            assert_eq!(role_escape(query).as_deref(), Some(expected), "{query}");
        }
    }

    #[test]
    fn allows_session_reset_to_startup_defaults() {
        assert_eq!(role_escape("RESET ALL"), None);
        assert_eq!(role_escape("DISCARD ALL"), None);
        assert_eq!(role_escape("SET statement_timeout TO 1"), None);
        assert_eq!(
            role_escape("SELECT set_config('work_mem', '8MB', false)"),
            None
        );
    }
}
