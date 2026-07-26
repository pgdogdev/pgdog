#[cfg(not(feature = "new_parser"))]
use super::String as PgString;
use super::*;
#[cfg(feature = "new_parser")]
use pg_raw_parse::{Owned, StmtList};
#[cfg(not(feature = "new_parser"))]
use std::string::String;

impl QueryParser {
    /// Handle SELECT set_config('key', 'value', is_local)
    ///
    /// If the function arguments are a form we cannot handle, we warn and
    /// pass through
    #[cfg(feature = "new_parser")]
    pub(super) fn set_config(
        &mut self,
        fcall: &nodes::FuncCall,
        context: &QueryParserContext,
    ) -> Command {
        if let Some(param) = parse_args(fcall) {
            Command::Set {
                params: vec![param],
                route: Route::write(context.shards_calculator.shard()),
                behave_like_select: true,
            }
        } else {
            Command::Query(
                Route::write(context.shards_calculator.shard()).with_read(context.read_only),
            )
        }
    }

    cfg_select! {
        not(feature = "new_parser") => {
            pub(super) fn set_config(&mut self, fcall: &FuncCall, context: &QueryParserContext) -> Command {
                if let Some(param) = parse_args(fcall) {
                    Command::Set {
                        params: vec![param],
                        route: Route::write(context.shards_calculator.shard()),
                        behave_like_select: true,
                    }
                } else {
                    Command::Query(
                        Route::write(context.shards_calculator.shard()).with_read(context.read_only),
                    )
                }
            }
        }
        _ => {}
    }
}

/// Session variables whose modification lets a client escape a `server_role`
/// impersonation.
const ROLE_ESCAPE_PARAMS: [&str; 2] = ["role", "session_authorization"];

fn is_role_escape(name: &str) -> bool {
    ROLE_ESCAPE_PARAMS
        .iter()
        .any(|param| name.eq_ignore_ascii_case(param))
}

/// Find a client-issued attempt to change the backend role in any of the
/// parsed statements, so impersonation pools can reject every form uniformly:
/// `SET ROLE`, `SET SESSION ROLE`, `SET LOCAL ROLE`, `RESET ROLE`,
/// `SET SESSION AUTHORIZATION`, `RESET SESSION AUTHORIZATION`, and
/// `set_config('role'|'session_authorization', ...)` (including non-constant
/// values). Returns the offending variable name, matched case-insensitively.
///
/// `RESET ALL` / `DISCARD ALL` are intentionally not matched: they restore
/// session defaults (the startup-parameter role) rather than clearing it.
#[cfg(feature = "new_parser")]
pub(super) fn role_escape_target(stmts: &Owned<StmtList>) -> Option<String> {
    for node in stmts.stmts() {
        match node {
            // SET ROLE / RESET ROLE / SET SESSION AUTHORIZATION, and their
            // SESSION/LOCAL spellings, all land here with the same `name`.
            Node::VariableSetStmt(stmt) => {
                if let Some(name) = stmt.name()
                    && is_role_escape(name)
                {
                    return Some(name.to_string());
                }
            }
            // SELECT set_config('role', <anything>, ...) — including a
            // non-constant value that would otherwise pass through verbatim.
            Node::SelectStmt(stmt) => {
                if let Some(fcall) = extract_set_config(stmt)
                    && let Some(name) = fcall.args().first().and_then(Node::as_str)
                    && is_role_escape(name)
                {
                    return Some(name.to_string());
                }
            }
            _ => (),
        }
    }

    None
}

/// See the `new_parser` variant above for what this matches and why.
#[cfg(not(feature = "new_parser"))]
pub(super) fn role_escape_target(stmts: &[RawStmt]) -> Option<String> {
    for raw in stmts {
        match raw.stmt.as_ref().and_then(|stmt| stmt.node.as_ref()) {
            Some(NodeEnum::VariableSetStmt(stmt)) if is_role_escape(&stmt.name) => {
                return Some(stmt.name.clone());
            }
            Some(NodeEnum::SelectStmt(stmt)) => {
                if let Some(fcall) = extract_set_config(stmt)
                    && let Some(name_arg) = fcall.args.first()
                    && let Some(name) = parse_config_name(name_arg)
                    && is_role_escape(&name)
                {
                    return Some(name);
                }
            }
            _ => (),
        }
    }

    None
}

/// Returns None if the arguments could not be parsed
#[cfg(feature = "new_parser")]
fn parse_args(fcall: &nodes::FuncCall) -> Option<SetParam> {
    let name = parse_config_name(fcall.args().first()?)?;
    let value = parse_config_value(fcall.args().get(1)?)?;
    let local = parse_is_local(fcall.args().get(2)?)?;
    Some(SetParam { name, value, local })
}

cfg_select! {
    not(feature = "new_parser") => {
        fn parse_args(fcall: &FuncCall) -> Option<SetParam> {
            let name = parse_config_name(fcall.args.first()?)?;
            let value = parse_config_value(fcall.args.get(1)?)?;
            let local = parse_is_local(fcall.args.get(2)?)?;
            Some(SetParam { name, value, local })
        }
    }
    _ => {}
}

/// Returns None if the name could not be parsed
#[cfg(feature = "new_parser")]
fn parse_config_name(arg: Node<'_>) -> Option<String> {
    match arg {
        Node::A_Const(c) => c.val()?.string_value().map(ToOwned::to_owned),
        // Only constant strings can be handled for now
        _ => None,
    }
}

/// Returns None if the name could not be parsed
#[cfg(not(feature = "new_parser"))]
fn parse_config_name(arg: &PgNode) -> Option<String> {
    match &arg.node {
        Some(NodeEnum::AConst(AConst {
            val: Some(Val::Sval(PgString { sval })),
            ..
        })) => Some(sval.clone()),
        // Only constant strings can be handled for now
        _ => None,
    }
}

/// Returns None if the value could not be parsed, Some(None) if the value
/// is NULL, and Some if the value was successfully parsed
#[cfg(feature = "new_parser")]
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

cfg_select! {
    not(feature = "new_parser") => {
        fn parse_config_value(arg: &PgNode) -> Option<Option<ParameterValue>> {
            match &arg.node {
                Some(NodeEnum::AConst(AConst {
                    val: Some(Val::Sval(PgString { sval })),
                    ..
                })) => Some(Some(ParameterValue::String(sval.clone()))),
                Some(NodeEnum::AConst(AConst { isnull: true, .. })) => Some(None),
                // FIXME(sage): The function only takes text. Do we need to deal with
                // other literals?
                _ => None,
            }
        }
    }
    _ => {}
}

/// Returns None if the node was not a constant boolean
#[cfg(feature = "new_parser")]
fn parse_is_local(arg: Node<'_>) -> Option<bool> {
    match arg {
        Node::A_Const(c) => c.val()?.bool_value(),
        _ => None,
    }
}

cfg_select! {
    not(feature = "new_parser") => {
        fn parse_is_local(arg: &PgNode) -> Option<bool> {
            match &arg.node {
                Some(NodeEnum::AConst(AConst {
                    val: Some(Val::Boolval(Boolean { boolval })),
                    ..
                })) => Some(*boolval),
                // Only constant strings can be handled for now
                _ => None,
            }
        }
    }
    _ => {}
}
