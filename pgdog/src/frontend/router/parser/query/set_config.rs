use super::{String as PgString, *};
use std::string::String;

impl QueryParser {
    /// Handle SELECT set_config('key', 'value', is_local)
    ///
    /// If the function arguments are a form we cannot handle, we warn and
    /// pass through
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

/// Returns None if the arguments could not be parsed
fn parse_args(fcall: &FuncCall) -> Option<SetParam> {
    let name = parse_config_name(fcall.args.first()?)?;
    let value = parse_config_value(fcall.args.get(1)?)?;
    let local = parse_is_local(fcall.args.get(2)?)?;
    Some(SetParam { name, value, local })
}

/// Returns None if the name could not be parsed
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

/// Returns None if the node was not a constant boolean
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
