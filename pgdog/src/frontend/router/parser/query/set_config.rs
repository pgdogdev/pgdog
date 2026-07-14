#[cfg(not(feature = "new_parser"))]
use super::String as PgString;
use super::*;
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

/// Returns None if the arguments could not be parsed
#[cfg(feature = "new_parser")]
fn parse_args(fcall: &nodes::FuncCall) -> Option<SetParam> {
    let name = fcall.args().first().and_then(Node::as_str)?.to_owned();
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
        Node::A_Const(c) => Some(Some(ParameterValue::String(
            c.val()?.string_value()?.to_owned(),
        ))),
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
