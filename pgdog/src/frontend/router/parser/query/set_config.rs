use super::*;
use crate::net::messages::Format;

impl QueryParser {
    /// Handle SELECT set_config('key', 'value', is_local)
    ///
    /// Arguments we can't resolve leave it an ordinary query: it runs, but
    /// we don't learn what it changed.
    pub(super) fn set_config(
        &mut self,
        fcall: &nodes::FuncCall,
        context: &QueryParserContext,
    ) -> Command {
        if let Some(param) = parse_args(fcall, context.router_context.bind) {
            Command::Set {
                params: vec![param],
                route: Route::write(context.shards_calculator.shard()),
                is_select: true,
            }
        } else {
            Command::Query(
                Route::write(context.shards_calculator.shard()).with_read(context.read_only),
            )
        }
    }
}

/// Returns None if the arguments could not be parsed
fn parse_args(
    fcall: &nodes::FuncCall,
    params: Option<StatementParameters<'_>>,
) -> Option<SetParam> {
    let name = parse_config_name(fcall.args().first()?, params)?;
    let value = parse_config_value(fcall.args().get(1)?, params)?;
    let local = parse_is_local(fcall.args().get(2)?, params)?;
    Some(SetParam { name, value, local })
}

/// Value bound to `$number`; the inner Option is the SQL NULL.
fn bound_text(params: Option<StatementParameters<'_>>, number: i32) -> Option<Option<String>> {
    let index = usize::try_from(number).ok()?.checked_sub(1)?;
    let param = params?.parameter(index).ok()??;

    if param.is_null() {
        Some(None)
    } else {
        Some(Some(param.text()?.to_owned()))
    }
}

fn bound_bool(params: Option<StatementParameters<'_>>, number: i32) -> Option<bool> {
    let index = usize::try_from(number).ok()?.checked_sub(1)?;
    let param = params?.parameter(index).ok()??;

    if param.is_null() {
        return None;
    }

    match param.format() {
        Format::Binary => match param.data() {
            [0] => Some(false),
            [1] => Some(true),
            _ => None,
        },
        Format::Text => match param.text()?.trim().to_lowercase().as_str() {
            "t" | "true" | "y" | "yes" | "on" | "1" => Some(true),
            "f" | "false" | "n" | "no" | "off" | "0" => Some(false),
            _ => None,
        },
    }
}

/// Returns None if the name could not be parsed
fn parse_config_name(arg: Node<'_>, params: Option<StatementParameters<'_>>) -> Option<String> {
    match arg {
        Node::A_Const(c) => c.val()?.string_value().map(ToOwned::to_owned),
        Node::ParamRef(nodes::ParamRef { number, .. }) => bound_text(params, *number)?,
        _ => None,
    }
}

/// Returns None if the value could not be parsed, Some(None) if the value
/// is NULL, and Some if the value was successfully parsed
fn parse_config_value(
    arg: Node<'_>,
    params: Option<StatementParameters<'_>>,
) -> Option<Option<ParameterValue>> {
    match arg {
        Node::A_Const(c) => match c.val() {
            Some(value) => Some(Some(ParameterValue::String(
                value.string_value()?.to_owned(),
            ))),
            None => Some(None),
        },
        Node::ParamRef(nodes::ParamRef { number, .. }) => {
            Some(bound_text(params, *number)?.map(ParameterValue::String))
        }
        _ => None,
    }
}

/// Returns None if the node was not a constant boolean
fn parse_is_local(arg: Node<'_>, params: Option<StatementParameters<'_>>) -> Option<bool> {
    match arg {
        Node::A_Const(c) => c.val()?.bool_value(),
        Node::ParamRef(nodes::ParamRef { number, .. }) => bound_bool(params, *number),
        _ => None,
    }
}
