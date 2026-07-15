use crate::frontend::router::parser::cache::Ast;
use pgdog_plugin::{
    Context as PdRouterContext, ReadWrite, Shard as PdShard,
    parameters::{Parameter, Parameters},
};
use std::string::String as StdString;

use super::*;

/// Output by one of the plugins.
#[derive(Default, Debug)]
pub(super) struct PluginOutput {
    pub(super) shard: Option<Shard>,
    pub(super) read: Option<bool>,
    pub(super) plugin_name: Option<StdString>,
}

impl PluginOutput {
    fn provided(&self) -> bool {
        self.shard.is_some() || self.read.is_some()
    }
}

impl QueryParser {
    /// Execute plugins, if any.
    pub(super) fn plugins(
        &mut self,
        context: &QueryParserContext,
        statement: &Ast,
        read: bool,
    ) -> Result<(), Error> {
        // Don't run plugins on Parse only.
        if context.router_context.bind.is_none() && statement.cached {
            return Ok(());
        }

        let plugins = if let Some(plugins) = plugins() {
            plugins
        } else {
            return Ok(());
        };

        if plugins.is_empty() {
            return Ok(());
        }

        // Run plugins, if any.
        // The first plugin to returns something, wins.
        debug!("executing {} router plugins", plugins.len());

        let params_data;
        let params = if let Some(bind) = context.router_context.bind {
            params_data = bind
                .params_raw()
                .iter()
                .map(|param| {
                    if param.len == -1 {
                        None
                    } else {
                        Some(&*param.data)
                    }
                    .into()
                })
                .collect::<Vec<Parameter<'_>>>();

            Parameters {
                parameters: &params_data,
                format_codes: bind.format_codes_raw(),
            }
        } else {
            Parameters::default()
        };
        let context = PdRouterContext {
            shards: context.shards as u64,
            has_replicas: !context.read_only,
            has_primary: !context.write_only,
            in_transaction: context.router_context.in_transaction(),
            write_override: self.write_override || !read, // This is set inside `QueryParser::plugins`.
            query: &statement.parse_result().protobuf,
            params,
        };

        for (plugin_name, plugin) in plugins {
            let route = plugin.route(context);
            match route.shard.try_into() {
                Ok(shard) => match shard {
                    PdShard::All => self.plugin_output.shard = Some(Shard::All),
                    PdShard::Direct(shard) => self.plugin_output.shard = Some(Shard::Direct(shard)),
                    PdShard::Unknown => self.plugin_output.shard = None,
                    PdShard::Blocked => {
                        return Err(Error::BlockedByPlugin(plugin_name.clone()));
                    }
                },
                Err(_) => self.plugin_output.shard = None,
            }

            match route.read_write.try_into() {
                Ok(ReadWrite::Read) => self.plugin_output.read = Some(true),
                Ok(ReadWrite::Write) => self.plugin_output.read = Some(false),
                _ => self.plugin_output.read = None,
            }

            self.plugin_output.plugin_name = Some(plugin_name.clone());

            if self.plugin_output.provided() {
                let shard_override = self.plugin_output.shard.clone();
                let read_override = self.plugin_output.read;
                if let Some(recorder) = self.recorder_mut() {
                    recorder.record_plugin_override(
                        plugin_name.clone(),
                        shard_override,
                        read_override,
                    );
                }
                debug!(
                    "plugin \"{}\" returned route [{}, {}]",
                    plugin_name,
                    match self.plugin_output.shard.as_ref() {
                        Some(shard) => format!("shard={}", shard),
                        None => "shard=unknown".to_string(),
                    },
                    match self.plugin_output.read {
                        Some(read) => format!("role={}", if read { "replica" } else { "primary" }),
                        None => "read=unknown".to_string(),
                    }
                );
                break;
            }
        }

        Ok(())
    }
}
