use crate::frontend::router::parser::cache::CachedAst;
use pgdog_plugin::{ReadWrite, Shard as PdShard};

use super::*;

/// Output by one of the plugins.
#[derive(Default, Debug)]
pub(super) struct PluginOutput {
    pub(super) shard: Option<Shard>,
    pub(super) read: Option<bool>,
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
        statement: &CachedAst,
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

        let mut context =
            context.plugin_context(&statement.ast().protobuf, &context.router_context.bind);
        context.write_override = if self.write_override || !read { 1 } else { 0 };

        for plugin in plugins {
            if let Some(route) = plugin.route(context) {
                let route: pgdog_plugin::Route = route.into();
                if let Some(error) = route.get_error() {
                    return Err(Error::ErrorResponse(error.into()));
                }
                match route.shard.try_into() {
                    Ok(shard) => match shard {
                        PdShard::All => self.plugin_output.shard = Some(Shard::All),
                        PdShard::Direct(shard) => {
                            self.plugin_output.shard = Some(Shard::Direct(shard))
                        }
                        PdShard::Unknown => self.plugin_output.shard = None,
                        PdShard::Blocked => {
                            return Err(Error::BlockedByPlugin(plugin.name().to_owned()))
                        }
                    },
                    Err(_) => self.plugin_output.shard = None,
                }

                match route.read_write.try_into() {
                    Ok(ReadWrite::Read) => self.plugin_output.read = Some(true),
                    Ok(ReadWrite::Write) => self.plugin_output.read = Some(false),
                    _ => self.plugin_output.read = None,
                }

                if self.plugin_output.provided() {
                    debug!(
                        "plugin \"{}\" returned route [{}, {}]",
                        plugin.name(),
                        match self.plugin_output.shard.as_ref() {
                            Some(shard) => format!("shard={}", shard),
                            None => "shard=unknown".to_string(),
                        },
                        match self.plugin_output.read {
                            Some(read) =>
                                format!("role={}", if read { "replica" } else { "primary" }),
                            None => "read=unknown".to_string(),
                        }
                    );
                    break;
                }
            }
        }

        Ok(())
    }
}
