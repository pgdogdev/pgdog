use crate::{
    backend::pool::Request,
    frontend::{
        buffer::BufferedQuery,
        router::{parser::Shard, Route},
        Command, Error, Router, RouterContext,
    },
    net::ErrorResponse,
    state::State,
};

use tracing::debug;

pub mod connect;
pub mod context;
pub mod deallocate;
pub mod end_transaction;
pub mod incomplete_requests;
pub mod pub_sub;
pub mod query;
pub mod route_query;
pub mod set;
pub mod show_shards;
pub mod start_transaction;
pub mod unknown_command;

pub use context::QueryEngineContext;

#[derive(Default)]
pub struct QueryEngine {
    begin_stmt: Option<BufferedQuery>,
    router: Router,
    streaming: bool,
}

impl<'a> QueryEngine {
    /// Handle client request.
    pub async fn handle(&mut self, context: &mut QueryEngineContext<'_>) -> Result<(), Error> {
        context.stats.received(context.buffer.total_message_len());

        // Intercept commands we don't have to forward to a server.
        if self.check_for_incomplete(context).await? {
            Self::update_stats(context);
            return Ok(());
        }

        // Route transaction to the right servers.
        if !self.route_transaction(context).await? {
            Self::update_stats(context);
            debug!("transaction has nowhere to go");
            return Ok(());
        }

        // Queue up request to mirrors, if any.
        // Do this before sending query to actual server
        // to have accurate timings between queries.
        context.backend.mirror(&context.buffer);

        let command = self.router.command();
        let route = command.route().clone();

        match command {
            Command::Shards(shards) => self.show_shards(context, *shards).await?,
            Command::StartTransaction(begin) => {
                self.start_transaction(context, begin.clone()).await?
            }
            Command::CommitTransaction => self.end_transaction(context, false).await?,
            Command::RollbackTransaction => self.end_transaction(context, true).await?,
            Command::Query(_) => self.query(context, &route).await?,
            Command::Listen { channel, shard } => {
                self.listen(context, &channel.clone(), shard.clone())
                    .await?
            }
            Command::Notify {
                channel,
                payload,
                shard,
            } => {
                self.notify(context, &channel.clone(), &payload.clone(), &shard.clone())
                    .await?
            }
            Command::Unlisten(channel) => self.unlisten(context, &channel.clone()).await?,
            Command::Set { name, value } => self.set(context, name.clone(), value.clone()).await?,
            Command::Copy(_) => self.query(context, &route).await?,
            Command::Rewrite(query) => {
                context.buffer.rewrite(query)?;
                self.query(context, &route).await?;
            }
            Command::Deallocate => self.deallocate(context).await?,
            command => self.unknown_command(context, command).await?,
        }

        if !context.in_transaction {
            if !context.backend.copy_mode() {
                self.router.reset();
            }

            context.backend.mirror_flush();
        }

        Self::update_stats(context);

        Ok(())
    }

    fn update_stats(context: &mut QueryEngineContext<'_>) {
        let state = match context.in_transaction {
            true => State::IdleInTransaction,
            false => State::Idle,
        };

        context.stats.state = state;

        context
            .stats
            .prepared_statements(context.prepared_statements.len_local());

        context.comms.stats(*context.stats);
    }
}
