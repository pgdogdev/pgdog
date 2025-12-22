use pgdog_config::RewriteMode;

use crate::{
    frontend::{
        client::query_engine::{QueryEngine, QueryEngineContext},
        router::parser::rewrite::statement::ShardingKeyUpdate,
        ClientRequest, Command, Router, RouterContext,
    },
    net::{DataRow, ErrorResponse, Protocol, RowDescription},
};

use super::{Error, UpdateError};

#[derive(Debug, Clone, Default)]
pub(super) struct Row {
    data_row: DataRow,
    row_description: RowDescription,
}

#[derive(Debug)]
pub(crate) struct UpdateMulti<'a> {
    pub(super) rewrite: ShardingKeyUpdate,
    pub(super) engine: &'a mut QueryEngine,
}

impl<'a> UpdateMulti<'a> {
    /// Create new sharding key update handler.
    pub(crate) fn new(engine: &'a mut QueryEngine, rewrite: ShardingKeyUpdate) -> Self {
        Self { rewrite, engine }
    }

    /// Execute sharding key update, if needed.
    pub(crate) async fn execute(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<(), Error> {
        let mut check = self.rewrite.check.build_request(&context.client_request)?;
        self.route(&mut check, context)?;

        // The new row is on the same shard as the old row
        // and we know this from the statement itself, e.g.
        //
        // UPDATE my_table SET shard_key = $1 WHERE shard_key = $2
        //
        // This is very likely if the number of shards is low or
        // you're using an ORM that puts all record columns
        // into the SET clause.
        //
        if self.is_same_shard(context)? {
            // Serve original request as-is.
            self.execute_original(context).await?;

            return Ok(());
        }

        if !self.engine.backend.is_multishard() {
            return Err(UpdateError::TransactionRequired.into());
        }

        // Fetch the old row from whatever shard it is on.
        let row = self.fetch_row(context).await?;

        if let Some(row) = row {
            self.insert_row(context, row).await?;
        } else {
            // This happens, but the UPDATE's WHERE clause
            // doesn't match any rows, so this whole thing is a no-op.
            self.engine
                .fake_command_response(context, "UPDATE 0")
                .await?;
        }

        Ok(())
    }

    pub(super) async fn insert_row(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        row: Row,
    ) -> Result<(), Error> {
        let mut request = self.rewrite.insert.build_request(
            &context.client_request,
            &row.row_description,
            &row.data_row,
        )?;
        self.route(&mut request, context)?;

        let original_shard = context.client_request.route().shard();
        let new_shard = request.route().shard();

        // The new row maps to the same shard as the old row.
        // We don't need to do the multi-step UPDATE anymore.
        // Forward the original request as-is.
        if original_shard.is_direct() && new_shard == original_shard {
            self.execute_original(context).await
        } else {
            // Check if we are allowed to do this operation by the config.
            if self.engine.backend.cluster()?.rewrite().shard_key == RewriteMode::Error {
                self.engine
                    .error_response(context, ErrorResponse::from_err(&UpdateError::Disabled))
                    .await?;
                return Ok(());
            }

            self.delete_row(context).await?;
            self.execute_internal(context, &mut request).await
        }
    }

    async fn execute_internal(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        request: &mut ClientRequest,
    ) -> Result<(), Error> {
        self.engine
            .backend
            .handle_client_request(request, &mut Router::default(), false)
            .await?;

        while self.engine.backend.has_more_messages() {
            let message = self.engine.read_server_message(context).await?;

            if message.code() == 'E' {
                return Err(Error::Execution(ErrorResponse::try_from(message)?));
            }
        }

        Ok(())
    }

    async fn execute_original(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<(), Error> {
        // Serve original request as-is.
        self.engine
            .backend
            .handle_client_request(
                &context.client_request,
                &mut self.engine.router,
                self.engine.streaming,
            )
            .await?;

        while self.engine.backend.has_more_messages() {
            let message = self.engine.read_server_message(context).await?;
            self.engine.process_server_message(context, message).await?;
        }

        Ok(())
    }

    pub(super) async fn delete_row(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<(), Error> {
        let mut request = self.rewrite.delete.build_request(&context.client_request)?;
        self.route(&mut request, context)?;

        self.execute_internal(context, &mut request).await
    }

    pub(super) async fn fetch_row(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<Option<Row>, Error> {
        let mut request = self.rewrite.select.build_request(&context.client_request)?;
        self.route(&mut request, context)?;

        self.engine
            .backend
            .handle_client_request(&mut request, &mut Router::default(), false)
            .await?;

        let mut row = Row::default();
        let mut rows = 0;

        while self.engine.backend.has_more_messages() {
            let message = self.engine.read_server_message(context).await?;
            match message.code() {
                'D' => {
                    row.data_row = DataRow::try_from(message)?;
                    rows += 1;
                }
                'T' => row.row_description = RowDescription::try_from(message)?,
                'E' => return Err(Error::Execution(ErrorResponse::try_from(message)?)),
                _ => (),
            }
        }

        match rows {
            0 => return Ok(None),
            1 => (),
            n => return Err(UpdateError::TooManyRows(n).into()),
        }

        Ok(Some(row))
    }

    /// Returns true if the new sharding key resides on the same shard
    /// as the old sharding key.
    ///
    /// This is an optimization to avoid doing a multi-shard UPDATE when
    /// we don't have to.
    pub(super) fn is_same_shard(&self, context: &QueryEngineContext<'_>) -> Result<bool, Error> {
        let mut check = self.rewrite.check.build_request(&context.client_request)?;
        self.route(&mut check, context)?;

        let new_shard = check.route().shard();
        let old_shard = context.client_request.route().shard();

        // The sharding key isn't actually being changed
        // or it maps to the same shard as before.
        Ok(new_shard == old_shard)
    }

    fn route(
        &self,
        request: &mut ClientRequest,
        context: &QueryEngineContext<'_>,
    ) -> Result<(), Error> {
        let cluster = self.engine.backend.cluster()?;

        let context = RouterContext::new(
            request,
            cluster,
            context.params,
            context.transaction(),
            context.sticky,
        )?;
        let mut router = Router::new();
        let command = router.query(context)?;
        if let Command::Query(route) = command {
            request.route = Some(route.clone());
        } else {
            return Err(UpdateError::NoRoute.into());
        }

        Ok(())
    }
}
