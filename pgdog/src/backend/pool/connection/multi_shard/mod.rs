//! Multi-shard connection state.

use std::collections::VecDeque;

use crate::{
    frontend::router::Route,
    net::{
        BackendPid, Bind, Decoder, ReadyForQuery,
        messages::{
            DataRow, FromBytes, Message, Protocol, RowDescription, ToBytes,
            command_complete::CommandComplete,
        },
    },
};

use super::buffer::Buffer;

mod error;
#[cfg(test)]
mod test;
mod validator;

pub use error::Error;
use validator::Validator;

#[derive(Default, Debug)]
struct RequestState {
    rows: usize,
    ready_for_query: usize,
    command_complete_count: usize,
    empty_query_response: usize,
    copy_in: usize,
    parse_complete: usize,
    parameter_description: usize,
    no_data: usize,
    row_description: usize,
    close_complete: usize,
    bind_complete: usize,
    command_complete: Option<Message>,
    transaction_error: bool,
    copy_done: usize,
    copy_out: usize,
    copy_data: usize,
    first_backend_data: Option<BackendPid>,
}

/// Multi-shard state.
#[derive(Default, Debug)]
pub struct MultiShard {
    /// Number of shards we are connected to.
    shards: usize,
    /// Route the query is taking.
    route: Route,
    /// Maps positional index in the servers vec to actual shard number.
    /// When all shards are connected, this is `[0, 1, 2, ...]`.
    /// When only a subset is connected (e.g. shards 0 and 2), this is `[0, 2]`.
    shard_indices: Vec<usize>,
    /// In-flight request state.
    request_state: RequestState,
    /// Sorting/aggregate buffer.
    buffer: Buffer,
    /// Row decoder.
    decoder: Decoder,
    /// Row consistency validator.
    validator: Validator,
    /// [`Bind`]s waiting for their [`crate::net::BindComplete`], in the order they were sent.
    bound_statements: VecDeque<Bind>,
}

impl MultiShard {
    /// New multi-shard state given the actual shard indices connected.
    pub(super) fn new(shard_indices: Vec<usize>, route: &Route) -> Self {
        let shards = shard_indices.len();
        Self {
            shards,
            shard_indices,
            route: route.clone(),
            ..Default::default()
        }
    }

    /// Map a positional index to the actual shard number.
    ///
    /// These can diverge since we can connect to less shards than there
    /// are in the config, while the query parser will produce shard numbers
    /// relative to all configured shards.
    ///
    pub(super) fn shard_number(&self, position: usize) -> usize {
        self.shard_indices
            .get(position)
            .copied()
            .unwrap_or(position)
    }

    /// Update multi-shard state.
    pub(super) fn update(&mut self, shards: usize, route: &Route) {
        self.reset();
        self.shards = shards;
        self.route = route.clone();
    }

    /// Update only the shards count without resetting message counters.
    ///
    /// Used for [`crate::net::Sync`]-only requests where we need correct [`ReadyForQuery`] counting
    /// but must preserve buffered [`CommandComplete`] from previous queries.
    ///
    pub(super) fn update_shards(&mut self, shards: usize) {
        self.shards = shards;
    }

    /// Query finished execution, reset any query state.
    pub(super) fn query_complete(&mut self) {
        self.reset();
        // Don't reset:
        //  1. Route to keep routing decision
        //  2. Number of shards
        //  3. Decoder
        //  4. Pending Binds, pushed before this run
    }

    #[inline]
    fn reset(&mut self) {
        self.request_state = RequestState::default();
        self.buffer = Buffer::default();
        self.validator.reset();

        // Intentionally does not reset:
        //  1. Route, to keep routing decision
        //  2. Number of shards
        //  3. Decoder state
        //  4. Pending Binds, pushed before this run
    }

    /// Check if the message we received from Postgres should be sent to the client, skipped,
    /// or modified.
    ///
    /// # Arguments
    ///
    /// * `message`: [`Message`] received from Postgres.
    ///
    /// # Returns
    ///
    /// [`Some`] if the message should be sent to the client, [`None`] if it should be dropped.
    ///
    pub(super) fn handle_server_message(
        &mut self,
        message: Message,
    ) -> Result<Option<Message>, Error> {
        Ok(match message.code() {
            'Z' => self.handle_ready_for_query(message)?,
            'C' => self.handle_command_complete(message)?,
            'T' => self.handle_row_description(message)?,
            'I' => self.handle_empty_query_response(message)?,
            'D' => self.handle_data_row(message)?,
            'G' => Self::handle_passthrough(message, &mut self.request_state.copy_in, self.shards),
            'n' => Self::handle_passthrough(message, &mut self.request_state.no_data, self.shards),
            '1' => Self::handle_passthrough(
                message,
                &mut self.request_state.parse_complete,
                self.shards,
            ),
            '3' => Self::handle_passthrough(
                message,
                &mut self.request_state.close_complete,
                self.shards,
            ),
            'c' => {
                Self::handle_passthrough(message, &mut self.request_state.copy_done, self.shards)
            }
            'H' => Self::handle_passthrough(message, &mut self.request_state.copy_out, self.shards),
            't' => Self::handle_passthrough(
                message,
                &mut self.request_state.parameter_description,
                self.shards,
            ),
            '2' => self.handle_bind(message),
            'd' => self.handle_copy_data(message),
            _ => Some(message),
        })
    }

    /// Get the next available server message to send to the client.
    ///
    /// Used when buffering and transforming messages. This returns [`None`] until
    /// the state is ready, i.e., we received everything we need to sort/limit/offset, etc.
    ///
    pub(super) fn get_server_message(&mut self) -> Option<Message> {
        match self.buffer.take() {
            Some(data_row) => Some(data_row),
            _ => self.request_state.command_complete.take(),
        }
    }

    /// The state has more messages for the client.
    pub(super) fn has_more_messages(&self) -> bool {
        self.buffer.can_take() || self.request_state.command_complete.is_some()
    }

    /// Push [`Bind`] message into decoding queue. This is used with multi-[`Bind`] extended
    /// protocol requests to make sure we process them in the right order and with correct decoding.
    pub(super) fn push_bind(&mut self, bind: &Bind) {
        self.bound_statements.push_back(bind.clone());
    }

    fn handle_ready_for_query(&mut self, message: Message) -> Result<Option<Message>, Error> {
        self.request_state.ready_for_query += 1;

        if message.transaction_error() {
            self.request_state.transaction_error = true;
        }

        Ok(
            if self
                .request_state
                .ready_for_query
                .is_multiple_of(self.shards)
            {
                self.bound_statements.clear();

                if self.request_state.transaction_error {
                    Some(ReadyForQuery::error().message())
                } else {
                    Some(message)
                }
            } else {
                None
            },
        )
    }

    // Count [`CommandComplete`] messages.
    //
    // Once all shards finished executing the command,
    // we can start aggregating and sorting.
    fn handle_command_complete(&mut self, message: Message) -> Result<Option<Message>, Error> {
        let cc = CommandComplete::from_bytes(message.to_bytes())?;

        // DDL commands don't return rows.
        let has_rows = if let Some(rows) = cc.rows()? {
            if self.route.is_omnisharded() {
                // Only use the first shard's row count for consistency with DataRow.
                if self.request_state.command_complete_count == 0 {
                    self.request_state.rows = rows;
                }
            } else {
                self.request_state.rows += rows;
            }
            true
        } else {
            false
        };

        self.request_state.command_complete_count += 1;

        if self
            .request_state
            .command_complete_count
            .is_multiple_of(self.shards)
        {
            self.buffer.mark_full();

            if !self.buffer.is_empty() {
                self.buffer
                    .aggregate(
                        self.route.aggregate(),
                        &self.decoder,
                        self.route.aggregate_rewrite_plan(),
                    )
                    .map_err(Error::from)?;

                self.buffer.sort(self.route.order_by(), &self.decoder);
                self.buffer.distinct(self.route.distinct(), &self.decoder);
                self.buffer.limit(self.route.limit());
            }

            if has_rows {
                let rows = if self.should_buffer() {
                    self.buffer.len()
                } else {
                    self.request_state.rows
                };
                self.request_state.command_complete = Some(cc.rewrite(rows)?.message());
            } else {
                return Ok(Some(cc.message()));
            }
        }

        Ok(None)
    }

    fn handle_row_description(&mut self, message: Message) -> Result<Option<Message>, Error> {
        let mut forward = None;

        self.request_state.row_description += 1;
        let rd = RowDescription::from_bytes(message.to_bytes())?;

        // Validate row description consistency inside the group.
        // we reset after shards processed the group of the same RD
        let is_first_in_group = self.validator.validate_row_description(&rd)?;

        // Set it as soon as we have it, so the aggregator and sorter
        // can use it.
        if is_first_in_group {
            self.decoder.set_row_description(rd.clone());
        }

        if self
            .request_state
            .row_description
            .is_multiple_of(self.shards)
        {
            // Only send it to the client once all shards sent it,
            // so we don't get early requests from clients.
            let plan = self.route.aggregate_rewrite_plan();
            if plan.is_noop() {
                forward = Some(message);
            } else {
                let client_rd = rd.drop_columns(plan.drop_columns());
                forward = Some(client_rd.message());
            }

            // The next statement describes a different result set.
            self.validator.reset();
        }

        Ok(forward)
    }

    fn handle_empty_query_response(&mut self, message: Message) -> Result<Option<Message>, Error> {
        self.request_state.empty_query_response += 1;

        Ok(
            if self
                .request_state
                .empty_query_response
                .is_multiple_of(self.shards)
            {
                Some(message)
            } else {
                None
            },
        )
    }

    fn handle_data_row(&mut self, message: Message) -> Result<Option<Message>, Error> {
        let mut forward = None;

        if self.shards > 1 {
            // Validate data row consistency.
            let data_row = DataRow::from_bytes(message.to_bytes())?;
            self.validator.validate_data_row(&data_row)?;
        }

        // INVARIANT: omni dedup relies on Source::Backend carrying a
        // process-unique BackendPid (see BackendPid::seq). Never tag messages
        // with a non-unique value; doing so silently corrupts row deduplication.
        if self.request_state.first_backend_data.is_none() {
            self.request_state.first_backend_data = message.source().backend_id();
        }

        if !self.should_buffer()
            && self
                .request_state
                .row_description
                .is_multiple_of(self.shards)
        {
            if self.route.is_omnisharded() {
                if self.request_state.first_backend_data == message.source().backend_id() {
                    forward = Some(message);
                }
            } else {
                forward = Some(message);
            }
        } else {
            self.buffer.add(message).map_err(Error::from)?;
        }

        Ok(forward)
    }

    fn handle_passthrough(message: Message, counter: &mut usize, shards: usize) -> Option<Message> {
        *counter += 1;

        if counter.is_multiple_of(shards) {
            Some(message)
        } else {
            None
        }
    }

    fn handle_bind(&mut self, message: Message) -> Option<Message> {
        let mut forward = None;
        self.request_state.bind_complete += 1;

        if self.request_state.bind_complete.is_multiple_of(self.shards) {
            forward = Some(message);

            if let Some(bind) = self.bound_statements.pop_front() {
                self.decoder.set_formats(&bind);
            }
        }

        forward
    }

    fn handle_copy_data(&mut self, message: Message) -> Option<Message> {
        self.request_state.copy_data += 1;
        Some(message)
    }

    /// Return true if we need to buffer [`DataRow`] messages
    /// received from the servers because we need to post-process them.
    fn should_buffer(&self) -> bool {
        // 1. We are talking to more than one shard (cross-shard query)
        // 2. The route contains transformations we need to perform, e.g., aggregates, sorting, etc.
        // 3. The route does not concern omnisharded tables which have the same data on all shards
        //    anyway.
        self.shards > 1 && self.route.requires_post_processing() && !self.route.is_omnisharded()
    }
}
