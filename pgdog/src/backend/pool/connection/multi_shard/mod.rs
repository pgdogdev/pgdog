//! Multi-shard connection state.

use context::Context;

use crate::{
    frontend::{router::Route, PreparedStatements},
    net::{
        messages::{
            command_complete::CommandComplete, DataRow, FromBytes, Message, Protocol,
            RowDescription, ToBytes,
        },
        BackendKeyData, Decoder, ReadyForQuery,
    },
};

use super::buffer::Buffer;

mod context;
mod error;
#[cfg(test)]
mod test;
mod validator;

pub use error::Error;
use validator::Validator;

#[derive(Default, Debug)]
struct Counters {
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
    first_backend_data: Option<BackendKeyData>,
}

/// Multi-shard state.
#[derive(Default, Debug)]
pub struct MultiShard {
    /// Number of shards we are connected to.
    shards: usize,
    /// Route the query is taking.
    route: Route,

    /// Counters
    counters: Counters,

    /// Sorting/aggregate buffer.
    buffer: Buffer,
    decoder: Decoder,
    /// Row consistency validator.
    validator: Validator,
}

impl MultiShard {
    /// New multi-shard state given the number of shards in the cluster.
    pub(super) fn new(shards: usize, route: &Route) -> Self {
        Self {
            shards,
            route: route.clone(),
            counters: Counters::default(),
            ..Default::default()
        }
    }

    /// Update multi-shard state.
    pub(super) fn update(&mut self, shards: usize, route: &Route) {
        self.reset();
        self.shards = shards;
        self.route = route.clone();
    }

    pub(super) fn reset(&mut self) {
        self.counters = Counters::default();
        self.buffer.reset();
        self.validator.reset();
        // Don't reset:
        //  1. Route to keep routing decision
        //  2. Number of shards
        //  3. Decoder
    }

    /// Check if the message should be sent to the client, skipped,
    /// or modified.
    pub(super) fn forward(&mut self, message: Message) -> Result<Option<Message>, Error> {
        let mut forward = None;

        match message.code() {
            'Z' => {
                self.counters.ready_for_query += 1;
                if message.transaction_error() {
                    self.counters.transaction_error = true;
                }

                forward = if self.counters.ready_for_query.is_multiple_of(self.shards) {
                    if self.counters.transaction_error {
                        Some(ReadyForQuery::error().message()?)
                    } else {
                        Some(message)
                    }
                } else {
                    None
                };
            }

            // Count CommandComplete messages.
            //
            // Once all shards finished executing the command,
            // we can start aggregating and sorting.
            'C' => {
                let cc = CommandComplete::from_bytes(message.to_bytes()?)?;
                let has_rows = if let Some(rows) = cc.rows()? {
                    if self.route.is_omni() {
                        // Only use the first shard's row count for consistency with DataRow.
                        if self.counters.command_complete_count == 0 {
                            self.counters.rows = rows;
                        }
                    } else {
                        self.counters.rows += rows;
                    }
                    true
                } else {
                    false
                };
                self.counters.command_complete_count += 1;

                if self
                    .counters
                    .command_complete_count
                    .is_multiple_of(self.shards)
                {
                    self.buffer.full();

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
                        self.buffer.limit(&self.route.limit());
                    }

                    if has_rows {
                        let rows = if self.should_buffer() {
                            self.buffer.len()
                        } else {
                            self.counters.rows
                        };
                        self.counters.command_complete = Some(cc.rewrite(rows)?.message()?);
                    } else {
                        forward = Some(cc.message()?);
                    }
                }
            }

            'T' => {
                self.counters.row_description += 1;
                let rd = RowDescription::from_bytes(message.to_bytes()?)?;

                // Validate row description consistency
                let is_first = self.validator.validate_row_description(&rd)?;

                // Set row description info as soon as we have it,
                // so it's available to the aggregator and sorter.
                if is_first {
                    self.decoder.row_description(&rd);
                }

                if self.counters.row_description == self.shards {
                    // Only send it to the client once all shards sent it,
                    // so we don't get early requests from clients.
                    let plan = self.route.aggregate_rewrite_plan();
                    if plan.drop_columns().is_empty() {
                        forward = Some(message);
                    } else {
                        let client_rd = rd.drop_columns(plan.drop_columns());
                        forward = Some(client_rd.message()?);
                    }
                }
            }

            'I' => {
                self.counters.empty_query_response += 1;
                if self
                    .counters
                    .empty_query_response
                    .is_multiple_of(self.shards)
                {
                    forward = Some(message);
                }
            }

            'D' => {
                if self.shards > 1 {
                    // Validate data row consistency.
                    let data_row = DataRow::from_bytes(message.to_bytes()?)?;
                    self.validator.validate_data_row(&data_row)?;
                }

                if self.counters.first_backend_data.is_none() {
                    self.counters.first_backend_data = message.source().backend_id();
                }

                if !self.should_buffer()
                    && self.counters.row_description.is_multiple_of(self.shards)
                {
                    if self.route.is_omni() {
                        if self.counters.first_backend_data == message.source().backend_id() {
                            forward = Some(message);
                        }
                    } else {
                        forward = Some(message);
                    }
                } else {
                    self.buffer.add(message).map_err(Error::from)?;
                }
            }

            'G' => {
                self.counters.copy_in += 1;
                if self.counters.copy_in.is_multiple_of(self.shards) {
                    forward = Some(message);
                }
            }

            'n' => {
                self.counters.no_data += 1;
                if self.counters.no_data.is_multiple_of(self.shards) {
                    forward = Some(message);
                }
            }

            '1' => {
                self.counters.parse_complete += 1;
                if self.counters.parse_complete.is_multiple_of(self.shards) {
                    forward = Some(message);
                }
            }

            '3' => {
                self.counters.close_complete += 1;
                if self.counters.close_complete.is_multiple_of(self.shards) {
                    forward = Some(message);
                }
            }

            '2' => {
                self.counters.bind_complete += 1;

                if self.counters.bind_complete.is_multiple_of(self.shards) {
                    forward = Some(message);
                }
            }

            'c' => {
                self.counters.copy_done += 1;
                if self.counters.copy_done.is_multiple_of(self.shards) {
                    forward = Some(message);
                }
            }

            'd' => {
                self.counters.copy_data += 1;
                forward = Some(message);
            }

            'H' => {
                self.counters.copy_out += 1;
                if self.counters.copy_out.is_multiple_of(self.shards) {
                    forward = Some(message);
                }
            }

            't' => {
                self.counters.parameter_description += 1;
                if self
                    .counters
                    .parameter_description
                    .is_multiple_of(self.shards)
                {
                    forward = Some(message);
                }
            }

            _ => forward = Some(message),
        }

        Ok(forward)
    }

    fn should_buffer(&self) -> bool {
        self.shards > 1 && self.route.should_buffer() && !self.route.is_omni()
    }

    /// Multi-shard state is ready to send messages.
    pub(super) fn message(&mut self) -> Option<Message> {
        if let Some(data_row) = self.buffer.take() {
            Some(data_row)
        } else {
            self.counters.command_complete.take()
        }
    }

    pub(super) fn set_context<'a>(&mut self, message: impl Into<Context<'a>>) {
        let context = message.into();
        match context {
            Context::Bind(bind) => {
                if self.decoder.rd().fields.is_empty() && !bind.anonymous() {
                    if let Some(rd) = PreparedStatements::global()
                        .read()
                        .row_description(bind.statement())
                    {
                        self.decoder.row_description(&rd);
                        self.validator.set_row_description(&rd);
                    }
                }
                self.decoder.bind(bind);
            }
            Context::RowDescription(rd) => {
                self.decoder.row_description(rd);
                self.validator.set_row_description(rd);
            }
        }
    }
}
