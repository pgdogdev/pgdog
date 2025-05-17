//! Multi-shard connection state.

use context::Context;

use crate::{
    frontend::{router::Route, PreparedStatements},
    net::{
        messages::{
            command_complete::CommandComplete, FromBytes, Message, Protocol, RowDescription,
            ToBytes,
        },
        Decoder,
    },
};

use super::buffer::Buffer;

mod context;
#[cfg(test)]
mod test;

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
}

/// Multi-shard state.
#[derive(Default, Debug)]
pub(super) struct MultiShard {
    /// Number of shards we are connected to.
    shards: usize,
    /// Route the query is taking.
    route: Route,

    /// Counters
    counters: Counters,

    /// Sorting/aggregate buffer.
    buffer: Buffer,
    decoder: Decoder,
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

    pub(super) fn reset(&mut self) {
        self.counters = Counters::default();
        self.buffer.reset();
        // Don't reset:
        //  1. Route to keep routing decision
        //  2. Number of shards
        //  3. Decoder
    }

    /// Check if the message should be sent to the client, skipped,
    /// or modified.
    pub(super) fn forward(&mut self, message: Message) -> Result<Option<Message>, super::Error> {
        let mut forward = None;

        match message.code() {
            'Z' => {
                self.counters.ready_for_query += 1;
                forward = if self.counters.ready_for_query % self.shards == 0 {
                    Some(message)
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
                    self.counters.rows += rows;
                    true
                } else {
                    false
                };
                self.counters.command_complete_count += 1;

                if self.counters.command_complete_count % self.shards == 0 {
                    self.buffer.full();
                    self.buffer
                        .aggregate(self.route.aggregate(), &self.decoder)?;
                    self.buffer.sort(self.route.order_by(), &self.decoder);

                    if has_rows {
                        let rows = if self.route.should_buffer() {
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
                // Set row description info as soon as we have it,
                // so it's available to the aggregator and sorter.
                if self.counters.row_description == 1 {
                    let rd = RowDescription::from_bytes(message.to_bytes()?)?;
                    self.decoder.row_description(&rd);
                }
                if self.counters.row_description == self.shards {
                    // Only send it to the client once all shards sent it,
                    // so we don't get early requests from clients.
                    forward = Some(message);
                }
            }

            'I' => {
                self.counters.empty_query_response += 1;
                if self.counters.empty_query_response % self.shards == 0 {
                    forward = Some(message);
                }
            }

            'D' => {
                if !self.route.should_buffer() && self.counters.row_description % self.shards == 0 {
                    forward = Some(message);
                } else {
                    self.buffer.add(message)?;
                }
            }

            'G' => {
                self.counters.copy_in += 1;
                if self.counters.copy_in % self.shards == 0 {
                    forward = Some(message);
                }
            }

            'n' => {
                self.counters.no_data += 1;
                if self.counters.no_data % self.shards == 0 {
                    forward = Some(message);
                }
            }

            '1' => {
                self.counters.parse_complete += 1;
                if self.counters.parse_complete % self.shards == 0 {
                    forward = Some(message);
                }
            }

            '3' => {
                self.counters.close_complete += 1;
                if self.counters.close_complete % self.shards == 0 {
                    forward = Some(message);
                }
            }

            '2' => {
                self.counters.bind_complete += 1;

                if self.counters.bind_complete % self.shards == 0 {
                    forward = Some(message);
                }
            }

            't' => {
                self.counters.parameter_description += 1;
                if self.counters.parameter_description % self.shards == 0 {
                    forward = Some(message);
                }
            }

            _ => forward = Some(message),
        }

        Ok(forward)
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
                        .lock()
                        .row_description(bind.statement())
                    {
                        self.decoder.row_description(&rd);
                    }
                }
                self.decoder.bind(bind);
            }
            Context::RowDescription(rd) => self.decoder.row_description(rd),
        }
    }
}
