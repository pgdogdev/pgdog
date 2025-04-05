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

/// Multi-shard state.
#[derive(Default, Debug)]
pub(super) struct MultiShard {
    /// Number of shards we are connected to.
    shards: usize,
    /// Route the query is taking.
    route: Route,
    /// How many rows we received so far.
    rows: usize,
    /// Number of ReadyForQuery messages.
    ready_for_query: usize,
    /// Number of CommandComplete messages.
    command_complete_count: usize,
    /// Number of NoData messages.
    empty_query_response: usize,
    /// Number of CopyInResponse messages.
    copy_in: usize,
    error_response: usize,
    parse_complete: usize,
    parameter_description: usize,
    no_data: usize,
    row_description: usize,
    /// Rewritten CommandComplete message.
    command_complete: Option<Message>,
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
            command_complete: None,
            ..Default::default()
        }
    }

    pub(super) fn new_reset(&self) -> Self {
        Self::new(self.shards, &self.route)
    }

    /// Check if the message should be sent to the client, skipped,
    /// or modified.
    pub(super) fn forward(&mut self, message: Message) -> Result<Option<Message>, super::Error> {
        let mut forward = None;

        match message.code() {
            'Z' => {
                self.ready_for_query += 1;
                forward = if self.ready_for_query == self.shards {
                    Some(message)
                } else {
                    None
                };
            }

            'C' => {
                let cc = CommandComplete::from_bytes(message.to_bytes()?)?;
                let has_rows = if let Some(rows) = cc.rows()? {
                    self.rows += rows;
                    true
                } else {
                    false
                };
                self.command_complete_count += 1;

                if self.command_complete_count == self.shards {
                    self.buffer.full();
                    self.buffer
                        .aggregate(self.route.aggregate(), &self.decoder)?;
                    self.buffer.sort(self.route.order_by(), &self.decoder);

                    if has_rows {
                        let rows = if self.route.should_buffer() {
                            self.buffer.len()
                        } else {
                            self.rows
                        };
                        self.command_complete = Some(cc.rewrite(rows)?.message()?);
                    } else {
                        forward = Some(cc.message()?);
                    }
                }
            }

            'T' => {
                self.row_description += 1;
                if self.row_description == 1 {
                    forward = Some(message);
                } else if self.row_description == self.shards {
                    let rd = RowDescription::from_bytes(message.to_bytes()?)?;
                    self.decoder.row_description(&rd);
                }
            }

            'I' => {
                self.empty_query_response += 1;
                if self.empty_query_response == self.shards {
                    forward = Some(message);
                }
            }

            'D' => {
                if !self.route.should_buffer() && self.row_description == self.shards {
                    forward = Some(message);
                } else {
                    self.buffer.add(message)?;
                }
            }

            'G' => {
                self.copy_in += 1;
                if self.copy_in == self.shards {
                    forward = Some(message);
                }
            }

            'n' => {
                self.no_data += 1;
                if self.no_data == self.shards {
                    forward = Some(message);
                }
            }

            '1' => {
                self.parse_complete += 1;
                if self.parse_complete == self.shards {
                    forward = Some(message);
                }
            }

            't' => {
                self.parameter_description += 1;
                if self.parameter_description == self.shards {
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
            self.command_complete.take()
        }
    }

    pub(super) fn set_context<'a>(&mut self, message: impl Into<Context<'a>>) {
        let context = message.into();
        match context {
            Context::Bind(bind) => {
                if self.decoder.rd().fields.is_empty() {
                    if !bind.anonymous() {
                        if let Some(rd) = PreparedStatements::global()
                            .lock()
                            .row_description(&bind.statement)
                        {
                            self.decoder.row_description(&rd);
                        }
                    }
                }
                self.decoder.bind(bind);
            }
            Context::RowDescription(rd) => self.decoder.row_description(rd),
        }
    }
}
