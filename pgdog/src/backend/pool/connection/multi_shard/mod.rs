//! Multi-shard connection state.

use context::Context;

use crate::{
    frontend::router::Route,
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
    rfq: usize,
    /// Number of CommandComplete messages.
    cc: usize,
    /// Number of NoData messages.
    nd: usize,
    /// Number of CopyInResponse messages.
    ci: usize,
    er: usize,
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
                self.rfq += 1;
                forward = if self.rfq == self.shards {
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
                self.cc += 1;

                if self.cc == self.shards {
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
                let rd = RowDescription::from_bytes(message.to_bytes()?)?;
                if self.decoder.rd().is_empty() {
                    self.decoder.row_description(&rd);
                    forward = Some(message);
                }
            }

            'I' => {
                self.nd += 1;
                if self.nd == self.shards {
                    forward = Some(message);
                }
            }

            'D' => {
                if !self.route.should_buffer() {
                    forward = Some(message);
                } else {
                    self.buffer.add(message)?;
                }
            }

            'G' => {
                self.ci += 1;
                if self.ci == self.shards {
                    forward = Some(message);
                }
            }

            'n' => {
                self.er += 1;
                if self.er == self.shards {
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
            Context::Bind(bind) => self.decoder.bind(bind),
            Context::RowDescription(rd) => self.decoder.row_description(rd),
        }
    }
}
