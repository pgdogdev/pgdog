//! Multi-shard connection state.

use tracing::warn;

use crate::{
    frontend::router::Route,
    net::messages::{
        command_complete::CommandComplete, FromBytes, Message, Protocol, RowDescription, ToBytes,
    },
};

use super::sort_buffer::SortBuffer;

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
    /// First RowDescription we received from any shard.
    rd: Option<RowDescription>,
    /// Rewritten CommandComplete message.
    command_complete: Option<Message>,
    /// Sorting buffer.
    sort_buffer: SortBuffer,
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

    /// Check if the message should be sent to the client, skipped,
    /// or modified.
    pub(super) fn forward(&mut self, message: Message) -> Result<Option<Message>, super::Error> {
        let mut forward = None;
        let order_by = self.route.order_by();

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
                    self.sort_buffer.full();
                    if let Some(ref rd) = self.rd {
                        self.sort_buffer.sort(order_by, rd);
                    }

                    if has_rows {
                        self.command_complete = Some(cc.rewrite(self.rows)?.message()?);
                    } else {
                        forward = Some(cc.message()?);
                    }
                }
            }

            'T' => {
                let rd = RowDescription::from_bytes(message.to_bytes()?)?;
                if let Some(ref prev) = self.rd {
                    if !prev.equivalent(&rd) {
                        warn!("RowDescription across shards doesn't match");
                    }
                } else {
                    self.rd = Some(rd);
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
                if order_by.is_empty() {
                    forward = Some(message);
                } else {
                    self.sort_buffer.add(message)?;
                }
            }

            'G' => {
                self.ci += 1;
                if self.ci == self.shards {
                    forward = Some(message);
                }
            }

            _ => forward = Some(message),
        }

        Ok(forward)
    }

    /// Multi-shard state is ready to send messages.
    pub(super) fn message(&mut self) -> Option<Message> {
        if let Some(data_row) = self.sort_buffer.take() {
            Some(data_row)
        } else {
            self.command_complete.take()
        }
    }
}
