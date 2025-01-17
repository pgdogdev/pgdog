//! Multi-shard connection state.

use tracing::warn;

use crate::net::messages::{
    command_complete::CommandComplete, FromBytes, Message, Protocol, RowDescription, ToBytes,
};

/// Multi-shard state.
#[derive(Default)]
pub(super) struct MultiShard {
    shards: usize,
    rows: usize,
    rfq: usize,
    cc: usize,
    rd: Option<RowDescription>,
}

impl MultiShard {
    /// New multi-shard state given the number of shards in the cluster.
    pub(super) fn new(shards: usize) -> Self {
        Self {
            shards,
            ..Default::default()
        }
    }

    /// Check if the message should be sent to the client, skipped,
    /// or modified.
    pub(super) fn forward(&mut self, message: Message) -> Result<Option<Message>, super::Error> {
        let mut forward = None;

        match message.code() {
            'Z' => {
                self.rfq += 1;
                forward = match self.rfq == self.shards {
                    true => Some(message),
                    false => None,
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
                    if has_rows {
                        forward = Some(cc.rewrite(self.rows)?.message()?);
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

            _ => forward = Some(message),
        }

        Ok(forward)
    }
}
