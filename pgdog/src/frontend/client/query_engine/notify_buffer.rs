use crate::frontend::router::parser::Shard;

#[derive(Debug)]
pub(crate) struct NotifyCommand {
    pub(crate) channel: String,
    pub(crate) payload: String,
    pub(crate) shard: Shard,
}

#[derive(Debug, Default)]
pub(crate) struct NotifyBuffer {
    commands: Vec<NotifyCommand>,
}

impl NotifyBuffer {
    pub(crate) fn add(&mut self, channel: String, payload: String, shard: Shard) {
        self.commands.push(NotifyCommand {
            channel,
            payload,
            shard,
        });
    }

    pub(crate) fn drain(&mut self) -> impl Iterator<Item = NotifyCommand> + '_ {
        self.commands.drain(..)
    }

    pub(crate) fn clear(&mut self) {
        self.commands.clear();
    }
}
