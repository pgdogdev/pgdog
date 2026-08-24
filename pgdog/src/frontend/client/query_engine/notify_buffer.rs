use crate::frontend::router::parser::Shard;

#[derive(Debug)]
pub struct NotifyCommand {
    pub channel: String,
    pub payload: String,
    pub shard: Shard,
}

#[derive(Debug, Default)]
pub struct NotifyBuffer {
    commands: Vec<NotifyCommand>,
}

impl NotifyBuffer {
    pub fn add(&mut self, channel: String, payload: String, shard: Shard) {
        self.commands.push(NotifyCommand {
            channel,
            payload,
            shard,
        });
    }

    pub fn drain(&mut self) -> impl Iterator<Item = NotifyCommand> + '_ {
        self.commands.drain(..)
    }

    pub fn clear(&mut self) {
        self.commands.clear();
    }
}
