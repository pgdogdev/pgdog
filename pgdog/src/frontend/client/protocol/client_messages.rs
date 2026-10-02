use crate::net::{Protocol, ProtocolMessage};

#[derive(Copy, Clone)]
pub(in crate::frontend) struct ClientMessages<'a> {
    pub(super) messages: &'a [ProtocolMessage],
}

impl<'a> From<&'a [ProtocolMessage]> for ClientMessages<'a> {
    fn from(messages: &'a [ProtocolMessage]) -> Self {
        Self { messages }
    }
}

impl ClientMessages<'_> {
    /// Messages received from client will trigger a change
    /// on the server, so we should act accordingly.
    pub(super) fn actionable(&self) -> bool {
        self.messages
            .iter()
            .any(|message| matches!(message.code(), 'E' | 'Q'))
    }
}
