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
    /// The request executes a statement, rather than only preparing or binding it.
    pub(super) fn executes_statement(&self) -> bool {
        self.messages
            .iter()
            .any(|message| matches!(message.code(), 'E' | 'Q'))
    }
}
