//! Handle client protocol state without
//! communicating with the backend.
mod responder;
pub(in crate::frontend) use responder::ProtocolResponder;

mod client_messages;
pub(in crate::frontend) use client_messages::ClientMessages;
