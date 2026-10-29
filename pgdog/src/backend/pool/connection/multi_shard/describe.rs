//! Describe portals whose rows we merge across shards.

use std::collections::VecDeque;

use crate::{
    frontend::ClientRequest,
    net::{Describe, ProtocolMessage},
};

/// Who asked for a [`crate::net::RowDescription`] or [`crate::net::NoData`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DescribedBy {
    Client,
    PgDog,
}

/// Describe every portal right before it's executed, unless the client just did,
/// so we decode its rows with the columns the shards return for them.
///
/// Returns the request to send instead, with who asked for each of its descriptions,
/// or [`None`] if the client describes every portal itself.
pub(super) fn describe_portals(
    request: &ClientRequest,
) -> Option<(ClientRequest, VecDeque<DescribedBy>)> {
    let undescribed = undescribed_portals(&request.messages).flatten().count();
    if undescribed == 0 {
        return None;
    }

    let mut messages = Vec::with_capacity(request.messages.len() + undescribed);
    let mut describes = VecDeque::new();

    for (message, portal) in request
        .messages
        .iter()
        .zip(undescribed_portals(&request.messages))
    {
        if let Some(portal) = portal {
            messages.push(ProtocolMessage::Describe(Describe::new_portal(portal)));
            describes.push_back(DescribedBy::PgDog);
        } else if matches!(message, ProtocolMessage::Describe(_)) {
            describes.push_back(DescribedBy::Client);
        }
        messages.push(message.clone());
    }

    Some((request.with_messages(messages), describes))
}

/// The portal each message executes without describing it first.
fn undescribed_portals(messages: &[ProtocolMessage]) -> impl Iterator<Item = Option<&str>> {
    let mut described = None;

    messages.iter().map(move |message| match message {
        ProtocolMessage::Describe(describe) => {
            described = describe.is_portal().then(|| describe.statement());
            None
        }
        ProtocolMessage::Bind(bind) if described == Some(bind.portal()) => {
            described = None;
            None
        }
        ProtocolMessage::Execute(execute) => {
            let portal = execute.portal();
            (described.take() != Some(portal)).then_some(portal)
        }
        _ => None,
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::net::{Bind, Execute, Parse, Protocol, Sync};

    fn summary(messages: &[ProtocolMessage]) -> Vec<String> {
        messages
            .iter()
            .map(|message| match message {
                ProtocolMessage::Describe(describe) => {
                    format!("D{}{}", describe.kind(), describe.statement())
                }
                ProtocolMessage::Execute(execute) => format!("E{}", execute.portal()),
                message => message.code().to_string(),
            })
            .collect()
    }

    #[test]
    fn test_describes_portals_the_client_did_not() {
        use DescribedBy::*;

        // lib/pq: Bind, Execute, Sync.
        let (sent, describes) = describe_portals(&ClientRequest::from(vec![
            Bind::new_statement("s1").into(),
            Execute::new().into(),
            Sync.into(),
        ]))
        .unwrap();
        assert_eq!(summary(&sent.messages), ["B", "DP", "E", "S"]);
        assert_eq!(describes, [PgDog]);

        // The statement's columns don't come with the result formats from Bind.
        let (sent, describes) = describe_portals(&ClientRequest::from(vec![
            Parse::named("s1", "SELECT 1").into(),
            Describe::new_statement("s1").into(),
            Bind::new_statement("s1").into(),
            Execute::new().into(),
            Sync.into(),
        ]))
        .unwrap();
        assert_eq!(summary(&sent.messages), ["P", "DSs1", "B", "DP", "E", "S"]);
        assert_eq!(describes, [Client, PgDog]);

        // Pipelined portals: only the second one needs describing.
        let (sent, describes) = describe_portals(&ClientRequest::from(vec![
            Bind::new_name_portal("s1", "p1").into(),
            Describe::new_portal("p1").into(),
            Execute::new_portal("p1").into(),
            Bind::new_name_portal("s2", "p2").into(),
            Execute::new_portal("p2").into(),
            Sync.into(),
        ]))
        .unwrap();
        assert_eq!(
            summary(&sent.messages),
            ["B", "DPp1", "Ep1", "B", "DPp2", "Ep2", "S"]
        );
        assert_eq!(describes, [Client, PgDog]);
    }

    #[test]
    fn test_client_described_portals_are_sent_as_is() {
        // JDBC, Npgsql: Bind, Describe portal, Execute, Sync.
        assert!(
            describe_portals(&ClientRequest::from(vec![
                Parse::named("s1", "SELECT 1").into(),
                Bind::new_statement("s1").into(),
                Describe::new_portal("").into(),
                Execute::new().into(),
                Sync.into(),
            ]))
            .is_none()
        );

        // Nothing executes.
        assert!(
            describe_portals(&ClientRequest::from(vec![
                Parse::named("s1", "SELECT 1").into(),
                Describe::new_statement("s1").into(),
                Sync.into(),
            ]))
            .is_none()
        );
    }
}
