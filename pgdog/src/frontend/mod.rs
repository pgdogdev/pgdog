//! pgDog frontend manages connections to clients.

pub(crate) mod buffered_query;
pub(crate) mod client;
pub(crate) mod client_request;
pub(crate) mod comms;
pub(crate) mod connected_client;
pub(crate) mod error;
pub(crate) mod listener;
pub(crate) mod prepared_statements;
pub(crate) mod regex_parser;
pub(crate) mod router;
pub(crate) mod stats;

pub(crate) use buffered_query::BufferedQuery;
pub(crate) use client::Client;
pub(crate) use client_request::ClientRequest;
pub(crate) use comms::ClientComms;
pub(crate) use connected_client::ConnectedClient;
pub(crate) use error::Error;
pub(crate) use prepared_statements::PreparedStatements;
pub(crate) use regex_parser::RegexParser;
pub(crate) use router::{Command, DiscardTarget, RewritePlan, Router, SetParam};
pub(crate) use router::{RouterContext, SearchPath};
pub(crate) use stats::Stats;
