pub mod bind;
pub mod cancel_request;
pub mod close;
pub mod copy_data;
pub mod copy_done;
pub mod copy_fail;
pub mod describe_portal;
pub mod describe_statement;
pub mod execute;
pub mod flush;
pub mod function_call;
pub mod gss_response;
pub mod gssenc_request;
pub mod parse;
pub mod password_message;
pub mod query;
pub mod sasl_initial_response;
pub mod ssl_request;
pub mod sspi_response;
pub mod startup;
pub mod sync;
pub mod terminate;

use crate::wire_protocol::frontend::bind::BindFrame;
use crate::wire_protocol::frontend::cancel_request::CancelRequestFrame;
use crate::wire_protocol::frontend::close::CloseFrame;
use crate::wire_protocol::frontend::copy_data::CopyDataFrame;
use crate::wire_protocol::frontend::copy_done::CopyDoneFrame;
use crate::wire_protocol::frontend::copy_fail::CopyFailFrame;
use crate::wire_protocol::frontend::describe_portal::DescribePortalFrame;
use crate::wire_protocol::frontend::describe_statement::DescribeStatementFrame;
use crate::wire_protocol::frontend::execute::ExecuteFrame;
use crate::wire_protocol::frontend::flush::FlushFrame;
use crate::wire_protocol::frontend::function_call::FunctionCallFrame;
use crate::wire_protocol::frontend::gss_response::GssResponseFrame;
use crate::wire_protocol::frontend::gssenc_request::GssencRequestFrame;
use crate::wire_protocol::frontend::parse::ParseFrame;
use crate::wire_protocol::frontend::password_message::PasswordMessageFrame;
use crate::wire_protocol::frontend::query::QueryFrame;
use crate::wire_protocol::frontend::sasl_initial_response::SaslInitialResponseFrame;
use crate::wire_protocol::frontend::ssl_request::SslRequestFrame;
use crate::wire_protocol::frontend::sspi_response::SspiResponseFrame;
use crate::wire_protocol::frontend::startup::StartupFrame;
use crate::wire_protocol::frontend::sync::SyncFrame;
use crate::wire_protocol::frontend::terminate::TerminateFrame;

/// Represents any frontend-initiated protocol message.
/// Bidirectional protocol messages are also included.
#[derive(Debug)]
pub enum FrontendProtocolMessage<'a> {
    /// Extended-protocol Bind message
    Bind(BindFrame<'a>),

    /// CancelRequest message for canceling queries
    CancelRequest(CancelRequestFrame),

    /// Close message
    Close(CloseFrame<'a>),

    /// CopyData message for COPY operations
    CopyData(CopyDataFrame<'a>),

    /// CopyDone message for COPY operations
    CopyDone(CopyDoneFrame),

    /// CopyFail message for COPY operations
    CopyFail(CopyFailFrame<'a>),

    /// Extended-protocol DescribePortal message for describing portals
    DescribePortal(DescribePortalFrame<'a>),

    /// Extended-protocol DescribeStatement message for describing prepared statements
    DescribeStatement(DescribeStatementFrame<'a>),

    /// Extended-protocol Execute message for executing prepared statements
    Execute(ExecuteFrame<'a>),

    /// Flush message for flushing data
    Flush(FlushFrame),

    /// FunctionCall message for calling functions
    FunctionCall(FunctionCallFrame<'a>),

    /// GssResponse message for GSSAPI authentication
    GssResponse(GssResponseFrame<'a>),

    /// GssEncRequest message for GSSAPI encryption
    GssEncRequest(GssencRequestFrame),

    /// Parse message for parsing SQL statements
    Parse(ParseFrame<'a>),

    /// Password message for password authentication
    Password(PasswordMessageFrame<'a>),

    /// Query message for executing SQL queries
    Query(QueryFrame<'a>),

    /// SaslInitialResponse message for SASL authentication
    SaslInitialResponse(SaslInitialResponseFrame<'a>),

    /// SslRequest message for SSL negotiation
    SslRequest(SslRequestFrame),

    /// SspiResponse message for SSPI authentication
    SspiResponse(SspiResponseFrame<'a>),

    /// Startup message for initializing the connection
    Startup(StartupFrame<'a>),

    /// Sync message for synchronizing the connection
    Sync(SyncFrame),

    /// Terminate message signaling session end
    Terminate(TerminateFrame),
}
