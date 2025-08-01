pub mod authentication_cleartext_password;
pub mod authentication_gss;
pub mod authentication_gss_continue;
pub mod authentication_kerberos_v5;
pub mod authentication_md5_password;
pub mod authentication_ok;
pub mod authentication_sasl;
pub mod authentication_sasl_continue;
pub mod authentication_sasl_final;
pub mod authentication_scm_credential;
pub mod authentication_sspi;
pub mod backend_key_data;
pub mod bind_complete;
pub mod close_complete;
pub mod command_complete;
pub mod copy_both_response;
pub mod copy_data;
pub mod copy_done;
pub mod copy_in_response;
pub mod copy_out_response;
pub mod data_row;
pub mod empty_query_response;
pub mod error_response;
pub mod function_call_response;
pub mod negotiate_protocol_version;
pub mod no_data;
pub mod notice_response;
pub mod notification_response;
pub mod parameter_description;
pub mod parameter_status;
pub mod parse_complete;
pub mod portal_suspended;
pub mod ready_for_query;
pub mod row_description;

use crate::wire_protocol::backend::authentication_cleartext_password::AuthenticationCleartextPasswordFrame;
use crate::wire_protocol::backend::authentication_gss::AuthenticationGssFrame;
use crate::wire_protocol::backend::authentication_gss_continue::AuthenticationGssContinueFrame;
use crate::wire_protocol::backend::authentication_kerberos_v5::AuthenticationKerberosV5Frame;
use crate::wire_protocol::backend::authentication_md5_password::AuthenticationMd5PasswordFrame;
use crate::wire_protocol::backend::authentication_ok::AuthenticationOkFrame;
use crate::wire_protocol::backend::authentication_sasl::AuthenticationSaslFrame;
use crate::wire_protocol::backend::authentication_sasl_continue::AuthenticationSaslContinueFrame;
use crate::wire_protocol::backend::authentication_sasl_final::AuthenticationSaslFinalFrame;
use crate::wire_protocol::backend::authentication_scm_credential::AuthenticationScmCredentialFrame;
use crate::wire_protocol::backend::authentication_sspi::AuthenticationSspiFrame;
use crate::wire_protocol::backend::backend_key_data::BackendKeyDataFrame;
use crate::wire_protocol::backend::bind_complete::BindCompleteFrame;
use crate::wire_protocol::backend::close_complete::CloseCompleteFrame;
use crate::wire_protocol::backend::command_complete::CommandCompleteFrame;
use crate::wire_protocol::backend::copy_both_response::CopyBothResponseFrame;
use crate::wire_protocol::backend::copy_data::CopyDataFrame;
use crate::wire_protocol::backend::copy_done::CopyDoneFrame;
use crate::wire_protocol::backend::copy_in_response::CopyInResponseFrame;
use crate::wire_protocol::backend::copy_out_response::CopyOutResponseFrame;
use crate::wire_protocol::backend::data_row::DataRowFrame;
use crate::wire_protocol::backend::empty_query_response::EmptyQueryResponseFrame;
use crate::wire_protocol::backend::error_response::ErrorResponseFrame;
use crate::wire_protocol::backend::function_call_response::FunctionCallResponseFrame;
use crate::wire_protocol::backend::negotiate_protocol_version::NegotiateProtocolVersionFrame;
use crate::wire_protocol::backend::no_data::NoDataFrame;
use crate::wire_protocol::backend::notice_response::NoticeResponseFrame;
use crate::wire_protocol::backend::notification_response::NotificationResponseFrame;
use crate::wire_protocol::backend::parameter_description::ParameterDescriptionFrame;
use crate::wire_protocol::backend::parameter_status::ParameterStatusFrame;
use crate::wire_protocol::backend::parse_complete::ParseCompleteFrame;
use crate::wire_protocol::backend::portal_suspended::PortalSuspendedFrame;
use crate::wire_protocol::backend::ready_for_query::ReadyForQueryFrame;
use crate::wire_protocol::backend::row_description::RowDescriptionFrame;

/// Represents any backend-initiated protocol message.
/// Bidirectional protocol messages are also included.
#[derive(Debug)]
pub enum BackendProtocolMessage<'a> {
    /// AuthenticationCleartextPassword message
    AuthenticationCleartextPassword(AuthenticationCleartextPasswordFrame),

    /// AuthenticationGss message
    AuthenticationGss(AuthenticationGssFrame),

    /// AuthenticationGssContinue message
    AuthenticationGssContinue(AuthenticationGssContinueFrame<'a>),

    /// AuthenticationKerberosV5 message
    AuthenticationKerberosV5(AuthenticationKerberosV5Frame),

    /// AuthenticationMd5Password message
    AuthenticationMd5Password(AuthenticationMd5PasswordFrame),

    /// AuthenticationOk message
    AuthenticationOk(AuthenticationOkFrame),

    /// AuthenticationSasl message
    AuthenticationSasl(AuthenticationSaslFrame),

    /// AuthenticationSaslContinue message
    AuthenticationSaslContinue(AuthenticationSaslContinueFrame<'a>),

    /// AuthenticationSaslFinal message
    AuthenticationSaslFinal(AuthenticationSaslFinalFrame<'a>),

    /// AuthenticationScmCredential message
    AuthenticationScmCredential(AuthenticationScmCredentialFrame),

    /// AuthenticationSspi message
    AuthenticationSspi(AuthenticationSspiFrame),

    /// BackendKeyData message
    BackendKeyData(BackendKeyDataFrame),

    /// BindComplete message
    BindComplete(BindCompleteFrame),

    /// CloseComplete message
    CloseComplete(CloseCompleteFrame),

    /// CommandComplete message
    CommandComplete(CommandCompleteFrame<'a>),

    /// CopyBothResponse message
    CopyBothResponse(CopyBothResponseFrame),

    /// CopyData message for COPY operations
    CopyData(CopyDataFrame<'a>),

    /// CopyDone message for COPY operations
    CopyDone(CopyDoneFrame),

    /// CopyInResponse message
    CopyInResponse(CopyInResponseFrame),

    /// CopyOutResponse message
    CopyOutResponse(CopyOutResponseFrame),

    /// DataRow message
    DataRow(DataRowFrame<'a>),

    /// EmptyQueryResponse message
    EmptyQueryResponse(EmptyQueryResponseFrame),

    /// ErrorResponse message
    ErrorResponse(ErrorResponseFrame<'a>),

    /// FunctionCallResponse message
    FunctionCallResponse(FunctionCallResponseFrame<'a>),

    /// NegotiateProtocolVersion message
    NegotiateProtocolVersion(NegotiateProtocolVersionFrame<'a>),

    /// NoData message
    NoData(NoDataFrame),

    /// NoticeResponse message
    NoticeResponse(NoticeResponseFrame<'a>),

    /// NotificationResponse message
    NotificationResponse(NotificationResponseFrame<'a>),

    /// ParameterDescription message
    ParameterDescription(ParameterDescriptionFrame),

    /// ParameterStatus message
    ParameterStatus(ParameterStatusFrame<'a>),

    /// ParseComplete message
    ParseComplete(ParseCompleteFrame),

    /// PortalSuspended message
    PortalSuspended(PortalSuspendedFrame),

    /// ReadyForQuery message
    ReadyForQuery(ReadyForQueryFrame),

    /// RowDescription message
    RowDescription(RowDescriptionFrame<'a>),
}
