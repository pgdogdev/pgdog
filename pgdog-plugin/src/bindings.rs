/* automatically generated by rust-bindgen 0.71.1 */

#[doc = " Query parameter value."]
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct Parameter {
    pub len: ::std::os::raw::c_int,
    pub data: *const ::std::os::raw::c_char,
    pub format: ::std::os::raw::c_int,
}
#[allow(clippy::unnecessary_operation, clippy::identity_op)]
const _: () = {
    ["Size of Parameter"][::std::mem::size_of::<Parameter>() - 24usize];
    ["Alignment of Parameter"][::std::mem::align_of::<Parameter>() - 8usize];
    ["Offset of field: Parameter::len"][::std::mem::offset_of!(Parameter, len) - 0usize];
    ["Offset of field: Parameter::data"][::std::mem::offset_of!(Parameter, data) - 8usize];
    ["Offset of field: Parameter::format"][::std::mem::offset_of!(Parameter, format) - 16usize];
};
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct Query {
    pub len: ::std::os::raw::c_int,
    pub query: *const ::std::os::raw::c_char,
    pub num_parameters: ::std::os::raw::c_int,
    pub parameters: *const Parameter,
}
#[allow(clippy::unnecessary_operation, clippy::identity_op)]
const _: () = {
    ["Size of Query"][::std::mem::size_of::<Query>() - 32usize];
    ["Alignment of Query"][::std::mem::align_of::<Query>() - 8usize];
    ["Offset of field: Query::len"][::std::mem::offset_of!(Query, len) - 0usize];
    ["Offset of field: Query::query"][::std::mem::offset_of!(Query, query) - 8usize];
    ["Offset of field: Query::num_parameters"]
        [::std::mem::offset_of!(Query, num_parameters) - 16usize];
    ["Offset of field: Query::parameters"][::std::mem::offset_of!(Query, parameters) - 24usize];
};
pub const Affinity_READ: Affinity = 1;
pub const Affinity_WRITE: Affinity = 2;
pub const Affinity_TRANSACTION_START: Affinity = 3;
pub const Affinity_TRANSACTION_END: Affinity = 4;
pub const Affinity_UNKNOWN: Affinity = -1;
#[doc = " The query is a read or a write.\n In case the plugin isn't able to figure it out, it can return UNKNOWN and\n pgDog will ignore the plugin's decision."]
pub type Affinity = ::std::os::raw::c_int;
pub const Shard_ANY: Shard = -1;
pub const Shard_ALL: Shard = -2;
#[doc = " In case the plugin doesn't know which shard to route the\n the query, it can decide to route it to any shard or to all\n shards. All shard queries return a result assembled by pgDog."]
pub type Shard = ::std::os::raw::c_int;
#[doc = " Route the query should take."]
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct Route {
    pub affinity: Affinity,
    pub shard: ::std::os::raw::c_int,
}
#[allow(clippy::unnecessary_operation, clippy::identity_op)]
const _: () = {
    ["Size of Route"][::std::mem::size_of::<Route>() - 8usize];
    ["Alignment of Route"][::std::mem::align_of::<Route>() - 4usize];
    ["Offset of field: Route::affinity"][::std::mem::offset_of!(Route, affinity) - 0usize];
    ["Offset of field: Route::shard"][::std::mem::offset_of!(Route, shard) - 4usize];
};
pub const RoutingDecision_FORWARD: RoutingDecision = 1;
pub const RoutingDecision_REWRITE: RoutingDecision = 2;
pub const RoutingDecision_ERROR: RoutingDecision = 3;
pub const RoutingDecision_INTERCEPT: RoutingDecision = 4;
pub const RoutingDecision_NO_DECISION: RoutingDecision = 5;
pub type RoutingDecision = ::std::os::raw::c_uint;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct Error {
    pub severity: *mut ::std::os::raw::c_char,
    pub code: *mut ::std::os::raw::c_char,
    pub message: *mut ::std::os::raw::c_char,
    pub detail: *mut ::std::os::raw::c_char,
}
#[allow(clippy::unnecessary_operation, clippy::identity_op)]
const _: () = {
    ["Size of Error"][::std::mem::size_of::<Error>() - 32usize];
    ["Alignment of Error"][::std::mem::align_of::<Error>() - 8usize];
    ["Offset of field: Error::severity"][::std::mem::offset_of!(Error, severity) - 0usize];
    ["Offset of field: Error::code"][::std::mem::offset_of!(Error, code) - 8usize];
    ["Offset of field: Error::message"][::std::mem::offset_of!(Error, message) - 16usize];
    ["Offset of field: Error::detail"][::std::mem::offset_of!(Error, detail) - 24usize];
};
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct RowColumn {
    pub length: ::std::os::raw::c_int,
    pub data: *mut ::std::os::raw::c_char,
}
#[allow(clippy::unnecessary_operation, clippy::identity_op)]
const _: () = {
    ["Size of RowColumn"][::std::mem::size_of::<RowColumn>() - 16usize];
    ["Alignment of RowColumn"][::std::mem::align_of::<RowColumn>() - 8usize];
    ["Offset of field: RowColumn::length"][::std::mem::offset_of!(RowColumn, length) - 0usize];
    ["Offset of field: RowColumn::data"][::std::mem::offset_of!(RowColumn, data) - 8usize];
};
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct Row {
    pub num_columns: ::std::os::raw::c_int,
    pub columns: *mut RowColumn,
}
#[allow(clippy::unnecessary_operation, clippy::identity_op)]
const _: () = {
    ["Size of Row"][::std::mem::size_of::<Row>() - 16usize];
    ["Alignment of Row"][::std::mem::align_of::<Row>() - 8usize];
    ["Offset of field: Row::num_columns"][::std::mem::offset_of!(Row, num_columns) - 0usize];
    ["Offset of field: Row::columns"][::std::mem::offset_of!(Row, columns) - 8usize];
};
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct RowDescriptionColumn {
    pub len: ::std::os::raw::c_int,
    pub name: *mut ::std::os::raw::c_char,
    pub oid: ::std::os::raw::c_int,
}
#[allow(clippy::unnecessary_operation, clippy::identity_op)]
const _: () = {
    ["Size of RowDescriptionColumn"][::std::mem::size_of::<RowDescriptionColumn>() - 24usize];
    ["Alignment of RowDescriptionColumn"][::std::mem::align_of::<RowDescriptionColumn>() - 8usize];
    ["Offset of field: RowDescriptionColumn::len"]
        [::std::mem::offset_of!(RowDescriptionColumn, len) - 0usize];
    ["Offset of field: RowDescriptionColumn::name"]
        [::std::mem::offset_of!(RowDescriptionColumn, name) - 8usize];
    ["Offset of field: RowDescriptionColumn::oid"]
        [::std::mem::offset_of!(RowDescriptionColumn, oid) - 16usize];
};
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct RowDescription {
    pub num_columns: ::std::os::raw::c_int,
    pub columns: *mut RowDescriptionColumn,
}
#[allow(clippy::unnecessary_operation, clippy::identity_op)]
const _: () = {
    ["Size of RowDescription"][::std::mem::size_of::<RowDescription>() - 16usize];
    ["Alignment of RowDescription"][::std::mem::align_of::<RowDescription>() - 8usize];
    ["Offset of field: RowDescription::num_columns"]
        [::std::mem::offset_of!(RowDescription, num_columns) - 0usize];
    ["Offset of field: RowDescription::columns"]
        [::std::mem::offset_of!(RowDescription, columns) - 8usize];
};
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct Intercept {
    pub row_description: RowDescription,
    pub num_rows: ::std::os::raw::c_int,
    pub rows: *mut Row,
}
#[allow(clippy::unnecessary_operation, clippy::identity_op)]
const _: () = {
    ["Size of Intercept"][::std::mem::size_of::<Intercept>() - 32usize];
    ["Alignment of Intercept"][::std::mem::align_of::<Intercept>() - 8usize];
    ["Offset of field: Intercept::row_description"]
        [::std::mem::offset_of!(Intercept, row_description) - 0usize];
    ["Offset of field: Intercept::num_rows"][::std::mem::offset_of!(Intercept, num_rows) - 16usize];
    ["Offset of field: Intercept::rows"][::std::mem::offset_of!(Intercept, rows) - 24usize];
};
#[repr(C)]
#[derive(Copy, Clone)]
pub union RoutingOutput {
    pub route: Route,
    pub error: Error,
    pub intercept: Intercept,
}
#[allow(clippy::unnecessary_operation, clippy::identity_op)]
const _: () = {
    ["Size of RoutingOutput"][::std::mem::size_of::<RoutingOutput>() - 32usize];
    ["Alignment of RoutingOutput"][::std::mem::align_of::<RoutingOutput>() - 8usize];
    ["Offset of field: RoutingOutput::route"]
        [::std::mem::offset_of!(RoutingOutput, route) - 0usize];
    ["Offset of field: RoutingOutput::error"]
        [::std::mem::offset_of!(RoutingOutput, error) - 0usize];
    ["Offset of field: RoutingOutput::intercept"]
        [::std::mem::offset_of!(RoutingOutput, intercept) - 0usize];
};
#[repr(C)]
#[derive(Copy, Clone)]
pub struct Output {
    pub decision: RoutingDecision,
    pub output: RoutingOutput,
}
#[allow(clippy::unnecessary_operation, clippy::identity_op)]
const _: () = {
    ["Size of Output"][::std::mem::size_of::<Output>() - 40usize];
    ["Alignment of Output"][::std::mem::align_of::<Output>() - 8usize];
    ["Offset of field: Output::decision"][::std::mem::offset_of!(Output, decision) - 0usize];
    ["Offset of field: Output::output"][::std::mem::offset_of!(Output, output) - 8usize];
};
pub const Role_PRIMARY: Role = 1;
pub const Role_REPLICA: Role = 2;
pub type Role = ::std::os::raw::c_uint;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct DatabaseConfig {
    pub shard: ::std::os::raw::c_int,
    pub role: Role,
    pub host: *mut ::std::os::raw::c_char,
    pub port: ::std::os::raw::c_int,
}
#[allow(clippy::unnecessary_operation, clippy::identity_op)]
const _: () = {
    ["Size of DatabaseConfig"][::std::mem::size_of::<DatabaseConfig>() - 24usize];
    ["Alignment of DatabaseConfig"][::std::mem::align_of::<DatabaseConfig>() - 8usize];
    ["Offset of field: DatabaseConfig::shard"]
        [::std::mem::offset_of!(DatabaseConfig, shard) - 0usize];
    ["Offset of field: DatabaseConfig::role"]
        [::std::mem::offset_of!(DatabaseConfig, role) - 4usize];
    ["Offset of field: DatabaseConfig::host"]
        [::std::mem::offset_of!(DatabaseConfig, host) - 8usize];
    ["Offset of field: DatabaseConfig::port"]
        [::std::mem::offset_of!(DatabaseConfig, port) - 16usize];
};
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct Config {
    pub num_databases: ::std::os::raw::c_int,
    pub databases: *mut DatabaseConfig,
    pub name: *mut ::std::os::raw::c_char,
    pub shards: ::std::os::raw::c_int,
}
#[allow(clippy::unnecessary_operation, clippy::identity_op)]
const _: () = {
    ["Size of Config"][::std::mem::size_of::<Config>() - 32usize];
    ["Alignment of Config"][::std::mem::align_of::<Config>() - 8usize];
    ["Offset of field: Config::num_databases"]
        [::std::mem::offset_of!(Config, num_databases) - 0usize];
    ["Offset of field: Config::databases"][::std::mem::offset_of!(Config, databases) - 8usize];
    ["Offset of field: Config::name"][::std::mem::offset_of!(Config, name) - 16usize];
    ["Offset of field: Config::shards"][::std::mem::offset_of!(Config, shards) - 24usize];
};
#[repr(C)]
#[derive(Copy, Clone)]
pub union RoutingInput {
    pub query: Query,
}
#[allow(clippy::unnecessary_operation, clippy::identity_op)]
const _: () = {
    ["Size of RoutingInput"][::std::mem::size_of::<RoutingInput>() - 32usize];
    ["Alignment of RoutingInput"][::std::mem::align_of::<RoutingInput>() - 8usize];
    ["Offset of field: RoutingInput::query"][::std::mem::offset_of!(RoutingInput, query) - 0usize];
};
pub const InputType_ROUTING_INPUT: InputType = 1;
pub type InputType = ::std::os::raw::c_uint;
#[repr(C)]
#[derive(Copy, Clone)]
pub struct Input {
    pub config: Config,
    pub input_type: InputType,
    pub input: RoutingInput,
}
#[allow(clippy::unnecessary_operation, clippy::identity_op)]
const _: () = {
    ["Size of Input"][::std::mem::size_of::<Input>() - 72usize];
    ["Alignment of Input"][::std::mem::align_of::<Input>() - 8usize];
    ["Offset of field: Input::config"][::std::mem::offset_of!(Input, config) - 0usize];
    ["Offset of field: Input::input_type"][::std::mem::offset_of!(Input, input_type) - 32usize];
    ["Offset of field: Input::input"][::std::mem::offset_of!(Input, input) - 40usize];
};
