use super::super::ee;
use super::insert::{build_resolved_split_requests, build_split_requests};
use super::nextval::SequenceCall;
use super::offset::OffsetPlan;
use super::{Error, InsertSplit, InsertSplitRewriteResult, PrepareExecute, ShardingKeyUpdate};
use crate::frontend::client::QueryTimestamps;

use crate::frontend::router::Route;
use crate::frontend::router::parser::ShardWithPriority;
use crate::frontend::router::parser::rewrite::statement::non_deterministic_funcs::NDFunction;
use crate::frontend::{ClientRequest, PreparedStatements};
use crate::net::messages::bind::{Format, Parameter};
use crate::net::{Bind, Parse, ProtocolMessage, Query, parameter::ParameterValue};
use crate::unique_id::UniqueId;
use itertools::Either;
use std::borrow::Cow;

/// TODO: Document that this is also stored in PreparedStatement cache.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::frontend) enum BindParam {
    FromClientBind(u16),
    UniqueId,
    Sequence(SequenceCall),
    /// This represents a function (such as date/time, UUID) that was re-written to a constant
    /// to be consistent across shards for omni writes.
    NDFunction(NDFunction),
}

#[derive(Clone, Debug)]
pub(in crate::frontend) enum BindParams {
    Original { param_count: u16 },
    Modified { params: Vec<BindParam> },
}

impl BindParams {
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Original { param_count } => *param_count as usize,
            Self::Modified { params } => params.len(),
        }
    }

    pub(super) fn is_original(&self) -> bool {
        matches!(self, Self::Original { .. })
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = Cow<'_, BindParam>> {
        match self {
            Self::Original { param_count } => {
                Either::Left((0..*param_count).map(|n| Cow::Owned(BindParam::FromClientBind(n))))
            }
            Self::Modified { params } => Either::Right(params.iter().map(Cow::Borrowed)),
        }
    }

    pub(super) fn push(&mut self, param: BindParam) {
        let params = self.force_modified();
        params.push(param);
    }

    pub(super) fn num_client_params(&self) -> u16 {
        match self {
            Self::Original { param_count } => *param_count,
            Self::Modified { params } => params
                .iter()
                .filter(|b| matches!(b, BindParam::FromClientBind(_)))
                .count() as u16,
        }
    }

    #[cfg(test)]
    pub(crate) fn generated(&self) -> impl Iterator<Item = Cow<'_, BindParam>> {
        self.iter()
            .filter(|b| !matches!(b.as_ref(), BindParam::FromClientBind(_)))
    }

    /// If `self` is `Self::Original`, canoncalize it as `Modified` containing
    /// `param_count` instances of `BindParam::FromClientBind`. Returns a
    /// reference to the canoncalized `Vec`
    fn force_modified(&mut self) -> &mut Vec<BindParam> {
        if let Self::Original { .. } = self {
            let params = self.iter().map(|p| p.into_owned()).collect();
            *self = Self::Modified { params };
        }

        match self {
            Self::Original { .. } => unreachable!("set to Modified above"),
            Self::Modified { params } => params,
        }
    }
}

impl Default for BindParams {
    fn default() -> Self {
        Self::Original { param_count: 0 }
    }
}

impl From<Vec<BindParam>> for BindParams {
    fn from(params: Vec<BindParam>) -> Self {
        Self::Modified { params }
    }
}

#[cfg(test)]
impl<T> PartialEq<T> for BindParams
where
    Vec<BindParam>: PartialEq<T>,
{
    fn eq(&self, other: &T) -> bool {
        match self {
            Self::Original { .. } => false,
            Self::Modified { params } => params == other,
        }
    }
}

/// Statement rewrite plan.
///
/// Executed in order of fields in this struct.
///
#[derive(Default, Clone, Debug)]
pub(crate) struct RewritePlan {
    /// One-based parameter indexes and ID sources in allocation order.
    /// Simple protocol records sequence calls here without using the indexes.
    /// TODO: Document that this is also stored in PreparedStatement cache.
    pub(super) bind_params: BindParams,

    /// Rewritten SQL statement.
    pub(crate) stmt: Option<String>,

    /// Prepared statements to prepend to the client request.
    /// Each tuple contains (name, statement) for ProtocolMessage::Prepare.
    pub(crate) prepare_rewrites: Vec<PrepareExecute>,

    /// Splitting of multi-tuple INSERT statements into
    /// multiple queries.
    pub(crate) insert_split: Vec<InsertSplit>,

    /// Sharding key is being updated, we need to execute
    /// a multi-step plan.
    pub(crate) sharding_key_update: Option<ShardingKeyUpdate>,

    /// Limit/offset pagination.
    pub(crate) offset: Option<OffsetPlan>,
}

#[derive(Debug, Clone)]
pub(crate) enum RewriteResult {
    InPlace { offset: Option<OffsetPlan> },
    InsertSplit(InsertSplitRewriteResult),
    ShardingKeyUpdate(ShardingKeyUpdate),
}

impl RewriteResult {
    /// The rewrite can possibly need more than one shard.
    pub(crate) fn connect_route(&self) -> Option<Route> {
        use crate::frontend::router::parser::{Shard, ShardWithPriority};
        match self {
            Self::InsertSplit(rewrite) => {
                if let Some(same_shard) = rewrite.same_shard() {
                    Some(Route::write(ShardWithPriority::new_table(same_shard)))
                } else {
                    Some(Route::write(ShardWithPriority::new_table(Shard::All)))
                }
            }
            Self::ShardingKeyUpdate(_) => {
                Some(Route::write(ShardWithPriority::new_table(Shard::All)))
            }
            Self::InPlace { .. } => None,
        }
    }

    pub(crate) fn offset_plan(&self) -> Option<&OffsetPlan> {
        match self {
            Self::InPlace { offset } => offset.as_ref(),
            _ => None,
        }
    }

    pub(crate) fn apply_after_route(&self, request: &mut ClientRequest) -> Result<(), Error> {
        match self {
            Self::InPlace {
                offset: Some(offset),
            } => offset.apply_after_route(request),
            Self::InsertSplit(requests) => {
                // Short-circuit multi-row inserts to one shard,
                // if they only need one.
                if let (Some(shard), Some(route)) = (requests.same_shard(), request.route.as_mut())
                {
                    route.set_shard(ShardWithPriority::new_table(shard));
                }

                Ok(())
            }
            _ => Ok(()),
        }
    }
}

impl RewritePlan {
    /// True if the plan would not modify the query or its messages.
    /// `params` is purely informational (count of original `$N` placeholders)
    /// and doesn't count as a rewrite.
    pub(crate) fn is_empty(&self) -> bool {
        self.bind_params.is_original()
            && self.stmt.is_none()
            && self.prepare_rewrites.is_empty()
            && self.insert_split.is_empty()
            && self.sharding_key_update.is_none()
            && self.offset.is_none()
    }

    /// Append generated unique IDs and sequence values to a Bind message.
    async fn apply_bind(
        &self,
        bind: &mut Bind,
        timezone: Option<&ParameterValue>,
        timestamps: QueryTimestamps,
    ) -> Result<(), Error> {
        self.apply_generated_ids(bind, timezone, timestamps, SequenceCall::execute)
            .await
    }

    /// Append values in the same order their placeholders were allocated.
    ///
    /// `timezone` = client's setting (or the database default when None)
    pub(super) async fn apply_generated_ids(
        &self,
        bind: &mut Bind,
        timezone: Option<&ParameterValue>,
        timestamps: QueryTimestamps,
        mut execute: impl AsyncFnMut(&SequenceCall) -> Result<i64, ee::Error>,
    ) -> Result<(), Error> {
        let format = bind.default_param_format();
        for (idx, source) in self.bind_params.iter().enumerate() {
            let param = match &*source {
                BindParam::FromClientBind(n) => {
                    debug_assert_eq!(idx, *n as usize, "mapped bind params should not appear yet");
                    None
                }
                BindParam::UniqueId => Some(Self::convert_int_to_param(
                    UniqueId::generator()?.next_id(),
                    format,
                )),
                BindParam::Sequence(call) => {
                    Some(Self::convert_int_to_param(execute(call).await?, format))
                }
                BindParam::NDFunction(nd_func) => {
                    let (text, binary) = nd_func.write_as_constant(&timestamps, timezone)?;
                    Some(match format {
                        Format::Binary => Parameter::new(binary.as_slice()),
                        Format::Text => Parameter::new(text.as_bytes()),
                    })
                }
            };

            if let Some(param) = param {
                bind.push_param(param, format);
            }
        }

        Ok(())
    }

    fn convert_int_to_param(id: i64, format: Format) -> Parameter {
        match format {
            Format::Binary => Parameter::new(&id.to_be_bytes()),
            Format::Text => Parameter::new(itoa::Buffer::new().format(id).as_bytes()),
        }
    }

    /// Apply the rewrite plan to a Parse message by updating the SQL.
    ///
    /// Returns the client's parameter count for an unnamed statement
    fn apply_parse(&self, parse: &mut Parse) -> Option<u16> {
        if let Some(ref stmt) = self.stmt {
            let client_params = (self.bind_params.num_client_params()).max(parse.num_data_types());

            parse.set_query(stmt);
            if !parse.anonymous() {
                PreparedStatements::global()
                    .write()
                    .rewrite(parse, client_params);
            } else {
                return Some(client_params);
            }
        }

        None
    }

    /// Apply the rewrite plan to a Query message by updating the SQL.
    async fn apply_query(&self, query: &mut Query) -> Result<(), Error> {
        if self
            .bind_params
            .iter()
            .any(|source| matches!(&*source, BindParam::Sequence(_)))
        {
            if let Some(stmt) = self.rewrite_sequence_simple().await? {
                query.set_query(&stmt);
            }
        } else if let Some(ref stmt) = self.stmt {
            query.set_query(stmt);
        }

        Ok(())
    }

    /// Apply the rewrite plan to a ClientRequest.
    pub(crate) async fn apply(
        &self,
        request: &mut ClientRequest,
        timezone: Option<&ParameterValue>,
        timestamps: QueryTimestamps,
    ) -> Result<RewriteResult, Error> {
        // Prepend any required Prepare messages for EXECUTE statements.
        if !self.prepare_rewrites.is_empty() {
            self.prepare_rewrites
                .iter()
                .for_each(|prepare| match prepare {
                    PrepareExecute::Prepare(prepare) => {
                        request.messages.clear();
                        request.push(ProtocolMessage::PrepareFromClient(prepare.clone()));
                    }
                    PrepareExecute::Execute(prepare) => {
                        request
                            .messages
                            .splice(0..0, vec![ProtocolMessage::EnsurePrepared(prepare.clone())]);
                    }
                });
        }

        let mut anonymous_client_params = None;

        for message in request.messages.iter_mut() {
            match message {
                ProtocolMessage::Parse(parse) => {
                    anonymous_client_params = self.apply_parse(parse);
                }
                ProtocolMessage::Query(query) => self.apply_query(query).await?,
                ProtocolMessage::Bind(bind) => self.apply_bind(bind, timezone, timestamps).await?,
                _ => {}
            }
        }

        if request.is_executable()
            && request.needs_parse_injection()
            && let Some(parse) = request.last_parse.as_mut()
        {
            anonymous_client_params = self.apply_parse(parse);
        }

        request.anonymous_client_params = anonymous_client_params;

        self.apply_after_messages(request)
    }

    /// Build the execution plan after SQL and Bind values have been rewritten.
    pub(super) fn apply_after_messages(
        &self,
        request: &ClientRequest,
    ) -> Result<RewriteResult, Error> {
        // Only rewrite executable requests. Some clients prepare the statement
        // separately (e.g. go/pq with Parse, Describe, Sync). We don't need to rewrite
        // those since insert split will return the same row(s) as multi-tuple insert.
        if !self.insert_split.is_empty() && request.is_executable() {
            if self
                .bind_params
                .iter()
                .any(|source| matches!(&*source, BindParam::Sequence(_)))
                && let Some(query) = request.messages.iter().find_map(|message| match message {
                    ProtocolMessage::Query(query) => Some(query),
                    _ => None,
                })
            {
                return Ok(RewriteResult::InsertSplit(InsertSplitRewriteResult {
                    requests: build_resolved_split_requests(query, request)?,
                }));
            }
            let requests = build_split_requests(&self.insert_split, request)?;
            return Ok(RewriteResult::InsertSplit(InsertSplitRewriteResult {
                requests,
            }));
        }

        if let Some(sharding_key_update) = &self.sharding_key_update
            && request.is_executable()
        {
            return Ok(RewriteResult::ShardingKeyUpdate(
                sharding_key_update.clone(),
            ));
        }

        Ok(RewriteResult::InPlace {
            offset: self.offset.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::set_env_var;
    use std::collections::HashSet;

    #[tokio::test]
    async fn test_apply_query_without_recorded_sequences_skips_nextval() {
        let stmt = "SELECT pgdog.nextval('seq')";
        let plan = RewritePlan {
            stmt: Some(stmt.to_owned()),
            ..Default::default()
        };
        let mut query = Query::new("SELECT 1");
        plan.apply_query(&mut query)
            .await
            .expect("no recorded sequences");
        assert_eq!(query.query(), stmt);
    }

    #[tokio::test]
    async fn test_apply_bind_no_unique_ids() {
        let _guard = set_env_var("NODE_ID", "pgdog-1");
        let plan = RewritePlan::default();
        let mut bind = Bind::default();
        plan.apply_bind(&mut bind, None, QueryTimestamps::now())
            .await
            .unwrap();
        assert_eq!(bind.params_raw().len(), 0);
    }

    #[tokio::test]
    async fn test_apply_bind_text_format() {
        let _guard = set_env_var("NODE_ID", "pgdog-1");
        let plan = RewritePlan {
            bind_params: vec![BindParam::UniqueId].into(),
            ..Default::default()
        };
        let mut bind = Bind::default();
        plan.apply_bind(&mut bind, None, QueryTimestamps::now())
            .await
            .unwrap();
        assert_eq!(bind.params_raw().len(), 1);

        // Default format is Text, so data should be a string
        let param = &bind.params_raw()[0];
        let text = std::str::from_utf8(&param.data).unwrap();
        let _id: i64 = text.parse().expect("should be valid i64 text");

        // No format codes needed for all-text
        assert_eq!(bind.format_codes_raw().len(), 0);
    }

    #[tokio::test]
    async fn test_apply_bind_binary_format_uniform() {
        let _guard = set_env_var("NODE_ID", "pgdog-1");
        let plan = RewritePlan {
            bind_params: vec![BindParam::FromClientBind(0), BindParam::UniqueId].into(),
            ..Default::default()
        };
        // Create bind with uniform binary format (1 code applies to all)
        let mut bind =
            Bind::new_params_codes("test", &[Parameter::new(b"existing")], &[Format::Binary]);
        plan.apply_bind(&mut bind, None, QueryTimestamps::now())
            .await
            .unwrap();
        assert_eq!(bind.params_raw().len(), 2);

        // Should use binary format: 8 bytes big-endian
        let param = &bind.params_raw()[1];
        assert_eq!(param.data.len(), 8, "binary bigint should be 8 bytes");
        let id = i64::from_be_bytes(param.data[..].try_into().unwrap());
        assert!(id > 0, "ID should be positive");

        // Uniform format preserved (still 1 code)
        assert_eq!(bind.format_codes_raw().len(), 1);
        assert_eq!(bind.format_codes_raw()[0], Format::Binary);
    }

    #[tokio::test]
    async fn test_apply_bind_binary_format_one_to_one() {
        let _guard = set_env_var("NODE_ID", "pgdog-1");
        let plan = RewritePlan {
            bind_params: vec![
                BindParam::FromClientBind(0),
                BindParam::FromClientBind(1),
                BindParam::UniqueId,
            ]
            .into(),
            ..Default::default()
        };
        // Create bind with one-to-one format codes
        let mut bind = Bind::new_params_codes(
            "test",
            &[Parameter::new(b"a"), Parameter::new(b"b")],
            &[Format::Binary, Format::Binary],
        );
        plan.apply_bind(&mut bind, None, QueryTimestamps::now())
            .await
            .unwrap();
        assert_eq!(bind.params_raw().len(), 3);

        // New param should be text (default for one-to-one)
        let param = &bind.params_raw()[2];
        let text = std::str::from_utf8(&param.data).unwrap();
        let _: i64 = text.parse().expect("should be valid i64 text");

        // Format code added for new param
        assert_eq!(bind.format_codes_raw().len(), 3);
        assert_eq!(bind.format_codes_raw()[2], Format::Text);
    }

    #[tokio::test]
    async fn test_apply_bind_multiple_unique_ids() {
        let _guard = set_env_var("NODE_ID", "pgdog-1");
        let plan = RewritePlan {
            bind_params: vec![BindParam::UniqueId; 3].into(),
            ..Default::default()
        };
        let mut bind = Bind::default();
        plan.apply_bind(&mut bind, None, QueryTimestamps::now())
            .await
            .unwrap();
        assert_eq!(bind.params_raw().len(), 3);

        let mut ids = HashSet::new();
        for param in bind.params_raw() {
            let text = std::str::from_utf8(&param.data).unwrap();
            let id: i64 = text.parse().expect("should be valid i64");
            ids.insert(id);
        }
        assert_eq!(ids.len(), 3, "all IDs should be unique");
    }

    #[tokio::test]
    async fn test_apply_bind_appends_to_existing_params() {
        let _guard = set_env_var("NODE_ID", "pgdog-1");
        let plan = RewritePlan {
            bind_params: vec![
                BindParam::FromClientBind(0),
                BindParam::FromClientBind(1),
                BindParam::UniqueId,
                BindParam::UniqueId,
            ]
            .into(),
            ..Default::default()
        };
        let mut bind = Bind::new_params(
            "test",
            &[Parameter::new(b"existing1"), Parameter::new(b"existing2")],
        );
        plan.apply_bind(&mut bind, None, QueryTimestamps::now())
            .await
            .unwrap();
        assert_eq!(bind.params_raw().len(), 4);

        assert_eq!(bind.params_raw()[0].data.as_ref(), b"existing1");
        assert_eq!(bind.params_raw()[1].data.as_ref(), b"existing2");

        let text = std::str::from_utf8(&bind.params_raw()[2].data).unwrap();
        let _: i64 = text.parse().expect("should be valid i64");
    }
}
