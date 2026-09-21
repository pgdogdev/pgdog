use super::super::ee;
use super::insert::{build_resolved_split_requests, build_split_requests};
use super::nextval::SequenceCall;
use super::offset::OffsetPlan;
use super::{
    Error, InsertSplit, PrepareExecute, ShardingKeyUpdate, aggregate::AggregateRewritePlan,
};
use crate::frontend::client::QueryTimestamps;
use crate::frontend::router::parser::rewrite::statement::non_deterministic_funcs::NDFunction;
use crate::frontend::{BufferedQuery, ClientRequest, PreparedStatements};
use crate::net::messages::bind::{Format, Parameter};
use crate::net::{Bind, Parse, ProtocolMessage, Query, parameter::ParameterValue};
use crate::unique_id::UniqueId;

/// TODO: Docs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GeneratedParam {
    pub(crate) generated_id: GeneratedId,
    pub(crate) param_num: u16,
}

/// TODO: Document that this is also stored in PreparedStatement cache.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum GeneratedId {
    UniqueId,
    Sequence(SequenceCall),
    /// This represents a function (such as date/time, UUID) that was re-written to a constant
    /// to be consistent across shards for omni writes.
    NDFunction(NDFunction),
}

/// Statement rewrite plan.
///
/// Executed in order of fields in this struct.
///
#[derive(Default, Clone, Debug)]
pub(crate) struct RewritePlan {
    /// Number of parameters ($1, $2, etc.) in
    /// the original statement. This is calculated first,
    /// and $params+n parameters are added to the statement to
    /// substitute values we are rewriting.
    pub(crate) params: u16,

    /// Number of unique IDs, also used by SQL PREPARE/EXECUTE rewriting.
    pub(crate) unique_ids: u16,

    /// Number of auto-injected primary key columns with pgdog.unique_id().
    pub(crate) auto_id_injected: u16,

    /// One-based parameter indexes and ID sources in allocation order.
    /// Simple protocol records sequence calls here without using the indexes.
    /// TODO: Document that this is also stored in PreparedStatement cache.
    pub(crate) generated_params: Vec<GeneratedParam>,

    /// Rewritten SQL statement.
    pub(crate) stmt: Option<String>,

    /// Prepared statements to prepend to the client request.
    /// Each tuple contains (name, statement) for ProtocolMessage::Prepare.
    pub(crate) prepare_rewrites: Vec<PrepareExecute>,

    /// Splitting of multi-tuple INSERT statements into
    /// multiple queries.
    pub(crate) insert_split: Vec<InsertSplit>,

    /// Position in the result where the count(*) or count(name)
    /// functions are added.
    pub(crate) aggregates: AggregateRewritePlan,

    /// Sharding key is being updated, we need to execute
    /// a multi-step plan.
    pub(crate) sharding_key_update: Option<ShardingKeyUpdate>,

    /// Limit/offset pagination.
    pub(crate) offset: Option<OffsetPlan>,
}

#[derive(Debug, Clone)]
pub(crate) enum RewriteResult {
    InPlace { offset: Option<OffsetPlan> },
    InsertSplit(Vec<ClientRequest>),
    ShardingKeyUpdate(ShardingKeyUpdate),
}

impl RewriteResult {
    pub(crate) fn apply_after_parser(&self, request: &mut ClientRequest) -> Result<(), Error> {
        match self {
            Self::InPlace {
                offset: Some(offset),
            } => offset.apply_after_parser(request),
            _ => Ok(()),
        }
    }
}

impl RewritePlan {
    /// True if the plan would not modify the query or its messages.
    /// `params` is purely informational (count of original `$N` placeholders)
    /// and doesn't count as a rewrite.
    pub(crate) fn is_empty(&self) -> bool {
        self.unique_ids == 0
            && self.auto_id_injected == 0
            && self.generated_params.is_empty()
            && self.stmt.is_none()
            && self.prepare_rewrites.is_empty()
            && self.insert_split.is_empty()
            && self.aggregates.is_noop()
            && self.sharding_key_update.is_none()
            && self.offset.is_none()
        // TODO: Check here.
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
        for generated_param in &self.generated_params {
            let source = &generated_param.generated_id;
            let num = generated_param.param_num;
            assert_eq!(bind.params_raw().len() + 1, num as usize);

            let param = match source {
                GeneratedId::UniqueId => {
                    Self::convert_int_to_param(UniqueId::generator()?.next_id(), format)
                }
                GeneratedId::Sequence(call) => {
                    Self::convert_int_to_param(execute(call).await?, format)
                }
                GeneratedId::NDFunction(nd_func) => {
                    let (text, binary) = nd_func.write_as_constant(&timestamps, timezone)?;
                    match format {
                        Format::Binary => Parameter::new(binary.as_slice()),
                        Format::Text => Parameter::new(text.as_bytes()),
                    }
                }
            };

            bind.push_param(param, format);
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
            let client_params = self.params.max(parse.num_data_types());

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
            .generated_params
            .iter()
            .any(|source| matches!(source.generated_id, GeneratedId::Sequence(_)))
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
                        if request
                            .messages
                            .iter()
                            .any(|message| matches!(message, ProtocolMessage::Query(_)))
                        {
                            request.messages.clear();
                            request.push(ProtocolMessage::PrepareFromClient(prepare.clone()));
                        } else {
                            // Keep Parse/Bind/Describe/Sync and their corresponding replies.
                            // Preparing the outer statement must not execute the SQL PREPARE.
                            for message in &mut request.messages {
                                if let ProtocolMessage::Execute(execute) = message {
                                    *message = ProtocolMessage::ExecutePrepare {
                                        execute: execute.clone(),
                                        prepare: prepare.clone(),
                                    };
                                }
                            }
                        }
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
                // Only ordinary statements need generated values appended to Bind.
                // A nonempty prepare_rewrites means SQL PREPARE/EXECUTE: in
                // `PREPARE foo AS INSERT INTO t VALUES ($1)`, $1 is supplied by
                // a later EXECUTE, not this Bind. EXECUTE's generated values
                // are already written into its SQL arguments.
                ProtocolMessage::Bind(bind) if self.prepare_rewrites.is_empty() => {
                    self.apply_bind(bind, timezone, timestamps).await?
                }
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

        if self
            .prepare_rewrites
            .iter()
            .any(|rewrite| matches!(rewrite, PrepareExecute::Execute(_)))
            && let Some(BufferedQuery::Prepared(mut parse)) = request.query()?
        {
            let mut bind_index = None;
            for (index, message) in request.messages.iter_mut().enumerate() {
                if let ProtocolMessage::Bind(bind) = message {
                    bind_index.get_or_insert(index);
                    // Keep the original Bind name for the cross-shard result decoder.
                    *message = ProtocolMessage::BindAnonymous(bind.clone());
                }
            }
            if let Some(bind_index) = bind_index {
                // A cached outer EXECUTE can contain per-execution values and shard-
                // dependent LIMIT/OFFSET. Parse it again without adding cache entries.
                parse.anonymize();
                request.anonymous_client_params = self.apply_parse(&mut parse);
                request.last_parse = None;
                // EnsurePrepared is a simple Query and destroys unnamed statements,
                // so the internal Parse must follow it and precede Bind.
                request
                    .messages
                    .insert(bind_index, ProtocolMessage::EnsureParsed(parse));
            }
        }

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
                .generated_params
                .iter()
                .any(|source| matches!(source.generated_id, GeneratedId::Sequence(_)))
                && let Some(query) = request.messages.iter().find_map(|message| match message {
                    ProtocolMessage::Query(query) => Some(query),
                    _ => None,
                })
            {
                return Ok(RewriteResult::InsertSplit(build_resolved_split_requests(
                    query, request,
                )?));
            }
            let requests = build_split_requests(&self.insert_split, request)?;
            return Ok(RewriteResult::InsertSplit(requests));
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
            unique_ids: 1,
            generated_params: vec![GeneratedParam {
                generated_id: GeneratedId::UniqueId,
                param_num: 1,
            }],
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
            params: 1,
            unique_ids: 1,
            generated_params: vec![GeneratedParam {
                param_num: 2,
                generated_id: GeneratedId::UniqueId,
            }],
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
            params: 2,
            unique_ids: 1,
            generated_params: vec![GeneratedParam {
                param_num: 3,
                generated_id: GeneratedId::UniqueId,
            }],
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
            unique_ids: 3,
            generated_params: vec![
                GeneratedParam {
                    param_num: 1,
                    generated_id: GeneratedId::UniqueId,
                },
                GeneratedParam {
                    param_num: 2,
                    generated_id: GeneratedId::UniqueId,
                },
                GeneratedParam {
                    param_num: 3,
                    generated_id: GeneratedId::UniqueId,
                },
            ],
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
            params: 2,
            unique_ids: 2,
            generated_params: vec![
                GeneratedParam {
                    param_num: 3,
                    generated_id: GeneratedId::UniqueId,
                },
                GeneratedParam {
                    param_num: 4,
                    generated_id: GeneratedId::UniqueId,
                },
            ],
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
