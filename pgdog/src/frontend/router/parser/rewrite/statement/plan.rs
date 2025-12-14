use crate::frontend::{ClientRequest, PreparedStatements};
use crate::net::messages::bind::{Format, Parameter};
use crate::net::{Bind, Parse, ProtocolMessage, Query};
use crate::unique_id::UniqueId;

use super::aggregate::RewriteOutput;
use super::insert::build_split_requests;
use super::{Error, InsertSplit};

/// Statement rewrite plan.
///
/// Executed in order of fields in this struct.
///
#[derive(Default, Clone, Debug)]
pub struct RewritePlan {
    /// Number of parameters ($1, $2, etc.) in
    /// the original statement. This is calculated first,
    /// and $params+n parameters are added to the statement to
    /// substitute values we are rewriting.
    pub(super) params: u16,

    /// Number of unique IDs to append to the Bind message.
    pub(super) unique_ids: u16,

    /// Rewritten SQL statement.
    pub(super) stmt: Option<String>,

    /// Prepared statements to prepend to the client request.
    /// Each tuple contains (name, statement) for ProtocolMessage::Prepare.
    pub(super) prepares: Vec<(String, String)>,

    /// Insert split.
    pub(super) insert_split: Vec<InsertSplit>,

    /// Position in the result where the count(*) or count(name)
    /// functions are added.
    pub(super) aggregates: RewriteOutput,
}

#[derive(Debug, Clone)]
pub enum RewriteResult {
    InPlace,
    InsertSplit(Vec<ClientRequest>),
}

impl RewritePlan {
    /// Apply the rewrite plan to a Bind message by appending generated unique IDs.
    pub fn apply_bind(&self, bind: &mut Bind) -> Result<(), Error> {
        let format = bind.default_param_format();

        for _ in 0..self.unique_ids {
            let generator = UniqueId::generator()?;
            let id = generator.next_id();
            let param = match format {
                Format::Binary => Parameter::new(&id.to_be_bytes()),
                Format::Text => Parameter::new(id.to_string().as_bytes()),
            };
            bind.push_param(param, format);
        }
        Ok(())
    }

    /// Apply the rewrite plan to a Parse message by updating the SQL.
    pub fn apply_parse(&self, parse: &mut Parse) {
        if let Some(ref stmt) = self.stmt {
            parse.set_query(stmt);
            if !parse.anonymous() {
                PreparedStatements::global().write().rewrite(&parse);
            }
        }
    }

    /// Apply the rewrite plan to a Query message by updating the SQL.
    pub fn apply_query(&self, query: &mut Query) {
        if let Some(ref stmt) = self.stmt {
            query.set_query(stmt);
        }
    }

    /// Apply the rewrite plan to a ClientRequest.
    pub fn apply(&self, request: &mut ClientRequest) -> Result<RewriteResult, Error> {
        // Prepend any required Prepare messages for EXECUTE statements.
        if !self.prepares.is_empty() {
            let prepends: Vec<ProtocolMessage> = self
                .prepares
                .iter()
                .map(|(name, statement)| ProtocolMessage::Prepare {
                    name: name.clone(),
                    statement: statement.clone(),
                })
                .collect();
            request.messages.splice(0..0, prepends);
        }

        for message in request.messages.iter_mut() {
            match message {
                ProtocolMessage::Parse(parse) => self.apply_parse(parse),
                ProtocolMessage::Query(query) => self.apply_query(query),
                ProtocolMessage::Bind(bind) => self.apply_bind(bind)?,
                _ => {}
            }
        }

        if !self.insert_split.is_empty() {
            let requests = build_split_requests(&self.insert_split, request);
            return Ok(RewriteResult::InsertSplit(requests));
        }

        Ok(RewriteResult::InPlace)
    }

    /// Rewrite plan doesn't do anything.
    pub fn no_op(&self) -> bool {
        self.stmt.is_none() && self.prepares.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_apply_bind_no_unique_ids() {
        unsafe {
            std::env::set_var("NODE_ID", "pgdog-1");
        }
        let plan = RewritePlan::default();
        let mut bind = Bind::default();
        plan.apply_bind(&mut bind).unwrap();
        assert_eq!(bind.params_raw().len(), 0);
    }

    #[test]
    fn test_apply_bind_text_format() {
        unsafe {
            std::env::set_var("NODE_ID", "pgdog-1");
        }
        let plan = RewritePlan {
            unique_ids: 1,
            ..Default::default()
        };
        let mut bind = Bind::default();
        plan.apply_bind(&mut bind).unwrap();
        assert_eq!(bind.params_raw().len(), 1);

        // Default format is Text, so data should be a string
        let param = &bind.params_raw()[0];
        let text = std::str::from_utf8(&param.data).unwrap();
        let _id: i64 = text.parse().expect("should be valid i64 text");

        // No format codes needed for all-text
        assert_eq!(bind.format_codes_raw().len(), 0);
    }

    #[test]
    fn test_apply_bind_binary_format_uniform() {
        unsafe {
            std::env::set_var("NODE_ID", "pgdog-1");
        }
        let plan = RewritePlan {
            params: 1,
            unique_ids: 1,
            ..Default::default()
        };
        // Create bind with uniform binary format (1 code applies to all)
        let mut bind =
            Bind::new_params_codes("test", &[Parameter::new(b"existing")], &[Format::Binary]);
        plan.apply_bind(&mut bind).unwrap();
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

    #[test]
    fn test_apply_bind_binary_format_one_to_one() {
        unsafe {
            std::env::set_var("NODE_ID", "pgdog-1");
        }
        let plan = RewritePlan {
            params: 2,
            unique_ids: 1,
            ..Default::default()
        };
        // Create bind with one-to-one format codes
        let mut bind = Bind::new_params_codes(
            "test",
            &[Parameter::new(b"a"), Parameter::new(b"b")],
            &[Format::Binary, Format::Binary],
        );
        plan.apply_bind(&mut bind).unwrap();
        assert_eq!(bind.params_raw().len(), 3);

        // New param should be text (default for one-to-one)
        let param = &bind.params_raw()[2];
        let text = std::str::from_utf8(&param.data).unwrap();
        let _: i64 = text.parse().expect("should be valid i64 text");

        // Format code added for new param
        assert_eq!(bind.format_codes_raw().len(), 3);
        assert_eq!(bind.format_codes_raw()[2], Format::Text);
    }

    #[test]
    fn test_apply_bind_multiple_unique_ids() {
        unsafe {
            std::env::set_var("NODE_ID", "pgdog-1");
        }
        let plan = RewritePlan {
            unique_ids: 3,
            ..Default::default()
        };
        let mut bind = Bind::default();
        plan.apply_bind(&mut bind).unwrap();
        assert_eq!(bind.params_raw().len(), 3);

        let mut ids = HashSet::new();
        for param in bind.params_raw() {
            let text = std::str::from_utf8(&param.data).unwrap();
            let id: i64 = text.parse().expect("should be valid i64");
            ids.insert(id);
        }
        assert_eq!(ids.len(), 3, "all IDs should be unique");
    }

    #[test]
    fn test_apply_bind_appends_to_existing_params() {
        unsafe {
            std::env::set_var("NODE_ID", "pgdog-1");
        }
        let plan = RewritePlan {
            params: 2,
            unique_ids: 2,
            ..Default::default()
        };
        let mut bind = Bind::new_params(
            "test",
            &[Parameter::new(b"existing1"), Parameter::new(b"existing2")],
        );
        plan.apply_bind(&mut bind).unwrap();
        assert_eq!(bind.params_raw().len(), 4);

        assert_eq!(bind.params_raw()[0].data.as_ref(), b"existing1");
        assert_eq!(bind.params_raw()[1].data.as_ref(), b"existing2");

        let text = std::str::from_utf8(&bind.params_raw()[2].data).unwrap();
        let _: i64 = text.parse().expect("should be valid i64");
    }
}
