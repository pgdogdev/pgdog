use std::ops::{Deref, DerefMut};

use super::{
    Error,
    insert::{UnnestArray, group_split_requests_by_shard, group_unnest_insert_by_shard},
};
use crate::frontend::{ClientRequest, PreparedStatements, router::parser::Shard};
use crate::net::Message;

#[derive(Debug, Clone)]
pub(crate) struct InsertSplitRewriteResult {
    pub(super) requests: Vec<ClientRequest>,
    pub(super) tuples: Option<Vec<Vec<usize>>>,
    pub(super) unnest_arrays: Option<Vec<UnnestArray>>,
}

impl Deref for InsertSplitRewriteResult {
    type Target = Vec<ClientRequest>;

    fn deref(&self) -> &Self::Target {
        &self.requests
    }
}

impl DerefMut for InsertSplitRewriteResult {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.requests
    }
}

impl InsertSplitRewriteResult {
    /// Create a split result from requests executed in tuple order.
    pub(crate) fn new(requests: Vec<ClientRequest>) -> Self {
        Self {
            requests,
            tuples: None,
            unnest_arrays: None,
        }
    }

    pub(crate) fn new_unnest_insert(
        single_row_requests: Vec<ClientRequest>,
        unnest_arrays: Vec<UnnestArray>,
    ) -> Self {
        Self {
            requests: single_row_requests,
            tuples: None,
            unnest_arrays: Some(unnest_arrays),
        }
    }

    /// Send one multi-tuple INSERT per shard.
    /// Requests must be routed first.
    pub(crate) fn group_by_shard(
        &mut self,
        request: &ClientRequest,
        prepared_statements: &mut PreparedStatements,
    ) -> Result<(), Error> {
        let requests = std::mem::take(&mut self.requests);

        *self = match self.unnest_arrays.take() {
            Some(unnest_arrays) => group_unnest_insert_by_shard(requests, request, unnest_arrays)?,
            None => group_split_requests_by_shard(requests, request, prepared_statements)?,
        };

        Ok(())
    }

    pub(crate) fn row_buffer(&self) -> Option<Vec<Vec<Message>>> {
        self.tuples
            .as_ref()
            .map(|tuples| tuples.iter().map(|_| Vec::new()).collect())
    }

    /// Reorder rows returned by each request into the original tuple order.
    pub(crate) fn rows_in_tuple_order(&self, rows: Vec<Vec<Message>>) -> Vec<Message> {
        let Some(tuples) = &self.tuples else {
            return rows.into_iter().flatten().collect();
        };

        let matched = rows
            .iter()
            .zip(tuples)
            .all(|(rows, tuples)| rows.len() == tuples.len());

        if !matched {
            return rows.into_iter().flatten().collect();
        }

        let mut ordered: Vec<(usize, Message)> = rows
            .into_iter()
            .zip(tuples)
            .flat_map(|(rows, tuples)| rows.into_iter().zip(tuples.iter().copied()))
            .map(|(row, tuple)| (tuple, row))
            .collect();

        ordered.sort_by_key(|(tuple, _)| *tuple);

        ordered.into_iter().map(|(_, row)| row).collect()
    }

    /// Check that the insert split actually targets
    /// the same shard.
    pub(crate) fn same_shard(&self) -> Option<Shard> {
        if let Some(first) = self
            .requests
            .first()
            .and_then(|request| request.route.as_ref().map(|route| route.shard()))
        {
            if !first.is_direct() {
                return None;
            }
            if self
                .requests
                .iter()
                .skip(1)
                .all(|req| req.route.as_ref().map(|r| r.shard()) == Some(first))
            {
                return Some(first.clone());
            }
        }

        None
    }
}
