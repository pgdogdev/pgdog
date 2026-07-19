use tracing::{info, warn};

use super::QueryEngineContext;
use crate::config::config;
use crate::net::ProtocolMessage;
use crate::util::{sanitize_log_sample, user_database_from_params};

pub(super) fn log_query_stdout(context: &QueryEngineContext<'_>) {
    let size_limit = context.query_size_limit;

    // Largest query message in the request, when it exceeds the limit.
    // The limit protects the query parser, so only messages carrying SQL
    // count.
    let oversize = size_limit.and_then(|size_limit| {
        context
            .client_request
            .messages
            .iter()
            .filter(|m| matches!(m, ProtocolMessage::Query(_) | ProtocolMessage::Parse(_)))
            .map(|m| m.len())
            .max()
            .filter(|&size| size > size_limit)
    });

    if !context.query_log_stdout && oversize.is_none() {
        return;
    }

    let Ok(Some(query)) = context.client_request.query() else {
        return;
    };

    let one_line = sanitize_log_sample(
        query.query(),
        config().config.general.log_query_sample_length,
    );
    let one_line = one_line.trim();

    let (user, database) = user_database_from_params(context.params);

    if let Some(size) = oversize
        && let Some(size_limit) = size_limit
    {
        warn!(
            "[large_query] size={}B query_size_limit={}B '{}...' [database: {}, user: {}]",
            size, size_limit, one_line, database, user,
        );
    } else if context.query_log_stdout {
        info!("{} [database: {}, user: {}]", one_line, database, user);
    }
}
