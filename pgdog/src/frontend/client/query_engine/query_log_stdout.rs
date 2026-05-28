use tracing::info;

use super::QueryEngineContext;
use crate::util::user_database_from_params;

pub(super) fn log_query_stdout(context: &QueryEngineContext<'_>) {
    if !context.query_log_stdout {
        return;
    }

    if let Ok(Some(query)) = context.client_request.query() {
        let (user, database) = user_database_from_params(context.params);
        let query_one_line = query.query().replace(['\r', '\n'], " ");
        info!(
            "{} [database: {}, user: {}]",
            query_one_line.trim(),
            database,
            user
        );
    }
}
