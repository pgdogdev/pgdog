//! Replay DDL run on the source on the destination shards.
//!
//! Source db writes the DDL to the source's WAL with the `pgdog.log_ddl_to_wal()` event trigger
//! (see `setup.sql`).
//!
//! We receive the DDL in the replication stream as a logical message.

use aws_lc_rs::digest::{SHA256, digest};
use serde::Deserialize;
use tracing::warn;

use super::super::Error;
use crate::backend::pool::Request;
use crate::backend::{Cluster, Error as BackendError};
use crate::config::config;
use crate::util::{constant_time_eq, escape_identifier};

/// We only look at logical messages with this prefix.
/// This is the prefix the event trigger uses when writing DDL to the WAL.
pub(super) const MESSAGE_PREFIX: &str = "ddl_replication";

/// Postgres error code we get when our user isn't allowed to read a table
const PERMISSION_DENIED: &str = "42501";

/// The event trigger writes the signature in front of the payload.
/// Signature is a sha256 hash in hex (thus, always 64 characters long)
const SIGNATURE_LEN: usize = 64;

/// Read the key the source signs the DDL with.
/// Also check that the event trigger is installed.
const KEY_QUERY: &str = "SELECT k.key FROM pgdog.ddl_replication_key k \
    WHERE EXISTS (SELECT FROM pg_event_trigger WHERE evtname = 'pgdog_ddl_replication')";

/// This is the payload the event trigger writes to the WAL for each DDL statement (as JSON)
#[derive(Debug, Deserialize)]
pub(super) struct DdlMessage {
    role: String,
    search_path: String,
    query: String,
}

impl DdlMessage {
    /// We run these statements on each destination shard to replay the DDL.
    /// The statements run inside the transaction being replicated, so that the DDL
    /// is applied in order with the rows.
    pub(super) fn statements_to_replay(&self) -> [String; 5] {
        let function = "pg_temp.pgdog_replay_ddl()";

        [
            // We set the search_path of the session that ran the DDL, so that unqualified
            // names (e.g., `users` in `ALTER TABLE users`) resolve to the same schema as on the
            // source. The search_path we set here only lasts for the transaction.
            format!(
                "SELECT set_config('search_path', {}, true)",
                quote(&self.search_path)
            ),
            // We put the DDL in a temporary function owned by the role that ran the DDL
            // on the source, so that new objects get the same owner.
            //
            // Why? The function is SECURITY DEFINER, so the DDL can't `SET ROLE` to PgDog's user
            // (Postgres forbids `SET ROLE` in a SECURITY DEFINER function)
            format!(
                "CREATE FUNCTION {function} RETURNS void LANGUAGE sql SECURITY DEFINER AS {}",
                quote(&self.query)
            ),
            format!(
                "ALTER FUNCTION {function} OWNER TO \"{}\"",
                escape_identifier(&self.role)
            ),
            // We run the function (so the DDL runs as that role), then drop the function.
            format!("SELECT {function}"),
            format!("DROP FUNCTION {function}"),
        ]
    }
}

/// Turn text into an SQL string literal, e.g., `it's` becomes `'it''s'`.
/// similar to `quote_literal()`
fn quote(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// We use this to check the DDL messages we receive from the source, and to get the statements
/// to replay out of the messages.
#[derive(Debug, Clone)]
pub(crate) struct DdlReplay {
    /// This is the key the source uses to sign DDL with.
    /// We use the key to check the signature of each message.
    key: String,
}

impl DdlReplay {
    /// We read the key from the source (if `resharding_replay_ddl` is enabled).
    pub(crate) async fn load(source: &Cluster) -> Result<Option<Self>, Error> {
        // Do nothing if replaying DDL is disabled
        if !config().config.general.resharding_replay_ddl {
            return Ok(None);
        }

        // We don't support a source with more than one shard yet (future PR in stack)
        if source.shards().len() > 1 {
            return Err(Error::DdlReplicationMultiShardSource);
        }

        // Read the key from the source
        let mut source_primary = source.primary(0, &Request::default()).await?;
        let key = match source_primary.fetch_all::<String>(KEY_QUERY).await {
            Ok(mut rows) => rows.pop(),
            Err(BackendError::ExecutionError(error)) if error.code == PERMISSION_DENIED => {
                let user = source_primary.addr().user.clone();
                return Err(Error::DdlReplicationKeyUnreadable(user));
            }
            Err(error) => return Err(error.into()),
        };

        let Some(key) = key else {
            return Err(Error::DdlReplicationNotInstalled);
        };

        Ok(Some(Self { key }))
    }

    /// We get the DDL out of a logical message (if DDL replay message)
    pub(super) fn parse_message(&self, message: &[u8]) -> Result<Option<DdlMessage>, Error> {
        // Check the signature. This is necessary as any role can write a message
        // to the WAL, but only our event trigger has the correct key to sign the message with.
        let signed_payload =
            message
                .split_at_checked(SIGNATURE_LEN)
                .filter(|(signature, payload)| {
                    constant_time_eq(self.signature(payload).as_bytes(), signature)
                });

        let Some((_, payload)) = signed_payload else {
            warn!("ignoring DDL in the WAL that wasn't written by pgdog.log_ddl_to_wal()");
            return Ok(None);
        };

        let ddl_message: DdlMessage = serde_json::from_slice(payload)?;

        // Skip a query with several statements. This is necessary as the event
        // trigger fires once per DDL statement, with the whole query string every time
        if has_multiple_statements(&ddl_message.query) {
            warn!(
                "DDL sent with other statements in one query isn't replicated, \
                 apply the DDL on the destination manually: {}",
                ddl_message.query
            );
            return Ok(None);
        }

        Ok(Some(ddl_message))
    }

    /// Sign the payload with same fmt as the event trigger
    /// `sha256(key || sha256(key || payload))` (in hex)
    fn signature(&self, payload: &[u8]) -> String {
        let key_bytes = self.key.as_bytes();
        let inner_hash = digest(&SHA256, &[key_bytes, payload].concat());
        let outer_hash = digest(&SHA256, &[key_bytes, inner_hash.as_ref()].concat());

        hex::encode(outer_hash)
    }
}

/// We check whether the query string has more than one statement.
fn has_multiple_statements(query: &str) -> bool {
    pg_raw_parse::parse(query)
        .is_ok_and(|statements| statements.into_inner().into_iter().count() > 1)
}

#[cfg(test)]
mod test {
    use super::*;

    const SOURCE_KEY: &str = "source-key";

    fn ddl_replay() -> DdlReplay {
        DdlReplay {
            key: SOURCE_KEY.into(),
        }
    }

    fn message_from_trigger(ddl_replay: &DdlReplay, query: &str) -> Vec<u8> {
        let payload = serde_json::json!({
            "role": "app_owner",
            "search_path": "app, public",
            "query": query,
        })
        .to_string();

        [ddl_replay.signature(payload.as_bytes()), payload]
            .concat()
            .into_bytes()
    }

    #[test]
    fn signature_matches_postgres() {
        // SELECT encode(sha256(convert_to('source-key', 'UTF8')
        //     || sha256(convert_to('source-key' || 'payload', 'UTF8'))), 'hex')
        assert_eq!(
            ddl_replay().signature(b"payload"),
            "56d3729ec487845a56625ec58a3c527f09358c4e4b602c00ad93930504ee343b"
        );
    }

    #[test]
    fn skips_query_with_multiple_statements() {
        let ddl_replay = ddl_replay();
        let message = message_from_trigger(
            &ddl_replay,
            "CREATE INDEX users_name ON users (name); CREATE INDEX users_email ON users (email)",
        );
        assert!(ddl_replay.parse_message(&message).unwrap().is_none());
    }

    #[test]
    fn replays_ddl_as_role_with_search_path() {
        let ddl_message = DdlMessage {
            role: "app_owner".into(),
            search_path: "app, public".into(),
            query: "CREATE TABLE users (name TEXT DEFAULT 'anonymous')".into(),
        };

        let statements = ddl_message.statements_to_replay();
        assert_eq!(
            statements[0],
            "SELECT set_config('search_path', 'app, public', true)"
        );
        assert_eq!(
            statements[1],
            "CREATE FUNCTION pg_temp.pgdog_replay_ddl() RETURNS void LANGUAGE sql SECURITY DEFINER \
             AS 'CREATE TABLE users (name TEXT DEFAULT ''anonymous'')'"
        );
        assert_eq!(
            statements[2],
            "ALTER FUNCTION pg_temp.pgdog_replay_ddl() OWNER TO \"app_owner\""
        );
        assert_eq!(statements[3], "SELECT pg_temp.pgdog_replay_ddl()");
        assert_eq!(statements[4], "DROP FUNCTION pg_temp.pgdog_replay_ddl()");
    }
}
