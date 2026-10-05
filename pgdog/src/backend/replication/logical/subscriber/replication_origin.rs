//! Replication origin on a destination shard.
//!
//! Allow to track the LSN progress from source to destination shard -
//! every transaction on dest will be tied with the source LSN.
//! If transaction is committed then related source LSN is also committed.
//!
//! See [docs](https://www.postgresql.org/docs/current/replication-origins.html)
//! and [reference](https://www.postgresql.org/docs/current/functions-admin.html#PG-REPLICATION-ORIGIN-XACT-SETUP)

use std::time::Duration;

use tracing::{debug, error, info, warn};

use super::super::Error;
use super::connect_address;
use crate::{
    backend::{Error as BackendError, Server, pool::Address},
    net::{Bind, Format, Parse, messages::bind::Parameter, replication::logical::commit::Commit},
    util::{
        retry::{Retry, RetryConfig},
        sql::quote_literal,
    },
};
use pgdog_stats::Lsn;

const XACT_SETUP_NAME: &str = "__pgdog_repl_origin_xact";

/// How many times to retry while another backend holds the origin, and how
/// long to wait between attempts.
const RETRY_ATTEMPTS: usize = 10;
const RETRY_DELAY: Duration = Duration::from_millis(100);

/// Replication origin tracking one source stream on one destination shard.
#[derive(Debug, Clone)]
pub(crate) struct ReplicationOrigin {
    /// name of the origin, quoted as an SQL literal
    name: String,
    /// destination address
    address: Address,
    /// attempts before giving up for origin used in another session
    retry_attempts: usize,
    /// delay between attempts
    retry_delay: Duration,
}

#[bon::bon]
impl ReplicationOrigin {
    /// Origin for the stream of replication slot `name` on destination
    /// `shard`. The same slot always maps to the same origin.
    #[builder]
    pub(crate) fn new(
        name: &str,
        address: &Address,
        shard: usize,
        #[builder(default = RETRY_ATTEMPTS)] retry_attempts: usize,
        #[builder(default = RETRY_DELAY)] retry_delay: Duration,
    ) -> Self {
        Self {
            name: quote_literal(&format!("__pgdog_origin_{name}_{shard}")),
            address: address.clone(),
            retry_attempts,
            retry_delay,
        }
    }

    #[cfg(test)]
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// Create the origin if it does not exist.
    pub(crate) async fn create_origin(&self) -> Result<(), Error> {
        let mut server = connect_address(&self.address).await?;
        let created = self.create_if_missing(&mut server).await?;

        if created {
            info!(
                origin = %self.name,
                addr = %self.address,
                "[replication] origin created"
            );
        } else {
            info!(
                origin = %self.name,
                addr = %self.address,
                "[replication] using existing origin"
            );
        }

        Ok(())
    }

    /// Drop the origin and create it again, so the progress of an earlier
    /// run is not reused.
    pub(crate) async fn recreate_origin(&self) -> Result<(), Error> {
        self.drop_if_exists().await?;
        self.create_origin().await
    }

    /// Drop the origin. Waits for a short time while a closing session
    /// still holds it. Warns when the origin was already gone.
    pub(crate) async fn drop_origin(&self) -> Result<(), Error> {
        if self.drop_if_exists().await? {
            info!(
                origin = %self.name,
                addr = %self.address,
                "[replication] origin dropped"
            );
        } else {
            warn!(
                origin = %self.name,
                addr = %self.address,
                "[replication] origin to drop does not exist"
            );
        }

        Ok(())
    }

    /// Drop the origin if it exists. `true` if it was dropped.
    async fn drop_if_exists(&self) -> Result<bool, Error> {
        let mut server = connect_address(&self.address).await?;
        let name = &self.name;
        let dropped = self
            .fetch_with_retry(
                &mut server,
                &format!(
                    "SELECT pg_replication_origin_drop({name})::text \
                     WHERE pg_replication_origin_oid({name}) IS NOT NULL"
                ),
            )
            .await?;

        Ok(!dropped.is_empty())
    }

    /// Setup the origin on this session, creating it if it is missing.
    /// Should be called once at the start of replication session.
    pub(crate) async fn setup_session(&self, server: &mut Server) -> Result<(), Error> {
        self.create_if_missing(server).await?;
        self.fetch_with_retry(
            server,
            &format!(
                "SELECT pg_replication_origin_session_setup({})::text",
                self.name
            ),
        )
        .await?;

        debug!(
            origin = %self.name,
            addr = %server.addr(),
            "[replication] origin bound to session"
        );

        Ok(())
    }

    /// Get the source LSN of the last transaction committed on the destination.
    /// With `force_flush`, the destination WAL is flushed up to that commit
    /// first, so the returned LSN is durable. Without it, the commit may not
    /// be on disk yet. `0/0` if no transaction was committed.
    pub(crate) async fn progress(
        &self,
        server: &mut Server,
        force_flush: bool,
    ) -> Result<Lsn, Error> {
        let progress: Vec<String> = server
            .fetch_all(format!(
                "SELECT COALESCE(pg_replication_origin_progress({}, {force_flush}), '0/0')::text",
                self.name
            ))
            .await
            .map_err(|error| self.map_error(error))?;

        let progress = progress.first().ok_or(Error::MissingData)?;
        Ok(progress.parse::<Lsn>()?)
    }

    /// Parse statement for transaction setup.
    pub(crate) fn xact_setup_parse() -> Parse {
        Parse::named(
            XACT_SETUP_NAME,
            "SELECT pg_replication_origin_xact_setup($1::pg_lsn, $2::timestamptz), pg_current_xact_id()",
        )
    }

    /// Bind statement for transaction setup. When called current transaction
    /// will be tied with the provided source LSN.
    pub(crate) fn xact_setup_bind(commit: &Commit) -> Bind {
        Bind::new_params_codes(
            XACT_SETUP_NAME,
            &[
                Parameter::new(&commit.end_lsn.to_be_bytes()),
                Parameter::new(&commit.commit_timestamp.to_be_bytes()),
            ],
            &[Format::Binary],
        )
    }

    /// Create the origin on `server` if it does not exist. `true` if created.
    async fn create_if_missing(&self, server: &mut Server) -> Result<bool, Error> {
        let name = &self.name;
        let rows: Vec<String> = server
            .fetch_all(format!(
                "SELECT pg_replication_origin_create({name})::text \
                 WHERE pg_replication_origin_oid({name}) IS NULL"
            ))
            .await
            .map_err(|error| self.map_error(error))?;

        Ok(!rows.is_empty())
    }

    /// Run the query, retrying while another session holds the origin
    /// up to [`ReplicationOrigin::retry_attempts`] times.
    async fn fetch_with_retry(
        &self,
        server: &mut Server,
        query: &str,
    ) -> Result<Vec<String>, Error> {
        let mut retry = Retry::new(
            RetryConfig::builder()
                .name(format!("[replication] origin {}", self.name))
                .max_attempts(self.retry_attempts)
                .delay(self.retry_delay)
                .build(),
        );

        loop {
            let err = match server.fetch_all(query).await {
                Ok(rows) => return Ok(rows),
                Err(error) => self.map_error(error),
            };

            if !matches!(err, Error::ReplicationOriginInUse(_)) || !retry.delay_retry(&err).await {
                return Err(err);
            }
        }
    }

    fn map_error(&self, error: BackendError) -> Error {
        match &error {
            BackendError::ExecutionError(response) if response.code == "42501" => {
                Error::ReplicationOriginPermissionDenied {
                    user: self.address.user.clone(),
                    database: self.address.database_name.clone(),
                    source: Box::new(error),
                }
            }
            BackendError::ExecutionError(response) if response.code == "55006" => {
                Error::ReplicationOriginInUse(self.name.clone())
            }
            _ => {
                error!(
                    origin = %self.name,
                    addr = %self.address,
                    "[replication] origin error: {error}"
                );
                error.into()
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        backend::server::test::test_server,
        net::{ErrorResponse, Execute, FromBytes, Protocol, ProtocolMessage, Sync, ToBytes},
    };

    use super::*;
    use tokio::time::sleep;

    struct Test {
        origin: ReplicationOrigin,
        admin: Server,
        track: Server,
        table: String,
    }

    impl Test {
        /// Create the origin and the table, after removing leftovers of an
        /// earlier run.
        async fn new(name: &str) -> Self {
            let mut admin = test_server().await;
            let origin = ReplicationOrigin::builder()
                .name(&format!("test_origin_{name}"))
                .address(admin.addr())
                .shard(0)
                .retry_attempts(4)
                .build();
            origin.drop_origin().await.unwrap();
            origin.create_origin().await.unwrap();

            let table = format!("public.test_replication_origin_{name}");
            admin
                .execute_checked(format!(
                    "DROP TABLE IF EXISTS {table}; CREATE TABLE {table} (id BIGINT)"
                ))
                .await
                .unwrap();

            Self {
                origin,
                admin,
                track: test_server().await,
                table,
            }
        }

        /// Apply connection as the stream opens it: `synchronous_commit` set,
        /// origin bound, origin statement prepared.
        async fn apply_session(&mut self, synchronous_commit: &str) -> Server {
            let mut server = test_server().await;
            server
                .execute_checked(format!("SET synchronous_commit TO {synchronous_commit}"))
                .await
                .unwrap();
            self.origin.setup_session(&mut server).await.unwrap();

            let prepare: Vec<ProtocolMessage> =
                vec![ReplicationOrigin::xact_setup_parse().into(), Sync.into()];
            Self::send(&mut server, prepare).await.unwrap();

            let pid = "SELECT pg_backend_pid()";
            let apply: Vec<i32> = server.fetch_all(pid).await.unwrap();
            let track: Vec<i32> = self.track.fetch_all(pid).await.unwrap();
            assert_ne!(
                apply, track,
                "progress must be tracked from another backend"
            );

            server
        }

        /// Commit one transaction marked with `end_lsn`, the way the apply
        /// connections do: pipelined, the statement first, the origin
        /// statement last, committed by `Sync`. Without `statement`, the
        /// transaction only commits.
        async fn apply(&self, session: &mut Server, end_lsn: i64, statement: Option<&str>) -> Lsn {
            self.try_apply(session, end_lsn, statement).await.unwrap()
        }

        async fn try_apply(
            &self,
            session: &mut Server,
            end_lsn: i64,
            statement: Option<&str>,
        ) -> Result<Lsn, ErrorResponse> {
            let commit = Commit {
                flags: 0,
                commit_lsn: end_lsn - 8,
                end_lsn,
                commit_timestamp: 812_345_678_901_234,
            };
            let mut messages: Vec<ProtocolMessage> = vec![];
            if let Some(statement) = statement {
                messages.push(Parse::new_anonymous(statement).into());
                messages.push(Bind::new_statement("").into());
                messages.push(Execute::new().into());
            }
            messages.push(ReplicationOrigin::xact_setup_bind(&commit).into());
            messages.push(Execute::new().into());
            messages.push(Sync.into());
            Self::send(session, messages).await?;

            Ok(Lsn::from_i64(end_lsn))
        }

        async fn send(
            server: &mut Server,
            messages: Vec<ProtocolMessage>,
        ) -> Result<(), ErrorResponse> {
            server.send(&messages.into()).await.unwrap();
            let mut error = None;
            loop {
                let message = server.read().await.unwrap();
                match message.code() {
                    'E' => error = Some(ErrorResponse::from_bytes(message.to_bytes()).unwrap()),
                    'Z' => return error.map_or(Ok(()), Err),
                    _ => (),
                }
            }
        }

        /// Origin progress read on the tracking connection.
        async fn progress(&mut self, flushed: bool) -> Lsn {
            self.origin
                .progress(&mut self.track, flushed)
                .await
                .unwrap()
        }

        async fn assert_progress(&mut self, lsn: Lsn, flushed_at_commit: bool) {
            let local = self.local_lsn().await;
            if flushed_at_commit {
                assert!(self.flush_lsn().await >= local, "commit is flushed");
            }
            assert_eq!(self.progress(false).await, lsn);
            assert_eq!(self.progress(true).await, lsn);
            assert!(self.flush_lsn().await >= local, "progress(true) must flush");
        }

        /// End of the last commit made through the origin, on the destination.
        async fn local_lsn(&mut self) -> Lsn {
            self.track_lsn(&format!(
                "SELECT local_lsn::text FROM pg_replication_origin_status WHERE external_id = {}",
                self.origin.name()
            ))
            .await
        }

        async fn flush_lsn(&mut self) -> Lsn {
            self.track_lsn("SELECT pg_current_wal_flush_lsn()::text")
                .await
        }

        async fn track_lsn(&mut self, query: &str) -> Lsn {
            let rows: Vec<String> = self.track.fetch_all(query).await.unwrap();
            rows[0].parse().unwrap()
        }

        async fn rows_count(&mut self) -> i64 {
            let rows: Vec<i64> = self
                .admin
                .fetch_all(format!("SELECT count(*) FROM {}", self.table))
                .await
                .unwrap();
            rows[0]
        }

        async fn origins_count(&mut self) -> i64 {
            let rows: Vec<i64> = self
                .admin
                .fetch_all(format!(
                    "SELECT count(*) FROM pg_replication_origin WHERE roname = {}",
                    self.origin.name()
                ))
                .await
                .unwrap();
            rows[0]
        }

        async fn finish(&mut self) {
            self.admin
                .execute_checked(format!("DROP TABLE {}", self.table))
                .await
                .unwrap();
            self.origin.drop_origin().await.unwrap();
        }
    }

    #[test]
    fn name_follows_the_slot_name_and_destination_shard() {
        let origin = ReplicationOrigin::builder()
            .name("resume_2")
            .address(&Address::new_test())
            .shard(1)
            .build();
        assert_eq!(origin.name(), "'__pgdog_origin_resume_2_1'");
    }

    #[tokio::test]
    async fn create_and_drop_are_idempotent() {
        let mut test = Test::new("lifecycle").await;
        test.origin.create_origin().await.unwrap();
        test.origin.create_origin().await.unwrap();
        assert_eq!(test.origins_count().await, 1);
        assert_eq!(test.progress(true).await, Lsn::default());

        test.origin.drop_origin().await.unwrap();
        test.origin.drop_origin().await.unwrap();
        assert_eq!(test.origins_count().await, 0);
        test.finish().await;
    }

    #[tokio::test]
    async fn drop_waits_for_the_session_to_close() {
        let mut test = Test::new("drop_wait").await;
        let session = test.apply_session("on").await;

        let origin = ReplicationOrigin {
            retry_attempts: 50,
            ..test.origin.clone()
        };
        let dropping = tokio::spawn(async move { origin.drop_origin().await });
        sleep(test.origin.retry_delay * 3).await;
        assert!(!dropping.is_finished(), "drop must wait for the session");
        assert_eq!(test.origins_count().await, 1);

        drop(session);
        dropping.await.unwrap().unwrap();
        assert_eq!(test.origins_count().await, 0);
        test.finish().await;
    }

    #[tokio::test]
    async fn session_setup_fails_while_another_session_holds_the_origin() {
        let mut test = Test::new("in_use").await;
        let holder = test.apply_session("on").await;

        let mut other = test_server().await;
        let error = test.origin.setup_session(&mut other).await.unwrap_err();
        assert!(
            matches!(&error, Error::ReplicationOriginInUse(name) if name == test.origin.name())
        );
        assert!(error.is_retryable());

        drop(holder);
        test.finish().await;
    }

    #[tokio::test]
    async fn failed_transaction_keeps_the_progress() {
        let mut test = Test::new("failed_xact").await;
        let mut session = test.apply_session("on").await;

        let insert = format!("INSERT INTO {} VALUES (1)", test.table);
        let committed = test.apply(&mut session, 0x1_0000_1000, Some(&insert)).await;

        let failing = format!("INSERT INTO {} VALUES (1 / 0)", test.table);
        let error = test
            .try_apply(&mut session, 0x1_0000_2000, Some(&failing))
            .await
            .unwrap_err();
        assert_eq!(error.code, "22012");
        test.assert_progress(committed, false).await;

        let lsn = test.apply(&mut session, 0x1_0000_3000, Some(&insert)).await;
        test.assert_progress(lsn, true).await;
        assert_eq!(test.rows_count().await, 2);

        drop(session);
        test.finish().await;
    }

    #[tokio::test]
    async fn constraint_error_keeps_the_progress() {
        let mut test = Test::new("unique").await;
        test.admin
            .execute_checked(format!("ALTER TABLE {} ADD UNIQUE (id)", test.table))
            .await
            .unwrap();
        let mut session = test.apply_session("on").await;

        let insert = format!("INSERT INTO {} VALUES (1)", test.table);
        let committed = test.apply(&mut session, 0x1_0000_1000, Some(&insert)).await;

        let error = test
            .try_apply(&mut session, 0x1_0000_2000, Some(&insert))
            .await
            .unwrap_err();
        assert_eq!(error.code, "23505");
        test.assert_progress(committed, false).await;
        assert_eq!(test.rows_count().await, 1);

        drop(session);
        test.finish().await;
    }

    /// With `on` a commit with data is flushed by the commit itself. A commit
    /// without data has a transaction ID but wrote no WAL before its commit
    /// record, so PostgreSQL commits it asynchronously even with `on`: only
    /// the progress is tracked, durability needs the flush.
    #[tokio::test]
    async fn sync_commit_on() {
        let mut test = Test::new("sync_commit_on").await;
        let mut session = test.apply_session("on").await;

        let insert = format!("INSERT INTO {} VALUES (1), (2)", test.table);
        let lsn = test.apply(&mut session, 0x1_0000_1000, Some(&insert)).await;
        test.assert_progress(lsn, true).await;

        let lsn = test.apply(&mut session, 0x1_0000_2000, None).await;
        test.assert_progress(lsn, false).await;
        assert_eq!(test.rows_count().await, 2);

        drop(session);
        test.finish().await;
    }

    /// With `off` the commit may stay in WAL buffers. Only the progress read
    /// with a flush makes it durable.
    #[tokio::test]
    async fn sync_commit_off() {
        let mut test = Test::new("sync_commit_off").await;
        let mut session = test.apply_session("off").await;

        let insert = format!("INSERT INTO {} VALUES (1), (2)", test.table);
        let lsn = test.apply(&mut session, 0x1_0000_1000, Some(&insert)).await;
        test.assert_progress(lsn, false).await;

        let lsn = test.apply(&mut session, 0x1_0000_2000, None).await;
        test.assert_progress(lsn, false).await;
        assert_eq!(test.rows_count().await, 2);

        drop(session);
        test.finish().await;
    }

    #[tokio::test]
    async fn unprivileged_user_gets_permission_error() {
        let user = "pgdog_test_origin_denied";
        let mut test = Test::new("denied").await;
        test.admin
            .execute(format!("DROP ROLE IF EXISTS {user}"))
            .await
            .unwrap();
        test.admin
            .execute_checked(format!(
                "CREATE ROLE {user} LOGIN NOSUPERUSER NOINHERIT PASSWORD 'pgdog'; \
                 GRANT CONNECT ON DATABASE pgdog TO {user}"
            ))
            .await
            .unwrap();

        let address = Address {
            user: user.into(),
            passwords: vec!["pgdog".into()],
            ..Address::new_test()
        };
        let result = ReplicationOrigin::builder()
            .name("test_origin_denied")
            .address(&address)
            .shard(0)
            .build()
            .create_origin()
            .await;

        test.admin
            .execute_checked(format!(
                "REVOKE CONNECT ON DATABASE pgdog FROM {user}; DROP ROLE {user}"
            ))
            .await
            .unwrap();
        test.finish().await;

        let error = result.unwrap_err();
        assert!(!error.is_retryable());
        assert!(matches!(
            &error,
            Error::ReplicationOriginPermissionDenied { user, source, .. }
                if user == "pgdog_test_origin_denied"
                    && matches!(source.as_ref(), BackendError::ExecutionError(response) if response.code == "42501")
        ));
    }
}
