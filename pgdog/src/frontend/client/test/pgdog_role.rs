use std::{ops::Deref, time::Duration};

use pgdog_config::{PoolerMode, ReadWriteSplit, ReadWriteStrategy, Role};
use tokio::time::timeout;

use crate::{
    backend::databases::{reload_from_existing, shutdown},
    config::{config, load_test_replicas, set},
    expect_message,
    net::{
        Bind, CommandComplete, CopyData, CopyDone, DataRow, ErrorResponse, Execute, Message,
        Parameters, Parse, Protocol, Query, ReadyForQuery, Sync,
    },
};

use super::test_client::TestClient;

/// `load_test_replicas` has one primary and one replica with round-robin
/// load balancing. With `include_primary`, two consecutive unforced reads
/// reach both targets, so a read that may land on the primary does.
const ATTEMPTS: usize = 2;

const READ_ONLY_QUERY: &str =
    "SELECT (current_setting('default_transaction_read_only') = 'on')::int";
const SET_ROLE_AFTER_CONNECT: &str =
    "cannot use \"SET pgdog.role\" after connecting to a server; set it before running any queries";
const READ_ONLY_SQLSTATE: &str = "25006";

const COMMENT_PRIMARY: &str = "/* pgdog_role: primary */";
const COMMENT_REPLICA: &str = "/* pgdog_role: replica */";
const COMMENT_AUTO: &str = "/* pgdog_role: auto */";

const SET_PRIMARY: &str = "SET pgdog.role TO 'primary'";
const SET_REPLICA: &str = "SET pgdog.role TO 'replica'";
const SET_AUTO: &str = "SET pgdog.role TO 'auto'";
const RESET_ROLE: &str = "RESET pgdog.role";

#[derive(bon::Builder)]
struct RoleTest {
    #[builder(into)]
    table: String,
    user_role: Option<Role>,
    #[builder(default)]
    user_read_only: bool,
    #[builder(default)]
    pooler_mode: PoolerMode,
    #[builder(default)]
    strategy: ReadWriteStrategy,
    #[builder(default)]
    split: ReadWriteSplit,
}

impl RoleTest {
    fn configure(&self, user_read_only: bool) {
        load_test_replicas();

        let mut config = config().deref().clone();
        for user in &mut config.users.users {
            user.role = self.user_role;
            user.read_only = user_read_only.then_some(true);
        }
        config.config.general.pooler_mode = self.pooler_mode;
        config.config.general.read_write_split = self.split;
        config.config.general.read_write_strategy = self.strategy;
        set(config).unwrap();
        reload_from_existing().unwrap();
    }

    async fn run(self) -> RoleEnv {
        self.configure(false);

        let table = self.table.as_str();
        let mut primary = new_client(Some("primary")).await;
        for sql in [
            format!("CREATE TABLE IF NOT EXISTS {table}(id BIGINT)"),
            format!("DELETE FROM {table}"),
            format!("INSERT INTO {table} VALUES (1)"),
        ] {
            execute(&mut primary, &sql).await.unwrap();
        }

        if self.user_read_only {
            self.configure(true);
        }

        RoleEnv {
            table: self.table,
            _primary: primary,
        }
    }
}

struct RoleEnv {
    table: String,
    _primary: TestClient,
}

impl Drop for RoleEnv {
    fn drop(&mut self) {
        shutdown();
    }
}

#[bon::bon]
impl RoleEnv {
    #[builder(finish_fn = connect)]
    async fn client(
        &self,
        startup_role: Option<&'static str>,
        #[builder(default)] session: Vec<&'static str>,
        #[builder(default)] comment: &'static str,
    ) -> RoleClient {
        let mut client = new_client(startup_role).await;
        for sql in &session {
            execute(&mut client, sql).await.unwrap();
        }

        RoleClient {
            label: format!("startup={startup_role:?} session={session:?} comment={comment:?}"),
            table: self.table.clone(),
            comment,
            client,
        }
    }
}

struct RoleClient {
    label: String,
    table: String,
    comment: &'static str,
    client: TestClient,
}

impl RoleClient {
    async fn execute(&mut self, sql: &str) -> Result<Vec<Message>, ErrorResponse> {
        execute(&mut self.client, sql).await
    }

    async fn read_only(&mut self) -> bool {
        let sql = format!("{} {READ_ONLY_QUERY}", self.comment);
        first_value_is_one(self.execute(&sql).await.unwrap())
    }

    async fn read_only_extended(&mut self) -> bool {
        first_value_is_one(
            execute_extended(&mut self.client, READ_ONLY_QUERY)
                .await
                .unwrap(),
        )
    }

    fn count_query(&self) -> String {
        format!(
            "{} SELECT (count(*) = 1 AND min(id) = 1)::int FROM {}",
            self.comment, self.table
        )
    }

    fn update(&self) -> String {
        format!("{} UPDATE {} SET id = 1", self.comment, self.table)
    }

    fn writes(&self) -> [String; 3] {
        let (comment, table) = (self.comment, &self.table);
        [
            format!("{comment} INSERT INTO {table} VALUES (2)"),
            format!("{comment} UPDATE {table} SET id = 2"),
            format!("{comment} DELETE FROM {table}"),
        ]
    }

    async fn assert_on_replica(&mut self) {
        assert!(
            self.read_only().await,
            "{}: read went to the primary",
            self.label
        );
    }

    async fn assert_on_primary(&mut self) {
        assert!(
            !self.read_only().await,
            "{}: read went to a replica",
            self.label
        );
    }

    async fn assert_read(&mut self) {
        let sql = self.count_query();
        let messages = self.execute(&sql).await.expect("read failed");
        assert!(
            first_value_is_one(messages),
            "{}: read returned wrong data",
            self.label
        );
    }

    async fn assert_write(&mut self) {
        let sql = self.update();
        if let Err(error) = self.execute(&sql).await {
            panic!("{}: write went to a replica: {}", self.label, error.message);
        }
    }

    async fn assert_write_rejected(&mut self, sql: &str) {
        let result = self.execute(sql).await;
        assert_rejected_read_only(&self.label, result, sql);
    }

    async fn assert_unchanged(&mut self) {
        let sql = format!(
            "SELECT (count(*) = 1 AND min(id) = 1)::int FROM {}",
            self.table
        );
        let messages = self.execute(&sql).await.unwrap();
        assert!(
            first_value_is_one(messages),
            "{}: data changed in {}",
            self.label,
            self.table
        );
    }

    async fn assert_set_role_rejected(&mut self, sql: &str) {
        let error = self.execute(sql).await.unwrap_err();
        assert_eq!(error.message, SET_ROLE_AFTER_CONNECT, "{}", self.label);
    }

    async fn begin(&mut self, begin: &str, prelude: &[&str]) {
        self.execute(begin).await.unwrap();
        for statement in prelude {
            self.execute(statement).await.unwrap();
        }
    }

    async fn rollback(&mut self) {
        self.execute("ROLLBACK").await.unwrap();
    }

    async fn assert_replica(&mut self) {
        for _ in 0..ATTEMPTS {
            self.assert_on_replica().await;
            self.assert_read().await;
        }

        for sql in self.writes() {
            for _ in 0..ATTEMPTS {
                self.assert_write_rejected(&sql).await;
            }
        }

        self.assert_unchanged().await;
    }

    async fn assert_primary(&mut self) {
        for _ in 0..ATTEMPTS {
            self.assert_on_primary().await;
            self.assert_write().await;
        }
    }

    /// With `exclude_primary`, reads use the replica and writes use the primary
    /// only when no role is forced.
    async fn assert_statement_decides(&mut self) {
        for _ in 0..ATTEMPTS {
            self.assert_on_replica().await;
            self.assert_write().await;
        }
    }

    async fn assert_load_balanced(&mut self) {
        let mut targets = Vec::new();
        for _ in 0..ATTEMPTS {
            targets.push(self.read_only().await);
        }
        assert_both_targets(&self.label, &targets);

        for _ in 0..ATTEMPTS {
            self.assert_read().await;
            self.assert_write().await;
        }
    }

    async fn assert_load_balanced_extended(&mut self) {
        let mut targets = Vec::new();
        for _ in 0..ATTEMPTS {
            targets.push(self.read_only_extended().await);
        }
        assert_both_targets(&self.label, &targets);

        for _ in 0..ATTEMPTS {
            let sql = self.update();
            execute_extended(&mut self.client, &sql)
                .await
                .expect("write went to a replica");
        }
    }

    async fn assert_replica_extended(&mut self) {
        for sql in self.writes() {
            for _ in 0..ATTEMPTS {
                let result = execute_extended(&mut self.client, &sql).await;
                assert_rejected_read_only(&self.label, result, &sql);
            }
        }

        self.assert_unchanged().await;
    }

    async fn assert_primary_extended(&mut self) {
        for _ in 0..ATTEMPTS {
            assert!(
                !self.read_only_extended().await,
                "{}: read went to a replica",
                self.label
            );
            let sql = self.update();
            execute_extended(&mut self.client, &sql)
                .await
                .expect("write went to a replica");
        }
    }

    async fn assert_replica_copy(&mut self) {
        let sql = format!("COPY {} FROM STDIN", self.table);
        for _ in 0..ATTEMPTS {
            let result = timeout(Duration::from_secs(5), self.execute(&sql))
                .await
                .expect("COPY went to the primary");
            assert_rejected_read_only(&self.label, result, &sql);
        }

        self.assert_unchanged().await;
    }

    async fn assert_primary_copy(&mut self) {
        self.client
            .send_simple(Query::new(format!("COPY {} FROM STDIN", self.table)))
            .await;
        self.client
            .read_until('G')
            .await
            .expect("COPY went to a replica");
        self.client.send(CopyData::new(b"1\n")).await;
        self.client.send(CopyDone).await;
        self.client.try_process().await.unwrap();
        self.client.read_until('Z').await.unwrap();
    }

    async fn assert_replica_in_transaction(&mut self, prelude: &[&str]) {
        for sql in self.writes() {
            for _ in 0..ATTEMPTS {
                self.begin("BEGIN", prelude).await;
                self.assert_read().await;
                self.assert_write_rejected(&sql).await;
                self.rollback().await;
            }
        }

        self.assert_unchanged().await;
    }

    async fn assert_primary_in_transaction(&mut self, prelude: &[&str]) {
        for _ in 0..ATTEMPTS {
            self.begin("BEGIN", prelude).await;
            self.assert_on_primary().await;
            self.assert_write().await;
            self.rollback().await;
        }
    }

    async fn assert_read_only_transaction(&mut self, prelude: &[&str], replica: bool) {
        for _ in 0..ATTEMPTS {
            self.begin("BEGIN READ ONLY", prelude).await;
            assert_eq!(
                self.read_only().await,
                replica,
                "{}: wrong target",
                self.label
            );
            self.assert_read().await;
            self.rollback().await;
        }
    }

    async fn assert_read_only_transaction_load_balanced(&mut self) {
        let mut targets = Vec::new();
        for _ in 0..ATTEMPTS {
            self.begin("BEGIN READ ONLY", &[]).await;
            targets.push(self.read_only().await);
            self.assert_read().await;
            self.rollback().await;
        }
        assert_both_targets(&self.label, &targets);
    }
}

async fn new_client(startup_role: Option<&str>) -> TestClient {
    let mut params = Parameters::default();
    if let Some(role) = startup_role {
        params.insert("pgdog.role", role);
    }

    TestClient::new(params).await.leak_pool()
}

async fn execute(client: &mut TestClient, sql: &str) -> Result<Vec<Message>, ErrorResponse> {
    client.send_simple(Query::new(sql)).await;
    finish(client).await
}

async fn execute_extended(
    client: &mut TestClient,
    sql: &str,
) -> Result<Vec<Message>, ErrorResponse> {
    client.send(Parse::new_anonymous(sql)).await;
    client.send(Bind::new_statement("")).await;
    client.send(Execute::new()).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();
    finish(client).await
}

async fn finish(client: &mut TestClient) -> Result<Vec<Message>, ErrorResponse> {
    let result = client.read_until('Z').await;
    if result.is_err() {
        expect_message!(client.read().await, ReadyForQuery);
    }
    result
}

fn first_value_is_one(messages: Vec<Message>) -> bool {
    let row = messages
        .into_iter()
        .find(|message| message.code() == 'D')
        .expect("data row");
    let row = expect_message!(row, DataRow);
    row.get_int(0, true).unwrap() == 1
}

fn assert_both_targets(label: &str, targets: &[bool]) {
    assert!(
        targets.contains(&true),
        "{label}: reads never used the replica"
    );
    assert!(
        targets.contains(&false),
        "{label}: reads never used the primary"
    );
}

fn assert_rejected_read_only(label: &str, result: Result<Vec<Message>, ErrorResponse>, sql: &str) {
    let error = result.expect_err(&format!("{label}: write went to the primary: {sql}"));
    assert_eq!(
        error.code, READ_ONLY_SQLSTATE,
        "{label}: write failed for another reason: {sql}: {}",
        error.message
    );
}

#[tokio::test]
async fn test_no_role() {
    let env = RoleTest::builder()
        .table("test_no_role")
        .build()
        .run()
        .await;

    let mut client = env.client().connect().await;
    client.assert_load_balanced().await;
    client.assert_load_balanced_extended().await;
    client.assert_read_only_transaction_load_balanced().await;
    client.assert_primary_in_transaction(&[]).await;

    env.client()
        .session(vec![SET_PRIMARY, RESET_ROLE])
        .connect()
        .await
        .assert_load_balanced()
        .await;
}

#[tokio::test]
async fn test_startup_role() {
    let env = RoleTest::builder()
        .table("test_startup_role")
        .build()
        .run()
        .await;

    let mut replica = env.client().startup_role("replica").connect().await;
    replica.assert_replica().await;
    replica.assert_replica_in_transaction(&[]).await;
    replica.assert_replica_extended().await;
    replica.assert_replica_copy().await;

    let mut primary = env.client().startup_role("primary").connect().await;
    primary.assert_primary().await;
    primary.assert_primary_extended().await;
    primary.assert_primary_copy().await;
}

#[tokio::test]
async fn test_set_role() {
    let env = RoleTest::builder()
        .table("test_set_role")
        .build()
        .run()
        .await;

    env.client()
        .session(vec![SET_REPLICA])
        .connect()
        .await
        .assert_replica()
        .await;
    env.client()
        .session(vec![SET_PRIMARY])
        .connect()
        .await
        .assert_primary()
        .await;
    env.client()
        .startup_role("replica")
        .session(vec![SET_PRIMARY])
        .connect()
        .await
        .assert_primary()
        .await;
    env.client()
        .startup_role("replica")
        .session(vec![SET_PRIMARY, RESET_ROLE])
        .connect()
        .await
        .assert_replica()
        .await;
    env.client()
        .startup_role("replica")
        .session(vec![SET_PRIMARY, "RESET ALL"])
        .connect()
        .await
        .assert_replica()
        .await;
}

#[tokio::test]
async fn test_set_local_role() {
    let env = RoleTest::builder()
        .table("test_set_local_role")
        .build()
        .run()
        .await;

    env.client()
        .connect()
        .await
        .assert_replica_in_transaction(&["SET LOCAL pgdog.role TO 'replica'"])
        .await;
    env.client()
        .startup_role("replica")
        .connect()
        .await
        .assert_primary_in_transaction(&["SET LOCAL pgdog.role TO 'primary'"])
        .await;

    let mut client = env.client().startup_role("primary").connect().await;
    for _ in 0..ATTEMPTS {
        client.begin("BEGIN", &[]).await;
        client.assert_on_primary().await;
        client
            .assert_set_role_rejected("SET LOCAL pgdog.role TO 'replica'")
            .await;
        client.rollback().await;
    }

    let insert = format!("INSERT INTO {} VALUES (2)", env.table);
    client.begin("BEGIN", &[insert.as_str()]).await;
    client
        .assert_set_role_rejected("SET LOCAL pgdog.role TO 'replica'")
        .await;
    let commit = client.execute("COMMIT").await.unwrap();
    assert!(
        commit.iter().any(|message| message.code() == 'C'
            && expect_message!(message.clone(), CommandComplete).command() == "ROLLBACK"),
        "COMMIT did not roll back"
    );
    client.assert_unchanged().await;
}

#[tokio::test]
async fn test_comment_role() {
    let env = RoleTest::builder()
        .table("test_comment_role")
        .build()
        .run()
        .await;

    env.client()
        .comment(COMMENT_REPLICA)
        .connect()
        .await
        .assert_replica()
        .await;
    env.client()
        .startup_role("replica")
        .comment(COMMENT_PRIMARY)
        .connect()
        .await
        .assert_primary()
        .await;
    env.client()
        .session(vec![SET_PRIMARY])
        .comment(COMMENT_REPLICA)
        .connect()
        .await
        .assert_replica()
        .await;
    env.client()
        .comment(COMMENT_REPLICA)
        .connect()
        .await
        .assert_primary_in_transaction(&[])
        .await;
}

#[tokio::test]
async fn test_user_role_replica() {
    let env = RoleTest::builder()
        .table("test_user_role_replica")
        .user_role(Role::Replica)
        .build()
        .run()
        .await;

    env.client().connect().await.assert_replica().await;
    env.client()
        .startup_role("invalid")
        .connect()
        .await
        .assert_replica()
        .await;
    env.client()
        .startup_role("primary")
        .connect()
        .await
        .assert_primary()
        .await;
    env.client()
        .session(vec![SET_PRIMARY])
        .connect()
        .await
        .assert_primary()
        .await;
    env.client()
        .session(vec![SET_PRIMARY, RESET_ROLE])
        .connect()
        .await
        .assert_replica()
        .await;
}

#[tokio::test]
async fn test_user_role_primary() {
    let env = RoleTest::builder()
        .table("test_user_role_primary")
        .user_role(Role::Primary)
        .build()
        .run()
        .await;

    env.client().connect().await.assert_primary().await;
    env.client()
        .comment(COMMENT_REPLICA)
        .connect()
        .await
        .assert_replica()
        .await;
}

#[tokio::test]
async fn test_user_role_auto() {
    let env = RoleTest::builder()
        .table("test_user_role_auto")
        .user_role(Role::Auto)
        .build()
        .run()
        .await;

    env.client().connect().await.assert_load_balanced().await;
}

#[tokio::test]
async fn test_auto_role() {
    let env = RoleTest::builder()
        .table("test_auto_role")
        .user_role(Role::Replica)
        .split(ReadWriteSplit::ExcludePrimary)
        .build()
        .run()
        .await;

    env.client()
        .startup_role("auto")
        .connect()
        .await
        .assert_statement_decides()
        .await;
    env.client()
        .session(vec![SET_AUTO])
        .connect()
        .await
        .assert_statement_decides()
        .await;
    env.client()
        .comment(COMMENT_AUTO)
        .connect()
        .await
        .assert_statement_decides()
        .await;
}

#[tokio::test]
async fn test_exclude_primary() {
    let env = RoleTest::builder()
        .table("test_exclude_primary")
        .split(ReadWriteSplit::ExcludePrimary)
        .build()
        .run()
        .await;

    let mut client = env.client().connect().await;
    client.assert_statement_decides().await;
    client
        .assert_read_only_transaction(&["SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"], true)
        .await;
}

#[tokio::test]
async fn test_aggressive_transactions() {
    let env = RoleTest::builder()
        .table("test_aggressive_transactions")
        .strategy(ReadWriteStrategy::Aggressive)
        .split(ReadWriteSplit::ExcludePrimary)
        .build()
        .run()
        .await;

    let mut client = env.client().connect().await;
    client.assert_replica_in_transaction(&[]).await;
    for _ in 0..ATTEMPTS {
        client.begin("BEGIN", &[]).await;
        client.assert_write().await;
        client.assert_on_primary().await;
        client.rollback().await;
    }

    env.client()
        .comment(COMMENT_REPLICA)
        .connect()
        .await
        .assert_replica_in_transaction(&[])
        .await;
}

#[tokio::test]
async fn test_prefer_primary() {
    let env = RoleTest::builder()
        .table("test_prefer_primary")
        .split(ReadWriteSplit::PreferPrimary)
        .build()
        .run()
        .await;

    let mut client = env.client().connect().await;
    client.assert_primary().await;
    client.assert_read_only_transaction(&[], false).await;

    env.client()
        .session(vec![SET_REPLICA])
        .connect()
        .await
        .assert_replica()
        .await;
}

#[tokio::test]
async fn test_set_role_after_first_query_in_session_mode() {
    let env = RoleTest::builder()
        .table("test_set_role_after_first_query_in_session_mode")
        .pooler_mode(PoolerMode::Session)
        .build()
        .run()
        .await;

    let mut client = env
        .client()
        .startup_role("primary")
        .session(vec![SET_REPLICA])
        .connect()
        .await;
    client.assert_on_replica().await;
    client.assert_set_role_rejected(SET_PRIMARY).await;
    client.assert_on_replica().await;
}

#[tokio::test]
async fn test_user_read_only_does_not_force_replica() {
    let env = RoleTest::builder()
        .table("test_user_read_only_does_not_force_replica")
        .user_read_only(true)
        .build()
        .run()
        .await;
    let mut client = env.client().connect().await;

    let in_recovery = "SELECT pg_is_in_recovery()::int";
    for _ in 0..ATTEMPTS {
        let messages = client
            .execute(&format!("{COMMENT_PRIMARY} {in_recovery}"))
            .await
            .unwrap();
        assert!(!first_value_is_one(messages), "read went to a replica");
    }

    let mut targets = Vec::new();
    for _ in 0..ATTEMPTS {
        targets.push(first_value_is_one(
            client.execute(in_recovery).await.unwrap(),
        ));
    }
    assert!(targets.contains(&false), "reads never used the primary");

    let mut forced = env.client().comment(COMMENT_PRIMARY).connect().await;
    for sql in forced.writes() {
        forced.assert_write_rejected(&sql).await;
    }
    forced.assert_unchanged().await;
}
