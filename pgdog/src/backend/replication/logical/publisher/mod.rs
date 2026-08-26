pub(crate) mod non_identity_columns_presence;
pub(crate) use non_identity_columns_presence::*;

pub(crate) mod slot;
pub(crate) use slot::*;
pub(crate) mod copy;
pub(crate) mod parallel_sync;
pub(crate) mod progress;
pub(crate) mod publisher_impl;
pub(crate) mod queries;
pub(crate) mod table;
pub(crate) use copy::*;
pub(crate) use parallel_sync::ParallelSyncManager;
pub(crate) use queries::*;
pub(crate) use table::*;

#[cfg(test)]
pub(crate) mod test {

    use crate::backend::{Server, server::test::test_replication_server};

    pub(crate) struct PublicationTest {
        pub(crate) server: Server,
    }

    impl PublicationTest {
        pub(crate) async fn cleanup(&mut self) {
            self.server
                .execute("DROP PUBLICATION IF EXISTS publication_test")
                .await
                .unwrap();
            self.server
                .execute("DROP TABLE IF EXISTS publication_test_two")
                .await
                .unwrap();
            self.server
                .execute("DROP TABLE IF EXISTS publication_test_one")
                .await
                .unwrap();
        }
    }

    pub(crate) async fn setup_publication() -> PublicationTest {
        let mut server = test_replication_server().await;

        server.execute("CREATE TABLE IF NOT EXISTS publication_test_one (id BIGSERIAL PRIMARY KEY, email VARCHAR NOT NULL)").await.unwrap();
        server.execute("CREATE TABLE IF NOT EXISTS publication_test_two (id BIGSERIAL PRIMARY KEY, fk_id BIGINT NOT NULL)").await.unwrap();

        for i in 0..25 {
            server
                .execute(format!(
                    "INSERT INTO publication_test_one (email) VALUES ('test_{}@test.com')",
                    i
                ))
                .await
                .unwrap();

            server
                .execute(format!(
                    "INSERT INTO publication_test_two (fk_id) VALUES ({})",
                    i
                ))
                .await
                .unwrap();
        }
        server
            .execute("DROP PUBLICATION IF EXISTS publication_test")
            .await
            .unwrap();
        server.execute("CREATE PUBLICATION publication_test FOR TABLE publication_test_one, publication_test_two").await.unwrap();

        PublicationTest { server }
    }
}
