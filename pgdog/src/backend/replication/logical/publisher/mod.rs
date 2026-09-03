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
        pub(crate) publication: String,
        pub(crate) tables: Vec<String>,
    }

    impl PublicationTest {
        pub(crate) async fn cleanup(&mut self) {
            let drop_publication = format!("DROP PUBLICATION IF EXISTS {}", self.publication);
            self.server.execute(drop_publication).await.unwrap();

            for table in self.tables.iter().rev() {
                let drop_table = format!("DROP TABLE IF EXISTS {}", table);
                self.server.execute(drop_table).await.unwrap();
            }
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

        PublicationTest {
            server,
            publication: "publication_test".into(),
            tables: vec!["publication_test_one".into(), "publication_test_two".into()],
        }
    }

    pub(crate) async fn setup_publication_tables(
        publication: &str,
        tables: &[&str],
    ) -> PublicationTest {
        let mut test = PublicationTest {
            server: test_replication_server().await,
            publication: publication.to_owned(),
            tables: tables.iter().map(|table| table.to_string()).collect(),
        };

        test.cleanup().await;

        for table in &test.tables {
            let create_table =
                format!("CREATE TABLE {} (id BIGINT PRIMARY KEY, value TEXT)", table);
            test.server.execute(create_table).await.unwrap();
        }

        let create_publication = format!(
            "CREATE PUBLICATION {} FOR TABLE {}",
            test.publication,
            test.tables.join(", ")
        );
        test.server.execute(create_publication).await.unwrap();

        test
    }
}
