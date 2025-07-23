pub mod slot;
pub use slot::*;
pub mod copy;
pub mod progress;
pub mod publisher_impl;
pub mod queries;
pub mod table;
pub use copy::*;
pub use queries::*;
pub use table::*;

#[cfg(test)]
mod test {

    use crate::backend::{server::test::test_replication_server, Server};

    pub struct PublicationTest {
        pub server: Server,
    }

    impl PublicationTest {
        pub async fn cleanup(&mut self) {
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

    pub async fn setup_publication() -> PublicationTest {
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
