use crate::net::{FrontendPid, Parameters};

use super::Guard;
use std::ops::{Deref, DerefMut};

/// Postgres connection with link state,
/// allowing the calls to [`Self::link_client`] to be idempotent.
#[derive(Debug)]
pub(crate) struct LinkedServer {
    // Postgres connection.
    pub(super) server: Guard,
    // Shard number.
    pub(super) shard: usize,
    // Parameters were sync'ed.
    pub(super) linked: bool,
}

impl LinkedServer {
    /// Link server to client. This is idempotent.
    pub(super) async fn link_client(
        &mut self,
        id: FrontendPid,
        params: &Parameters,
        transaction_stmt: Option<&str>,
    ) -> Result<usize, super::Error> {
        if self.linked {
            return Ok(0);
        }

        let params = self
            .server
            .link_client(id, params, transaction_stmt)
            .await?;

        self.linked = true;

        Ok(params)
    }
}

impl Deref for LinkedServer {
    type Target = Guard;

    fn deref(&self) -> &Self::Target {
        &self.server
    }
}

impl DerefMut for LinkedServer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.server
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::{backend::Pool, net::Parameter};

    pub(crate) struct TestLinkedServer {
        pub(crate) server: Option<LinkedServer>,
        pool: Pool,
    }

    impl Drop for TestLinkedServer {
        fn drop(&mut self) {
            self.pool.shutdown();
        }
    }

    impl Deref for TestLinkedServer {
        type Target = LinkedServer;

        fn deref(&self) -> &Self::Target {
            self.server.as_ref().unwrap()
        }
    }

    impl DerefMut for TestLinkedServer {
        fn deref_mut(&mut self) -> &mut Self::Target {
            self.server.as_mut().unwrap()
        }
    }

    impl TestLinkedServer {
        pub(crate) async fn new(shard: usize) -> TestLinkedServer {
            let pool = Pool::new_test();
            pool.launch();

            let server = pool.get_test().await.unwrap();

            let server = LinkedServer {
                server,
                shard,
                linked: false,
            };

            TestLinkedServer {
                pool,
                server: Some(server),
            }
        }
    }

    #[tokio::test]
    async fn test_link_client_idempotent() {
        let mut link = TestLinkedServer::new(0).await;

        let pid = FrontendPid::new();
        let params = Parameters::from(vec![Parameter::from((
            "application_name".to_string(),
            "test_link_client_idempotent".to_string(),
        ))]);

        let linked = link.link_client(pid, &params, None).await.unwrap();

        assert_eq!(linked, 1);

        let linked = link.link_client(pid, &params, None).await.unwrap();

        assert_eq!(linked, 0);
    }
}
