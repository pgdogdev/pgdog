use crate::{
    frontend::BufferedQuery,
    net::{FrontendPid, Parameters},
};
use std::ops::{Deref, DerefMut};

use super::*;

/// Direct-to-shard (single server) connection binding.
#[derive(Debug)]
pub(crate) struct DirectBinding {
    // Postgres server connection.
    pub(super) server: LinkedServer,
    // Transaction start statement. Used for upgrades to `MultiShard` binding,
    // making it start a transaction on newly connected servers.
    pub(super) transaction_stmt: Option<BufferedQuery>,
    // Read/write intent.
    pub(super) is_read: bool,
}

impl DirectBinding {
    /// Create new direct-to-shard binding.
    pub(super) fn new(
        server: Guard,
        shard: usize,
        transaction_stmt: Option<BufferedQuery>,
        is_read: bool,
    ) -> Self {
        Self {
            server: LinkedServer {
                server,
                shard,
                linked: false,
            },
            transaction_stmt,
            is_read,
        }
    }

    /// Link client to server. This is idempotent.
    pub(super) async fn link_client(
        &mut self,
        id: FrontendPid,
        params: &Parameters,
    ) -> Result<usize, Error> {
        let start_transaction = self.transaction_stmt.as_ref().map(|q| q.query());

        let params = self
            .server
            .link_client(id, params, start_transaction)
            .await?;

        Ok(params)
    }
}

impl Deref for DirectBinding {
    type Target = LinkedServer;

    fn deref(&self) -> &Self::Target {
        &self.server
    }
}

impl DerefMut for DirectBinding {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.server
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::linked_server::test::TestLinkedServer;
    use super::*;

    pub(crate) struct TestDirectBinding {
        pub(crate) binding: Option<DirectBinding>,
        #[allow(unused)] // For its `Drop` trait.
        link: TestLinkedServer,
    }

    impl TestDirectBinding {
        pub(crate) async fn new(in_transaction: bool, shard: usize) -> Self {
            let mut link = TestLinkedServer::new(shard).await;
            let server = link.server.take().unwrap();

            let binding = DirectBinding {
                server,
                transaction_stmt: if in_transaction {
                    Some(BufferedQuery::Query(Query::new("BEGIN")))
                } else {
                    None
                },
                is_read: false,
            };

            Self {
                binding: Some(binding),
                link,
            }
        }
    }

    #[tokio::test]
    async fn test_link() {
        let mut binding = TestDirectBinding::new(true, 0)
            .await
            .binding
            .take()
            .unwrap();

        binding
            .link_client(FrontendPid::new(), &Parameters::default())
            .await
            .unwrap();

        assert!(binding.server.in_transaction());
        assert!(binding.server.in_sync());
    }
}
