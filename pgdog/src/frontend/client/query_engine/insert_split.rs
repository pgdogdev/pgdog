use super::*;
use crate::{
    backend::pool::{connection::binding::Binding, Guard, Request},
    frontend::router::{self as router, parser::rewrite::InsertSplitPlan},
    net::{
        messages::Protocol, BindComplete, CloseComplete, CommandComplete, ErrorResponse, NoData,
        ParameterDescription, ParseComplete, ReadyForQuery,
    },
};
use tracing::{trace, warn};

impl QueryEngine {
    pub(super) async fn insert_split(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        plan: InsertSplitPlan,
    ) -> Result<(), Error> {
        if !context.client_request.executable() {
            return self.send_split_parse_ack(context).await;
        }

        if context.in_transaction() {
            return self.send_split_transaction_error(context, &plan).await;
        }

        let cluster = self.backend.cluster()?.clone();
        let shards = plan.shard_list().to_vec();
        let use_two_pc = cluster.two_pc_enabled() && shards.len() > 1;

        let request = Request::default();
        let mut guards = Vec::with_capacity(shards.len());

        for shard in &shards {
            let mut guard = cluster
                .primary(*shard, &request)
                .await
                .map_err(|err| Error::Router(router::Error::Pool(err)))?;
            guard.execute("BEGIN").await?;
            guards.push(guard);
        }

        if let Err(err) = self.execute_split_plan(&plan, &shards, &mut guards).await {
            self.rollback_split(&plan, &shards, &mut guards, "execute")
                .await;
            return Err(err);
        }

        if use_two_pc {
            if let Err(err) = self
                .finish_split_two_pc(&cluster, &plan, &shards, &mut guards)
                .await
            {
                self.rollback_split(&plan, &shards, &mut guards, "2pc")
                    .await;
                return Err(err);
            }
        } else if let Err(err) = self.commit_split(&mut guards).await {
            self.rollback_split(&plan, &shards, &mut guards, "commit")
                .await;
            return Err(err);
        }

        self.send_insert_complete(context, plan.total_rows(), use_two_pc)
            .await
    }

    async fn execute_split_plan(
        &mut self,
        plan: &InsertSplitPlan,
        shards: &[usize],
        guards: &mut [Guard],
    ) -> Result<(), Error> {
        for (index, shard) in shards.iter().enumerate() {
            if let Some(values) = plan.values_sql_for_shard(*shard) {
                let sql = if plan.columns().is_empty() {
                    format!("INSERT INTO {} VALUES {}", plan.table(), values)
                } else {
                    format!(
                        "INSERT INTO {} ({}) VALUES {}",
                        plan.table(),
                        plan.columns().join(", "),
                        values
                    )
                };
                trace!(table = %plan.table(), shard, %sql, "executing split insert on shard");
                guards[index].execute(&sql).await?;
            }
        }
        Ok(())
    }

    async fn commit_split(&mut self, guards: &mut [Guard]) -> Result<(), Error> {
        for guard in guards.iter_mut() {
            guard.execute("COMMIT").await?;
        }
        Ok(())
    }

    async fn finish_split_two_pc(
        &mut self,
        cluster: &crate::backend::pool::cluster::Cluster,
        plan: &InsertSplitPlan,
        shards: &[usize],
        guards: &mut [Guard],
    ) -> Result<(), Error> {
        let identifier = cluster.identifier();
        let transaction_name = self.two_pc.transaction().to_string();
        let guard_phase_one = self.two_pc.phase_one(&identifier).await?;

        if let Err(err) =
            Binding::two_pc_on_guards(guards, &transaction_name, TwoPcPhase::Phase1).await
        {
            self.rollback_split(plan, shards, guards, "2pc_prepare")
                .await;
            drop(guard_phase_one);
            return Err(err.into());
        }

        let guard_phase_two = self.two_pc.phase_two(&identifier).await?;
        if let Err(err) =
            Binding::two_pc_on_guards(guards, &transaction_name, TwoPcPhase::Phase2).await
        {
            self.rollback_split(plan, shards, guards, "2pc_commit")
                .await;
            drop(guard_phase_two);
            drop(guard_phase_one);
            return Err(err.into());
        }

        self.two_pc.done().await?;

        drop(guard_phase_two);
        drop(guard_phase_one);
        Ok(())
    }

    async fn rollback_split(
        &self,
        plan: &InsertSplitPlan,
        shards: &[usize],
        guards: &mut [Guard],
        stage: &str,
    ) {
        for (index, shard) in shards.iter().enumerate() {
            if let Some(guard) = guards.get_mut(index) {
                if let Err(err) = guard.execute("ROLLBACK").await {
                    warn!(
                        table = %plan.table(),
                        shard = shard,
                        stage,
                        error = %err,
                        "failed to rollback split insert transaction"
                    );
                }
            }
        }
    }

    async fn send_split_transaction_error(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        plan: &InsertSplitPlan,
    ) -> Result<(), Error> {
        let mut error = ErrorResponse::default();
        error.code = "25001".into();
        error.message = format!(
            "multi-row insert rewrites must run outside explicit transactions (table {})",
            plan.table()
        );

        let bytes_sent = context
            .stream
            .error(error, context.in_transaction())
            .await?;
        self.stats.sent(bytes_sent);
        self.stats.error();
        self.stats.idle(context.in_transaction());
        Ok(())
    }

    async fn send_insert_complete(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        rows: usize,
        two_pc: bool,
    ) -> Result<(), Error> {
        let command_tag = format!("INSERT 0 {}", rows);

        let extended = context
            .client_request
            .iter()
            .any(|message| matches!(message.code(), 'P' | 'B' | 'E' | 'S'));

        let bytes_sent = if extended {
            let mut replies = Vec::new();
            for message in context.client_request.iter() {
                match message.code() {
                    'P' => replies.push(ParseComplete.message()?),
                    'B' => replies.push(BindComplete.message()?),
                    'D' => {
                        replies.push(ParameterDescription::empty().message()?);
                        replies.push(NoData.message()?);
                    }
                    'H' => (),
                    'E' => {
                        replies.push(CommandComplete::from_str(&command_tag).message()?.backend())
                    }
                    'C' => replies.push(CloseComplete.message()?),
                    'S' => replies
                        .push(ReadyForQuery::in_transaction(context.in_transaction()).message()?),
                    c => return Err(Error::UnexpectedMessage(c)),
                }
            }

            context.stream.send_many(&replies).await?
        } else {
            context
                .stream
                .send_many(&[
                    CommandComplete::from_str(&command_tag).message()?.backend(),
                    ReadyForQuery::in_transaction(context.in_transaction()).message()?,
                ])
                .await?
        };
        self.stats.sent(bytes_sent);
        self.stats.query();
        self.stats.idle(context.in_transaction());
        if !context.in_transaction() {
            self.stats.transaction(two_pc);
        }
        Ok(())
    }

    async fn send_split_parse_ack(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<(), Error> {
        let mut replies = Vec::new();
        for message in context.client_request.iter() {
            match message.code() {
                'P' => replies.push(ParseComplete.message()?),
                'D' => {
                    replies.push(ParameterDescription::empty().message()?);
                    replies.push(NoData.message()?);
                }
                'H' => (),
                'C' => replies.push(CloseComplete.message()?),
                'S' => {
                    replies.push(ReadyForQuery::in_transaction(context.in_transaction()).message()?)
                }
                c => return Err(Error::UnexpectedMessage(c)),
            }
        }

        let bytes_sent = context.stream.send_many(&replies).await?;
        self.stats.sent(bytes_sent);
        self.stats.idle(context.in_transaction());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::QueryEngineContext;
    use super::*;
    use crate::{
        config,
        frontend::{
            client::{Client, TransactionType},
            router::parser::{
                rewrite::{InsertSplitPlan, InsertSplitRow},
                route::Route,
                table::OwnedTable,
                Shard,
            },
        },
        net::{
            messages::{Bind, Describe, Execute, Parse, Query, Sync},
            Stream,
        },
    };
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn sample_plan() -> InsertSplitPlan {
        InsertSplitPlan::new(
            Route::write(Shard::Multi(vec![0, 1])),
            OwnedTable {
                name: "sharded".into(),
                ..Default::default()
            },
            vec!["\"id\"".into()],
            vec![
                InsertSplitRow::new(0, vec!["1".into()]),
                InsertSplitRow::new(1, vec!["11".into()]),
            ],
        )
    }

    fn test_client() -> Client {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        Client::new_test(Stream::dev_null(), addr)
    }

    #[tokio::test]
    async fn parse_ack_sent_for_non_executable_split() {
        config::load_test();

        let mut client = test_client();
        client
            .client_request
            .messages
            .push(Parse::named("split", "INSERT INTO sharded (id) VALUES (1, 11)").into());
        client.client_request.messages.push(Sync::new().into());

        let mut engine = QueryEngine::from_client(&client).unwrap();
        let mut context = QueryEngineContext::new(&mut client);
        let plan = sample_plan();

        assert!(!context.client_request.executable());

        engine.insert_split(&mut context, plan).await.unwrap();

        assert!(engine.stats().bytes_sent > 0);
        assert_eq!(engine.stats().errors, 0);
    }

    #[tokio::test]
    async fn split_insert_rejected_inside_transaction() {
        config::load_test();

        let mut client = test_client();
        client.transaction = Some(TransactionType::ReadWrite);
        client
            .client_request
            .messages
            .push(Query::new("INSERT INTO sharded (id) VALUES (1), (11)").into());

        let mut engine = QueryEngine::from_client(&client).unwrap();
        let mut context = QueryEngineContext::new(&mut client);
        let plan = sample_plan();

        assert!(context.client_request.executable());
        assert!(context.in_transaction());

        engine.insert_split(&mut context, plan).await.unwrap();

        assert!(engine.stats().bytes_sent > 0);
        assert_eq!(engine.stats().errors, 1);
    }

    #[tokio::test]
    async fn send_insert_complete_simple_flow() {
        config::load_test();

        let mut client = test_client();
        client
            .client_request
            .messages
            .push(Query::new("INSERT INTO sharded (id) VALUES (1)").into());

        let mut engine = QueryEngine::default();
        let mut context = QueryEngineContext::new(&mut client);

        engine
            .send_insert_complete(&mut context, 1, false)
            .await
            .expect("simple insert complete should succeed");

        let stats = engine.stats();
        assert_eq!(stats.queries, 1);
        assert!(stats.bytes_sent > 0);
        assert_eq!(stats.transactions_2pc, 0);
    }

    #[tokio::test]
    async fn send_insert_complete_extended_flow() {
        config::load_test();

        let mut client = test_client();
        client.client_request.messages.extend_from_slice(&[
            Parse::named("multi", "INSERT INTO sharded (id, value) VALUES ($1, $2)").into(),
            Bind::new_statement("multi").into(),
            Describe::new_statement("multi").into(),
            Execute::new().into(),
            Sync::new().into(),
        ]);

        let mut engine = QueryEngine::default();
        let mut context = QueryEngineContext::new(&mut client);

        engine
            .send_insert_complete(&mut context, 2, true)
            .await
            .expect("extended insert complete should succeed");

        let stats = engine.stats();
        assert_eq!(stats.queries, 1);
        assert!(stats.bytes_sent > 0);
        assert_eq!(stats.transactions, 1);
        assert_eq!(stats.transactions_2pc, 1);
    }
}
