use rust::setup::connections_sqlx;
use sqlx::Executor;

#[tokio::test]
async fn test_savepoint() {
    let conns = connections_sqlx().await;

    for conn in conns {
        let mut transaction = conn.begin().await.unwrap();
        transaction
            .execute("CREATE TABLE test_savepoint (id BIGINT)")
            .await
            .unwrap(); //p//p
        transaction.execute("SAVEPOINT test").await.unwrap();
        assert!(transaction.execute("SELECT sdfsf").await.is_err());
        transaction
            .execute("ROLLBACK TO SAVEPOINT test")
            .await
            .unwrap();
        transaction
            .execute("SELECT * FROM test_savepoint")
            .await
            .unwrap();
        transaction.rollback().await.unwrap();
    }
}
