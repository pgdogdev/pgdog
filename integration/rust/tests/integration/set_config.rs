use crate::setup::connections_sqlx;
use sqlx::query_scalar;
use std::assert_matches;

#[tokio::test]
async fn test_set_config_behaves_like_set() {
    let pool = connections_sqlx().await.pop().unwrap();
    let mut conn = pool.acquire().await.unwrap();
    let set_config: String = query_scalar("SELECT set_config('lock_timeout', '1000s', false);")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(set_config, "1000s");
    let lock_timeout: String = query_scalar("SHOW lock_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(lock_timeout, "1000s");

    let set_config: String = query_scalar("SELECT set_config('lock_timeout', '500s', false);")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(set_config, "500s");
    let lock_timeout: String = query_scalar("SHOW lock_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(lock_timeout, "500s");

    let set_config: Option<String> =
        query_scalar("SELECT set_config('lock_timeout', NULL, false);")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
    assert_eq!(set_config, None);

    let lock_timeout: String = query_scalar("SHOW lock_timeout")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(lock_timeout, "0");
}

#[tokio::test]
// We can't handle every possible invocation yet, but we at least shouldn't
// error
async fn test_set_config_does_something_when_unable_to_resolve_args() {
    let pool = connections_sqlx().await.pop().unwrap();
    let mut conn = pool.acquire().await.unwrap();

    let set_config: Result<String, _> =
        query_scalar("SELECT set_config('lock_timeout', (SELECT '1'), false);")
            .fetch_one(&mut *conn)
            .await;
    assert_matches!(set_config, Ok(_));
}
