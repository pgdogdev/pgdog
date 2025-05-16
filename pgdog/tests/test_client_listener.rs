use pgdog::frontend::listener::*;
use tokio::select;
use tokio::{spawn, sync::mpsc::*};

async fn client(host: &str) -> Sender<()> {
    let (tx, mut rx) = channel(1);

    let mut listener = Listener::new("127.0.0.1:6666");

    spawn(async move {
        select! {
            _ = rx.recv() => {
                listener.shutdown();
            }
            _ = listener.listen() => (),
        }
    });

    tx
}

#[tokio::test]
async fn test_basic_conn() {
    let listener = client("127.0.0.1:6666").await;
}
