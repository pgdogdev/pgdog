use frontend::listener::Listener;
use tracing::Level;

pub mod auth;
pub mod backend;
pub mod channel;
pub mod frontend;
pub mod net;
pub mod state;

// pub mod plugin;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .init();

    let mut listener = Listener::new("0.0.0.0:6432");
    listener.listen().await.unwrap();
    println!("Hello, world!");
}
