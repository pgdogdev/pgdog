use backend::databases::databases;
use frontend::listener::Listener;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

pub mod admin;
pub mod auth;
pub mod backend;
pub mod channel;
pub mod frontend;
pub mod net;
pub mod state;
pub mod stats;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    // Preload TLS.
    net::tls::load()?;

    // Load databases and connect if needed.
    databases();

    let mut listener = Listener::new("0.0.0.0:6432");
    listener.listen().await?;

    Ok(())
}
