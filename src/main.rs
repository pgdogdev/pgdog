use frontend::listener::Listener;

pub mod backend;
pub mod frontend;
pub mod net;
// pub mod plugin;

#[tokio::main]
async fn main() {
    let mut listener = Listener::new("0.0.0.0:6432");
    listener.listen().await.unwrap();
    println!("Hello, world!");
}
