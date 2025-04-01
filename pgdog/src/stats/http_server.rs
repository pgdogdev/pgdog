use std::convert::Infallible;
use std::net::SocketAddr;

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tracing::info;

use super::{Clients, Pools};

async fn metrics(_: Request<hyper::body::Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    let clients = Clients::load();
    let pools = Pools::load();
    Ok(Response::new(Full::new(Bytes::from(
        clients.to_string() + "\n" + &pools.to_string(),
    ))))
}

pub async fn server(port: u16) -> std::io::Result<()> {
    info!("OpenMetrics endpoint http://0.0.0.0:{}", port);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await?;

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);

        tokio::task::spawn(async move {
            if let Err(err) = http1::Builder::new()
                .serve_connection(io, service_fn(metrics))
                .await
            {
                eprintln!("OpenMetrics endpoint error: {:?}", err);
            }
        });
    }
}
