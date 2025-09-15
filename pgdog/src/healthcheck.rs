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

use crate::backend::databases::databases;

pub async fn server(port: u16) -> std::io::Result<()> {
    info!("healthcheck endpoint http://0.0.0.0:{}", port);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await?;

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);

        tokio::task::spawn(async move {
            if let Err(err) = http1::Builder::new()
                .serve_connection(io, service_fn(healthcheck))
                .await
            {
                eprintln!("Healthcheck endpoint error: {:?}", err);
            }
        });
    }
}

async fn healthcheck(
    _: Request<hyper::body::Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let databases = databases();
    let broken = databases.all().iter().all(|(_, cluster)| {
        let pools = cluster
            .shards()
            .iter()
            .map(|shard| shard.pools())
            .collect::<Vec<_>>()
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        pools.iter().all(|p| p.banned())
    });

    let response = if broken { "down" } else { "up" };
    let status = if broken { 502 } else { 200 };

    let response = Response::builder()
        .header(hyper::header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .status(status)
        .body(Full::new(Bytes::from(response)))
        .unwrap_or_else(|_| Response::new(Full::new(Bytes::from("Healthcheck unavailable"))));

    Ok(response)
}
