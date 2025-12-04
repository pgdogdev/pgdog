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

use super::{Clients, MirrorStatsMetrics, Pools, QueryCache, RewriteStatsMetrics};

async fn metrics(_: Request<hyper::body::Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    let clients = Clients::load();
    let pools = Pools::load();
    let mirror_stats: Vec<_> = MirrorStatsMetrics::load()
        .into_iter()
        .map(|m| m.to_string())
        .collect();
    let mirror_stats = mirror_stats.join("\n");
    let query_cache: Vec<_> = QueryCache::load()
        .metrics()
        .into_iter()
        .map(|m| m.to_string())
        .collect();
    let query_cache = query_cache.join("\n");
    let rewrite_stats: Vec<_> = RewriteStatsMetrics::load()
        .into_iter()
        .map(|m| m.to_string())
        .collect();
    let rewrite_stats = rewrite_stats.join("\n");
    let metrics_data = clients.to_string()
        + "\n"
        + &pools.to_string()
        + "\n"
        + &mirror_stats
        + "\n"
        + &query_cache
        + "\n"
        + &rewrite_stats;
    let response = Response::builder()
        .header(
            hyper::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )
        .body(Full::new(Bytes::from(metrics_data)))
        .unwrap_or_else(|_| Response::new(Full::new(Bytes::from("Metrics unavailable"))));

    Ok(response)
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
