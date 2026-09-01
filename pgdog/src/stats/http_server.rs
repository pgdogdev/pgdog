use std::convert::Infallible;

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::select;
use tracing::{info, warn};

use super::{
    Clients, ClientsLocked, Listeners, Logins, LookupMetrics, MirrorStatsMetrics, Pools,
    QueryCache, TwoPc,
};
use crate::tasks;

async fn metrics(_: Request<hyper::body::Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    let clients = Clients::load();
    let clients_locked = ClientsLocked::load();
    let pools = Pools::load();
    let mirror_stats: Vec<_> = MirrorStatsMetrics::load()
        .into_iter()
        .map(|m| m.to_string())
        .collect();
    let mirror_stats = mirror_stats.join("\n");
    let lookup_stats: Vec<_> = LookupMetrics::load()
        .into_iter()
        .map(|m| m.to_string())
        .collect();
    let lookup_stats = lookup_stats.join("\n");
    let listeners: Vec<_> = Listeners::load()
        .into_iter()
        .map(|m| m.to_string())
        .collect();
    let listeners = listeners.join("\n");
    let query_cache: Vec<_> = QueryCache::load()
        .metrics()
        .into_iter()
        .map(|m| m.to_string())
        .collect();
    let query_cache = query_cache.join("\n");
    let two_pc = TwoPc::load();
    let logins: Vec<_> = Logins::load()
        .into_iter()
        .chain(std::iter::once(Logins::histogram()))
        .map(|m| m.to_string())
        .collect();
    let logins = logins.join("\n");
    let metrics_data = clients.to_string()
        + "\n"
        + &clients_locked.to_string()
        + "\n"
        + &pools.to_string()
        + "\n"
        + &mirror_stats
        + "\n"
        + &lookup_stats
        + "\n"
        + &listeners
        + "\n"
        + &query_cache
        + "\n"
        + &logins
        + "\n"
        + &two_pc.to_string();
    let response = Response::builder()
        .header(
            hyper::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )
        .body(Full::new(Bytes::from(metrics_data)))
        .unwrap_or_else(|_| Response::new(Full::new(Bytes::from("Metrics unavailable"))));

    Ok(response)
}

pub(crate) async fn server(host: &str, port: u16) -> std::io::Result<()> {
    let addr = format!("{}:{}", host, port);
    info!("OpenMetrics endpoint http://{}", addr);
    let listener = TcpListener::bind(&addr).await?;
    let shutdown = tasks::shutdown_signal();

    loop {
        let (stream, _) = select! {
            result = listener.accept() => result?,
            _ = shutdown.cancelled() => break,
        };
        let io = TokioIo::new(stream);
        let shutdown = shutdown.clone();

        tasks::spawn("openmetrics http server", async move {
            let connection = http1::Builder::new().serve_connection(io, service_fn(metrics));

            tokio::select! {
                result = connection => {
                    if let Err(err) = result {
                        // Clients (TCP health probes, scrapers that disconnect
                        // mid-request, keep-alive teardown) routinely close the socket
                        // before sending a complete request. That surfaces as
                        // IncompleteMessage and is benign.
                        if !err.is_incomplete_message() {
                            warn!("OpenMetrics endpoint error: {:?}", err);
                        }
                    }
                }
                _ = shutdown.cancelled() => {}
            }
        });
    }

    Ok(())
}
