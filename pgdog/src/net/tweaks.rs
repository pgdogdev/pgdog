use std::io::Result;

use socket2::{SockRef, TcpKeepalive};
use tokio::net::TcpStream;

use crate::config::config;

pub fn tweak(socket: &TcpStream) -> Result<()> {
    let config = config();
    let config = &config.config.tcp;

    // Disable the Nagle algorithm.
    socket.set_nodelay(true)?;

    let sock_ref = SockRef::from(socket);
    sock_ref.set_keepalive(config.keepalive())?;
    let mut params = TcpKeepalive::new();
    if let Some(time) = config.time() {
        params = params.with_time(time);
    }
    if let Some(interval) = config.interval() {
        params = params.with_interval(interval);
    }
    if let Some(retries) = config.retries() {
        params = params.with_retries(retries);
    }
    sock_ref.set_tcp_keepalive(&params)?;

    #[cfg(target_os = "linux")]
    if let Some(congestion_control) = config.congestion_control() {
        sock_ref.set_tcp_congestion(congestion_control.as_bytes())?;
    }

    #[cfg(target_os = "linux")]
    sock_ref.set_tcp_user_timeout(config.user_timeout())?;

    Ok(())
}
