use std::{
    ffi::c_void,
    io::Result,
    os::fd::{AsFd, AsRawFd},
};

use socket2::{SockRef, TcpKeepalive};
use tokio::net::TcpStream;
use tracing::debug;

use crate::config::config;

#[cfg(target_os = "linux")]
use libc::{setsockopt, IPPROTO_TCP, TCP_USER_TIMEOUT};

pub fn tweak(socket: &TcpStream) -> Result<()> {
    let config = config().config.tcp;
    debug!("TCP settings: {:?}", config);

    // Disable the Nagle algorithm.
    socket.set_nodelay(true)?;

    let sock_ref = SockRef::from(socket);
    sock_ref.set_keepalive(true)?;
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
    if let Some(user_timeout) = config.user_timeout() {
        let timeout = user_timeout.as_millis() as u64;
        unsafe {
            let sock = sock_ref.as_fd().as_raw_fd();
            setsockopt(
                sock,
                TCP_USER_TIMEOUT,
                1,
                timeout as *const c_void,
                std::mem::size_of::<u64>() as u32,
            );
        }
    }

    Ok(())
}
