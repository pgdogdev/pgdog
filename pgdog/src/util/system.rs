//! Process limits and signals.

use std::future::pending;

#[cfg(unix)]
use tokio::signal::unix::{Signal, SignalKind, signal};
use tracing::warn;

/// Raise the NOFILE soft limit to the hard limit and return the new value.
///
/// Some container runtimes (e.g. containerd v2) set a low soft limit
/// while keeping a high hard limit. This causes "Too many open files"
/// errors under load. Raising the soft limit on startup avoids this.
#[allow(clippy::unnecessary_cast)]
#[cfg(unix)]
pub(crate) fn raise_nofile_limit() -> u64 {
    use libc::{RLIMIT_NOFILE, getrlimit, rlimit, setrlimit};

    let mut rlim = rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };

    unsafe {
        if getrlimit(RLIMIT_NOFILE, &mut rlim) != 0 {
            warn!("failed to get NOFILE limit");
            return 0;
        }
    }

    if rlim.rlim_cur < rlim.rlim_max {
        let prev = rlim.rlim_cur;
        rlim.rlim_cur = rlim.rlim_max;

        unsafe {
            if setrlimit(RLIMIT_NOFILE, &rlim) != 0 {
                warn!(
                    "failed to raise NOFILE soft limit from {} to {}",
                    prev, rlim.rlim_max
                );
                return prev as u64;
            }
        }
    }

    rlim.rlim_cur as u64
}

#[cfg(not(unix))]
pub(crate) fn raise_nofile_limit() -> u64 {
    0
}

/// SIGINT (Ctrl-C) and SIGTERM receivers: both request a graceful shutdown.
/// Registered once, so a signal that arrives before or between `recv` calls is kept.
pub(crate) struct ShutdownSignal {
    #[cfg(unix)]
    interrupt: Option<Signal>,
    #[cfg(unix)]
    terminate: Option<Signal>,
}

impl ShutdownSignal {
    /// Install the handlers. A failure is logged, and that signal then never resolves `recv`.
    pub(crate) fn new() -> Self {
        Self {
            #[cfg(unix)]
            interrupt: install(SignalKind::interrupt(), "SIGINT"),
            #[cfg(unix)]
            terminate: install(SignalKind::terminate(), "SIGTERM"),
        }
    }

    /// Resolve on the next SIGINT or SIGTERM and return the signal name. Cancel safe.
    #[cfg(unix)]
    pub(crate) async fn recv(&mut self) -> &'static str {
        tokio::select! {
            _ = next(&mut self.interrupt) => "SIGINT",
            _ = next(&mut self.terminate) => "SIGTERM",
        }
    }

    /// Resolve on the next Ctrl-C. A failure to install the handler is logged,
    /// and `recv` then never resolves.
    #[cfg(not(unix))]
    pub(crate) async fn recv(&mut self) -> &'static str {
        if let Err(err) = tokio::signal::ctrl_c().await {
            warn!("Ctrl-C handler not installed, Ctrl-C will stop PgDog immediately: {err}");
            pending::<()>().await;
        }
        "Ctrl-C"
    }
}

/// SIGHUP receiver. Registered once, so a SIGHUP that arrives between two
/// `recv` calls is kept and returned by the next `recv`.
pub(crate) struct Sighup {
    #[cfg(unix)]
    signal: Option<Signal>,
}

impl Sighup {
    /// Install the SIGHUP handler. A failure is logged, and `recv` then never resolves.
    pub(crate) fn new() -> Self {
        Self {
            #[cfg(unix)]
            signal: install(SignalKind::hangup(), "SIGHUP"),
        }
    }

    /// Resolve on the next SIGHUP. Never resolves on platforms without SIGHUP,
    /// or when the handler is not installed. Cancel safe.
    pub(crate) async fn recv(&mut self) {
        #[cfg(unix)]
        next(&mut self.signal).await;
        #[cfg(not(unix))]
        pending::<()>().await;
    }
}

#[cfg(unix)]
fn install(kind: SignalKind, name: &str) -> Option<Signal> {
    signal(kind)
        .inspect_err(|err| {
            warn!("{name} handler not installed, {name} will stop PgDog immediately: {err}")
        })
        .ok()
}

#[cfg(unix)]
async fn next(signal: &mut Option<Signal>) {
    match signal {
        Some(signal) => {
            signal.recv().await;
        }
        None => pending::<()>().await,
    }
}
