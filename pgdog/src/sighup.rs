#[cfg(target_family = "unix")]
use tokio::signal::unix::*;

pub struct Sighup {
    #[cfg(target_family = "unix")]
    sig: Signal,
}

impl Sighup {
    pub(crate) fn new() -> std::io::Result<Self> {
        let sig = signal(SignalKind::hangup())?;
        Ok(Self { sig })
    }

    pub(crate) async fn listen(&mut self) {
        #[cfg(target_family = "unix")]
        self.sig.recv().await;

        #[cfg(not(target_family = "unix"))]
        loop {
            use std::time::Duration;
            use tokio::time::sleep;

            sleep(Duration::MAX).await;
        }
    }
}
