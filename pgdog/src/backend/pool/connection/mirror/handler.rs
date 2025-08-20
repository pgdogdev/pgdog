use super::*;

#[derive(Debug, Clone, PartialEq, Copy)]
enum MirrorHandlerState {
    Dropping,
    Sending,
    Idle,
}

#[derive(Debug)]
pub struct MirrorHandler {
    tx: Sender<MirrorRequest>,
    exposure: f32,
    state: MirrorHandlerState,
    buffer: Vec<BufferWithDelay>,
    timer: Instant,
}

impl MirrorHandler {
    #[cfg(test)]
    pub(super) fn buffer(&self) -> &[BufferWithDelay] {
        &self.buffer
    }

    pub fn new(tx: Sender<MirrorRequest>, exposure: f32) -> Self {
        Self {
            tx,
            exposure,
            state: MirrorHandlerState::Idle,
            buffer: vec![],
            timer: Instant::now(),
        }
    }

    /// Maybe send request to handler.
    pub fn send(&mut self, buffer: &Buffer) -> bool {
        match self.state {
            MirrorHandlerState::Dropping => {
                debug!("mirror dropping request");
                false
            }
            MirrorHandlerState::Idle => {
                let roll = if self.exposure < 1.0 {
                    thread_rng().gen_range(0.0..1.0)
                } else {
                    0.99
                };

                if roll < self.exposure {
                    self.state = MirrorHandlerState::Sending;
                    self.buffer.push(BufferWithDelay {
                        buffer: buffer.clone(),
                        delay: Duration::ZERO,
                    });
                    self.timer = Instant::now();
                    true
                } else {
                    self.state = MirrorHandlerState::Dropping;
                    debug!("mirror dropping transaction [exposure: {}]", self.exposure);
                    false
                }
            }
            MirrorHandlerState::Sending => {
                let now = Instant::now();
                self.buffer.push(BufferWithDelay {
                    delay: now.duration_since(self.timer),
                    buffer: buffer.clone(),
                });
                self.timer = now;
                true
            }
        }
    }

    pub fn flush(&mut self) -> bool {
        if self.state == MirrorHandlerState::Dropping {
            debug!("mirror transaction dropped");
            self.state = MirrorHandlerState::Idle;
            false
        } else {
            debug!("mirror transaction flushed");
            self.state = MirrorHandlerState::Idle;

            self.tx
                .try_send(MirrorRequest {
                    buffer: std::mem::take(&mut self.buffer),
                })
                .is_ok()
        }
    }
}
