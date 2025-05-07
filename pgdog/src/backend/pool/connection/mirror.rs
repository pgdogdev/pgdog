use crate::{
    backend::{pool::Request, Cluster},
    frontend::{router::Route, Buffer},
};

use super::Connection;
use super::Error;

#[derive(Clone, Debug)]
pub(crate) struct MirrorRequest {
    pub(super) route: Route,
    pub(super) request: Request,
    pub(super) buffer: Buffer,
}

pub(crate) struct Mirror {
    connection: Connection,
}

impl Mirror {
    pub(crate) async fn handle(&mut self, request: &MirrorRequest) -> Result<(), Error> {
        if !self.connection.connected() {
            self.connection
                .connect(&request.request, &request.route)
                .await?;
        }

        self.connection.send(&request.buffer).await?;

        while self.connection.has_more_messages() {
            let _ = self.connection.read().await?;
        }

        if self.connection.done() {
            self.connection.disconnect();
        }

        Ok(())
    }
}
