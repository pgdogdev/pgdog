//! Server message I/O operations.

use tokio::{io::AsyncWriteExt, time::Instant};
use tracing::{debug, error, trace};

use super::{Error, HandleResult, Server};
use crate::{
    frontend::ClientRequest,
    net::{
        messages::{
            CommandComplete, ErrorResponse, FromBytes, Message, ParameterStatus, Protocol,
            ReadyForQuery, ToBytes,
        },
        ProtocolMessage,
    },
    state::State,
    stats::memory::MemoryUsage,
};

impl Server {
    /// Send messages to the server and flush the buffer.
    pub async fn send(&mut self, client_request: &ClientRequest) -> Result<(), Error> {
        self.stats.state(State::Active);

        for message in client_request.messages.iter() {
            self.send_one(message).await?;
        }
        self.flush().await?;

        self.stats.state(State::ReceivingData);

        Ok(())
    }

    /// Send one message to the server but don't flush the buffer,
    /// accelerating bulk transfers.
    pub async fn send_one(&mut self, message: &ProtocolMessage) -> Result<(), Error> {
        self.stats.state(State::Active);

        let result = self.prepared_statements.handle(message)?;

        let queue = match result {
            HandleResult::Drop => [None, None],
            HandleResult::Prepend(ref prepare) => [Some(prepare), Some(message)],
            HandleResult::Forward => [Some(message), None],
        };

        for message in queue.into_iter().flatten() {
            match self.stream().send(message).await {
                Ok(sent) => self.stats.send(sent),
                Err(err) => {
                    self.stats.state(State::Error);
                    return Err(err.into());
                }
            }
        }

        Ok(())
    }

    /// Flush all pending messages making sure they are sent to the server immediately.
    pub async fn flush(&mut self) -> Result<(), Error> {
        if let Err(err) = self.stream().flush().await {
            trace!("😳");
            self.stats.state(State::Error);
            Err(err.into())
        } else {
            Ok(())
        }
    }

    /// Read a single message from the server.
    pub async fn read(&mut self) -> Result<Message, Error> {
        let message = loop {
            if let Some(message) = self.prepared_statements.state_mut().get_simulated() {
                return Ok(message.backend());
            }
            match self
                .stream
                .as_mut()
                .unwrap()
                .read_buf(&mut self.stream_buffer)
                .await
            {
                Ok(message) => {
                    let message = message.stream(self.streaming).backend();
                    match self.prepared_statements.forward(&message) {
                        Ok(forward) => {
                            if forward {
                                break message;
                            }
                        }
                        Err(err) => {
                            error!(
                                "{:?} got: {}, extended buffer: {:?}",
                                err,
                                message.code(),
                                self.prepared_statements.state(),
                            );
                            return Err(err);
                        }
                    }
                }

                Err(err) => {
                    self.stats.state(State::Error);
                    return Err(err.into());
                }
            }
        };

        self.stats.receive(message.len());

        match message.code() {
            'Z' => {
                let now = Instant::now();
                self.stats.query(now);
                self.stats.memory_used(self.memory_usage());

                let rfq = ReadyForQuery::from_bytes(message.payload())?;

                match rfq.status {
                    'I' => {
                        self.in_transaction = false;
                        self.stats.transaction(now);
                    }
                    'T' => {
                        self.in_transaction = true;
                        self.stats.state(State::IdleInTransaction);
                    }
                    'E' => self.stats.transaction_error(now),
                    status => {
                        self.stats.state(State::Error);
                        return Err(Error::UnexpectedTransactionStatus(status));
                    }
                }

                self.streaming = false;
            }
            'E' => {
                let error = ErrorResponse::from_bytes(message.to_bytes()?)?;
                self.schema_changed = error.code == "0A000";
                self.stats.error();
            }
            'W' => {
                debug!("streaming replication on [{}]", self.addr());
                self.streaming = true;
            }
            'S' => {
                let ps = ParameterStatus::from_bytes(message.to_bytes()?)?;
                self.changed_params.insert(ps.name, ps.value);
            }
            'C' => {
                let cmd = CommandComplete::from_bytes(message.to_bytes()?)?;
                match cmd.command() {
                    "PREPARE" | "DEALLOCATE" => self.sync_prepared = true,
                    "RESET" => self.client_params.clear(), // Someone reset params, we're gonna need to re-sync.
                    _ => (),
                }
            }
            'G' => self.stats.copy_mode(),
            '1' => self.stats.parse_complete(),
            '2' => self.stats.bind_complete(),
            _ => (),
        }

        Ok(message)
    }
}

#[cfg(test)]
pub mod test {
    // Messaging tests will be moved here
}
