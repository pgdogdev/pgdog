use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use parking_lot::Mutex;
use tokio::{
    select, spawn,
    sync::{broadcast, Notify},
};
use tracing::{error, info};

use crate::{
    backend::{pool::Address, Error, Server, ServerOptions},
    config::config,
    net::{FromBytes, Message, NotificationResponse, Protocol, ToBytes},
};

#[derive(Debug)]
enum Request {
    Subscribe(String),
    Unsubscribe(String),
}

#[derive(Debug)]
pub(super) struct Inner {
    pub(super) channels: Mutex<HashMap<String, broadcast::Sender<NotificationResponse>>>,
    pub(super) requests: Mutex<VecDeque<Request>>,
    pub(super) request_notify: Notify,
    pub(super) shutdown: Notify,
    pub(super) restart: Notify,
}

impl Inner {
    pub(super) fn new() -> Self {
        Self {
            channels: Mutex::new(HashMap::new()),
            requests: Mutex::new(VecDeque::new()),
            request_notify: Notify::new(),
            shutdown: Notify::new(),
            restart: Notify::new(),
        }
    }

    pub(super) fn handle_message(&self, message: Message) -> Result<(), Error> {
        let notification = NotificationResponse::from_bytes(message.to_bytes()?)?;

        let guard = self.channels.lock();
        if let Some(channel) = guard.get(notification.channel()) {
            if let Err(err) = channel.send(notification) {
                self.requests
                    .lock()
                    .push_back(Request::Unsubscribe(err.0.channel().to_string()));
            }
        }

        Ok(())
    }

    pub(super) fn request_subscribe(
        &self,
        name: &str,
    ) -> Result<broadcast::Receiver<NotificationResponse>, Error> {
        let mut guard = self.channels.lock();
        if let Some(channel) = guard.get(name) {
            Ok(channel.subscribe())
        } else {
            let (tx, rx) = broadcast::channel(config().config.general.pub_sub_channel_size);

            guard.insert(name.to_string(), tx);

            self.requests
                .lock()
                .push_back(Request::Subscribe(name.to_string()));
            self.request_notify.notify_one();

            Ok(rx)
        }
    }

    pub(super) async fn subscribe(&self, server: &mut Server) -> Result<(), Error> {
        loop {
            let request = self.requests.lock().pop_front();
            if let Some(request) = request {
                let query = match &request {
                    Request::Subscribe(channel) => {
                        format!("LISTEN {}", channel)
                    }

                    Request::Unsubscribe(channel) => {
                        format!("UNLISTEN {}", channel)
                    }
                };
                if let Err(err) = server.execute(query).await {
                    self.requests.lock().push_front(request);
                    return Err(err);
                }
            } else {
                break;
            }
        }

        Ok(())
    }

    pub(super) async fn reconnect(&self, addr: &Address) -> Result<Server, Error> {
        let mut server = Server::connect(addr, ServerOptions::default()).await?;

        let channels = self
            .channels
            .lock()
            .keys()
            .map(|k| Request::Subscribe(k.clone()))
            .collect::<Vec<_>>();

        {
            let mut guard = self.requests.lock();
            guard.extend(channels);
        }

        self.subscribe(&mut server).await?;

        Ok(server)
    }

    pub(super) async fn serve(&self, addr: &Address) -> Result<(), Error> {
        info!("launching listener [{}]", addr);

        let mut server = self.reconnect(addr).await?;
        loop {
            select! {
                 message = server.read() => {
                   let message = message?;

                    if message.code() != 'A' {
                        continue;
                    }

                    self.handle_message(message)?;
                }

                _ = self.request_notify.notified() => {
                    self.subscribe(&mut server).await?;
                }

                _ = self.restart.notified() => { break ;}

                _ = self.shutdown.notified() => {
                    break;
                }
            }
        }

        Ok(())
    }
}
