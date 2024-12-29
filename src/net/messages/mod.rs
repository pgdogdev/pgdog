pub mod hello;
pub use hello::Startup;

pub mod payload;
pub use payload::Payload;

pub mod auth;
pub use auth::AuthenticationOk;

pub mod rfq;
pub use rfq::ReadyForQuery;

pub mod backend_key;
pub use backend_key::BackendKeyData;

pub mod parameter_status;
pub use parameter_status::ParameterStatus;

use crate::net::Error;

use bytes::Bytes;
use tokio::io::AsyncWrite;
use tracing::debug;

pub trait ToBytes {
    fn to_bytes(&self) -> Result<Bytes, Error>;
}

#[async_trait::async_trait]
pub trait Protocol: ToBytes {
    fn code(&self) -> char;

    async fn write(&self, stream: &mut (impl AsyncWrite + Unpin + Send)) -> Result<(), Error> {
        use tokio::io::AsyncWriteExt;

        let bytes = self.to_bytes()?;

        debug!("📡 <= {}", self.code());

        stream.write_all(&bytes).await?;

        Ok(())
    }
}

macro_rules! send {
    ($t:tt) => {
        impl $t {
            pub async fn write(
                &self,
                stream: &mut (impl tokio::io::AsyncWrite + std::marker::Unpin),
            ) -> Result<(), crate::net::Error> {
                use tokio::io::AsyncWriteExt;

                let bytes = self.to_bytes()?;
                stream.write_all(&bytes).await?;

                Ok(())
            }
        }
    };
}

pub(crate) use send;
