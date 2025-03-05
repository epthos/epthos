use std::sync::Arc;

use anyhow::Context;
use mockall::automock;
use settings::connection;
use sink_proto::{sink_client::SinkClient, StoreRequest};
use tokio::sync::Mutex;
use tonic::async_trait;
use tonic::{
    transport::{Channel, ClientTlsConfig},
    Request, Result,
};

/// A Sink is responsible for storing data from a Source.
#[automock]
#[async_trait]
pub trait Sink {
    // Send a chunk of bytes to the Sink for storage.
    async fn store(&self, chunk: &[u8]) -> Result<()>;
}

/// A SinkBuilder is responsible for creating a Sink.
#[automock]
#[async_trait]
pub trait SinkBuilder<S: Sink + Sized + std::marker::Sync> {
    /// Create a new Sink.
    async fn connect(
        &self,
        settings: &connection::Info,
        id: &str,
        address: String,
    ) -> anyhow::Result<S>;
}

#[derive(Default)]
pub struct SinkBuilderImpl;

pub struct SinkImpl {
    stub: Arc<Mutex<SinkClient<Channel>>>,
}

#[async_trait]
impl SinkBuilder<SinkImpl> for SinkBuilderImpl {
    async fn connect(
        &self,
        settings: &connection::Info,
        id: &str,
        address: String,
    ) -> anyhow::Result<SinkImpl> {
        let tls = ClientTlsConfig::new()
            .domain_name(id)
            .ca_certificate(settings.peer_root().clone())
            .identity(settings.identity().clone());
        let channel = Channel::from_shared(address)
            .context("can't parse the URL")?
            .tls_config(tls.clone())
            .context("failed to configure TLS")?
            .connect()
            .await
            .context("failed to connect")?;

        Ok(SinkImpl {
            stub: Arc::new(Mutex::new(SinkClient::new(channel))),
        })
    }
}

#[async_trait]
impl Sink for SinkImpl {
    async fn store(&self, chunk: &[u8]) -> Result<()> {
        let mut stub = self.stub.lock().await;
        stub.store(Request::new(StoreRequest {
            data: chunk.to_owned(),
        }))
        .await?;

        Ok(())
    }
}
