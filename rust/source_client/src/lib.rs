use anyhow::Context;
use mockall::automock;
use settings::{client, connection};
use source_proto::{GetStatsRequest, source_client::SourceClient};
use tonic::{
    Request, async_trait,
    transport::{Channel, ClientTlsConfig},
};

/// API exposed by Sources.
#[automock]
#[async_trait]
pub trait Source {
    async fn get_stats(&mut self) -> anyhow::Result<Stats>;
}

pub struct Stats {
    pub total_file_count: i32,
}

/// Create a new Source client.
pub async fn new(
    client: &connection::Info,
    server: &client::Settings,
) -> anyhow::Result<impl Source> {
    let tls = ClientTlsConfig::new()
        .domain_name(server.name())
        .ca_certificate(client.peer_root().clone())
        .identity(client.identity().clone());
    let channel = Channel::builder(server.address().clone())
        .tls_config(tls.clone())?
        .connect_lazy();
    Ok(SourceImpl {
        client: SourceClient::new(channel),
    })
}

struct SourceImpl {
    client: SourceClient<Channel>,
}

#[async_trait]
impl Source for SourceImpl {
    async fn get_stats(&mut self) -> anyhow::Result<Stats> {
        let request = Request::new(GetStatsRequest {});
        let resp = self
            .client
            .get_stats(request)
            .await
            .context("rpc failed")?
            .into_inner();
        return Ok(Stats {
            total_file_count: resp.total_file_count,
        });
    }
}
