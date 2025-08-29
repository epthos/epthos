use self::{builder::Builder, peer::Peer};
use crate::filemanager::{self, FileManager};
use anyhow::{Context, Result};
use rpcutil::auth::AuthInterceptor;
use settings::connection;
use source_proto::{
    GetStatsReply, GetStatsRequest,
    source_server::{Source, SourceServer},
};
use std::{net::SocketAddr, path::PathBuf};
use tonic::{
    Response,
    transport::{self, ServerTlsConfig},
};

mod builder;
mod peer;

/// A Source server, which watches the filesystem and backs data up to a Sink.
pub struct Server<P: Peer> {
    connection: connection::Info,
    address: SocketAddr,
    roots: Vec<PathBuf>,
    _peer: P,
    manager: filemanager::FileManagerContext,
}

pub fn builder() -> Builder {
    Builder::default()
}

impl<P: Peer> Server<P> {
    /// Run the server.
    pub async fn serve(self) -> Result<()> {
        let filemanager = self.manager.manager;
        let filemanager_handle = self.manager.handle;

        // Set up the file store.
        filemanager.set_roots(self.roots).await?;

        let source_server = SourceImpl { filemanager };
        tracing::info!("Listening on {}", &self.address);
        let svc = SourceServer::with_interceptor(source_server, AuthInterceptor::default());
        let server = transport::Server::builder()
            .tls_config(
                ServerTlsConfig::new()
                    .identity(self.connection.identity().clone())
                    .client_ca_root(self.connection.peer_root().clone()),
            )?
            .add_service(svc)
            .serve(self.address);

        // We stop the server at the first failure of a submodule, as there is
        // no real way to continue at the moment.
        tokio::select! {
            r = filemanager_handle => {
                r.context("FileManager thread")?.context("FileManager status")?;
            },
            r = server => {
                r.context("SourceImpl failed")?;
            }
        }
        Ok(())
    }
}

struct SourceImpl {
    filemanager: FileManager,
}

#[tonic::async_trait]
impl Source for SourceImpl {
    async fn get_stats(
        &self,
        _request: tonic::Request<GetStatsRequest>,
    ) -> anyhow::Result<tonic::Response<GetStatsReply>, tonic::Status> {
        match self.filemanager.get_stats().await {
            Ok(stats) => Ok(Response::new(GetStatsReply {
                total_file_count: stats.total_file_count,
            })),
            Err(err) => Err(tonic::Status::internal(err.to_string())),
        }
    }
}
