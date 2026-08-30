use self::{builder::Builder, peer::Peer};
use crate::{
    fatal::{self, Fatal},
    filemanager::{self, FileManager},
};
use anyhow::{Context, Result};
use error_collection::Errors;
use rpcutil::auth::AuthInterceptor;
use settings::connection;
use source_proto::{
    GetStatsReply, GetStatsRequest,
    get_stats_reply::StateInfo,
    source_server::{Source, SourceServer},
};
use std::{net::SocketAddr, path::PathBuf};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
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
    filemanager_context: filemanager::FileManagerContext,
    token: CancellationToken,
    datamanager_handle: JoinHandle<()>,
}

pub fn builder(token: CancellationToken) -> Builder {
    Builder::new(token)
}

impl<P: Peer> Server<P> {
    /// Run the server.
    pub async fn serve(mut self) -> Result<()> {
        let source_server = SourceImpl {
            filemanager: self.filemanager_context.manager.clone(),
        };
        tracing::info!("Listening on {}", &self.address);
        let svc = SourceServer::with_interceptor(source_server, AuthInterceptor::default());
        let server = transport::Server::builder()
            .tls_config(
                ServerTlsConfig::new()
                    .identity(self.connection.identity().clone())
                    .client_ca_root(self.connection.peer_root().clone()),
            )?
            .add_service(svc);

        let mut server = Box::pin(
            server
                // Ensure the server shuts down along with the rest.
                .serve_with_shutdown(self.address, self.token.child_token().cancelled_owned()),
        );

        let mut server_result = None;
        let mut filemanager_result = None;
        let mut datamanager_result = None;
        match self.initialize().await {
            Ok(_) => {
                // We stop the server at the first failure of a submodule, as there is
                // no real way to continue at the moment.
                tokio::select! {
                    r = &mut self.filemanager_context.handle => {
                        tracing::info!("FileManager stopped");
                        // Pull the ripcord lto ensure everything shuts down.
                        self.token.cancel();
                        filemanager_result = Some(r);
                    },
                    r = &mut self.datamanager_handle => {
                        tracing::info!("DataManager stopped");
                        // Pull the ripcord lto ensure everything shuts down.
                        self.token.cancel();
                        datamanager_result = Some(r);
                    },
                    r = &mut server => {
                        tracing::info!("Server stopped");
                        // Pull the ripcord to ensure everything shuts down.
                        self.token.cancel();
                        server_result = Some(r.context("SourceImpl"));
                    }
                }
            }
            Err(Fatal::Shutdown) => {
                self.token.cancel();
            }
            Err(e) => {
                tracing::error!("Server init failed: {}", e);
                self.token.cancel();
            }
        };
        // We can then fill in the results of all the other missing futures.
        if server_result.is_none() {
            server_result = Some(server.await.context("Server"));
        }
        if filemanager_result.is_none() {
            filemanager_result = Some(self.filemanager_context.handle.await);
        }
        if datamanager_result.is_none() {
            datamanager_result = Some(self.datamanager_handle.await);
        }

        let mut errors = Errors::new();
        errors.collect(server_result.unwrap());
        errors.collect(filemanager_result.unwrap());
        errors.collect(datamanager_result.unwrap());
        errors.as_result()
    }

    // All failible initialization goes here so we can safely shut down if any
    // fails.
    async fn initialize(&self) -> fatal::Result<()> {
        self.filemanager_context
            .manager
            .set_roots(self.roots.clone())
            .await
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
                state_info: stats
                    .file_count
                    .into_iter()
                    .map(|(state, file_count)| StateInfo { state, file_count })
                    .collect(),
            })),
            Err(err) => Err(tonic::Status::internal(err.to_string())),
        }
    }
}
