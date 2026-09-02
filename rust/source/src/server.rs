use self::builder::Builder;
use crate::{
    datamanager,
    filemanager::{self, FileManager},
    watcher,
};
use actor::{Tracker, router::Actor};
use anyhow::{Context, Result};
use crypto::SharedRandom;
use rpcutil::auth::AuthInterceptor;
use settings::{client, connection};
use source_proto::{
    GetStatsReply, GetStatsRequest,
    get_stats_reply::StateInfo,
    source_server::{Source, SourceServer},
};
use std::{net::SocketAddr, path::PathBuf};
use tokio_util::sync::CancellationToken;
use tonic::{
    Response,
    transport::{self, ServerTlsConfig},
};

mod builder;
mod peer;

/// A Source server, which watches the filesystem and backs data up to a Sink.
pub struct Server {
    connection: connection::Info,
    broker_info: client::Settings,
    address: SocketAddr,
    roots: Vec<PathBuf>,
    rnd: SharedRandom,
    datastore_path: PathBuf,
    filestore_path: PathBuf,
    token: CancellationToken,
}

pub fn builder(token: CancellationToken) -> Builder {
    Builder::new(token)
}

impl Server {
    /// Run the server.
    pub async fn serve(self) -> Result<()> {
        let mut actors = Tracker::new(self.token);

        let broker = broker_client::new(&self.connection, &self.broker_info, &mut actors).await?;
        let _peer = peer::new(broker, self.connection.clone(), &mut actors);
        let datamanager = datamanager::new(&self.datastore_path, &mut actors)
            .await
            .context("DataManager")?;
        let watcher = watcher::new(&mut actors)?;
        let filemanager = filemanager::new(
            &self.filestore_path,
            self.rnd,
            datamanager,
            watcher,
            &mut actors,
        )
        .await
        .context("FileManager")?;
        filemanager.set_roots(self.roots.clone()).await?;

        let source_server = SourceImpl { filemanager };
        tracing::info!("Listening on {}", &self.address);
        let svc = SourceServer::with_interceptor(source_server, AuthInterceptor::default());
        let server = transport::Server::builder()
            .tls_config(
                ServerTlsConfig::new()
                    .identity(self.connection.identity().clone())
                    .client_ca_root(self.connection.peer_root().clone()),
            )?
            .add_service(svc);
        actors.start(Actor::new(self.address, server), "Server");

        actor::combine(actors.run().await)?;
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
