use ::settings::connection;
use anyhow::Result;
use broker_proto::{
    CheckinReply, CheckinRequest, SinkInfo,
    broker_server::{Broker, BrokerServer},
};
use rpcutil::auth::{self, AuthInterceptor, Peer};
use std::net::{AddrParseError, SocketAddr};
use tonic::transport;
use tonic::{Request, Response, Status, transport::ServerTlsConfig};

pub mod settings;
mod topology;

#[derive(Debug)]
struct BrokerImpl {
    cache: topology::Cache,
}

impl BrokerImpl {
    fn new(mapping: topology::Mapping) -> BrokerImpl {
        BrokerImpl {
            cache: topology::Cache::new(mapping),
        }
    }
}

#[tonic::async_trait]
impl Broker for BrokerImpl {
    async fn checkin(
        &self,
        request: Request<CheckinRequest>,
    ) -> Result<Response<CheckinReply>, Status> {
        let peer = auth::peer(&request)?;
        let request = request.get_ref();

        tracing::info!("[{}] checkin({:?})", &peer, &request);

        // TODO: this is obviously per user.
        let mut reply_sinks: Vec<SinkInfo> = vec![];
        match peer {
            Peer::User(_) => {}
            Peer::Cli(_) => {}
            Peer::Source(source) => {
                reply_sinks = self.cache.for_source(&source.id);
            }
            Peer::Sink(sink) => {
                self.cache.update_sink(SinkInfo {
                    id: sink.id().to_string(),
                    listening_on: request.listening_on.clone(),
                });
            }
        }

        let reply = CheckinReply {
            next_checkin_s: 10,
            sink: reply_sinks,
        };
        Ok(Response::new(reply))
    }
}

#[derive(thiserror::Error, Debug)]
pub enum BuilderError {
    #[error("Missing listening address")]
    MissingAddress,
    #[error("Missing client connection settings")]
    MissingClientConnection,
    #[error("Invalid listening address")]
    InvalidAddress(#[from] AddrParseError),
}

pub struct Server {
    address: SocketAddr,
    client_connection: connection::Info,
    mapping: topology::Mapping,
}

#[derive(Default)]
pub struct Builder {
    address: Option<SocketAddr>,
    client_connection: Option<connection::Info>,
    mapping: topology::Mapping,
}

impl Builder {
    pub fn build(self) -> Result<Server, BuilderError> {
        Ok(Server {
            address: self.address.ok_or(BuilderError::MissingAddress)?,
            client_connection: self
                .client_connection
                .ok_or(BuilderError::MissingClientConnection)?,
            mapping: self.mapping,
        })
    }

    pub fn address(mut self, addr: SocketAddr) -> Builder {
        self.address = Some(addr);
        self
    }

    pub fn client_connection(mut self, info: connection::Info) -> Builder {
        self.client_connection = Some(info);
        self
    }

    pub fn mapping(mut self, mapping: topology::Mapping) -> Builder {
        self.mapping = mapping;
        self
    }

    pub fn settings(self, settings: &settings::Settings) -> Result<Builder, BuilderError> {
        Ok(self
            .address(*settings.server().address())
            .client_connection(settings.connection().info().clone())
            .mapping(settings.mappings().clone()))
    }
}

impl Server {
    pub fn builder() -> Builder {
        Builder::default()
    }

    pub async fn serve(&self) -> Result<()> {
        tracing::info!("starting up on {:?}", &self.address);

        let broker = BrokerImpl::new(self.mapping.clone());
        let svc = BrokerServer::with_interceptor(broker, AuthInterceptor::default());
        transport::Server::builder()
            .tls_config(
                ServerTlsConfig::new()
                    .identity(self.client_connection.identity().clone())
                    .client_ca_root(self.client_connection.peer_root().clone()),
            )?
            .add_service(svc)
            .serve(self.address)
            .await?;

        tracing::info!("shutting down...");
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn sources_find_sinks() -> anyhow::Result<()> {
        let mut mapping = topology::Mapping::default();
        mapping.add_pair("111.src", "222.snk");

        let srv = BrokerImpl::new(mapping);

        let from_source = rpcutil::testing::request(
            CheckinRequest {
                listening_on: vec![],
            },
            auth::Peer::Source(auth::Source {
                id: "111.src".to_string(),
            }),
        );
        let response = srv.checkin(from_source).await?.into_inner();
        assert_eq!(response.sink, vec![]); // no known sink yet.

        let from_sink = rpcutil::testing::request(
            CheckinRequest {
                listening_on: vec!["a".to_string()],
            },
            auth::Peer::Sink(auth::Sink {
                id: "222.snk".to_string(),
            }),
        );
        srv.checkin(from_sink).await?;

        // Now we know a sink!
        let from_source = rpcutil::testing::request(
            CheckinRequest {
                listening_on: vec![],
            },
            auth::Peer::Source(auth::Source {
                id: "111.src".to_string(),
            }),
        );
        let response = srv.checkin(from_source).await?.into_inner();
        assert_eq!(
            response.sink,
            vec![SinkInfo {
                id: "222.snk".to_string(),
                listening_on: vec!["a".to_string()]
            }]
        );

        Ok(())
    }
}
