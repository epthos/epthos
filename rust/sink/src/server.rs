use actor::{Tracker, router::Actor};
use anyhow::Result;
use broker_client::Broker;
use rpcutil::auth::{self, AuthInterceptor};
use settings::{client, connection};
use sink_proto::{
    StoreReply, StoreRequest,
    sink_server::{Sink, SinkServer},
};
use sink_settings::Settings;
use std::net::{AddrParseError, SocketAddr};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tonic::{
    Request, Response, Status,
    transport::{self, ServerTlsConfig},
};

pub struct Server {
    address: SocketAddr,
    connection: connection::Info,
    broker: client::Settings,
    token: CancellationToken,
}

#[derive(Default)]
pub struct Builder {
    token: CancellationToken,
    address: Option<SocketAddr>,
    broker_connection: Option<connection::Info>,
    broker: Option<client::Settings>,
}

#[derive(thiserror::Error, Debug)]
pub enum BuilderError {
    #[error("Missing Broker info")]
    MissingBrokerInfo,
    #[error("Missing connection settings")]
    MissingConnection,
    #[error("Missing listening address")]
    MissingAddress,
    #[error("Invalid listening address")]
    InvalidAddress(#[from] AddrParseError),
}

impl Builder {
    pub fn new(token: CancellationToken) -> Self {
        Builder {
            token,
            ..Builder::default()
        }
    }
    pub fn settings(self, settings: &Settings) -> Result<Builder, BuilderError> {
        Ok(self
            .connection(settings.connection())
            .address(*settings.server().address())
            .broker(settings.broker()))
    }

    pub fn address(mut self, addr: SocketAddr) -> Builder {
        self.address = Some(addr);
        self
    }

    pub fn connection(mut self, broker_connection: &connection::Settings) -> Builder {
        self.broker_connection = Some(broker_connection.info().clone());
        self
    }

    pub fn broker(mut self, broker: &client::Settings) -> Builder {
        self.broker = Some((*broker).clone());
        self
    }

    pub fn build(self) -> Result<Server, BuilderError> {
        Ok(Server {
            address: self.address.ok_or(BuilderError::MissingAddress)?,
            connection: self
                .broker_connection
                .ok_or(BuilderError::MissingConnection)?,
            broker: self.broker.ok_or(BuilderError::MissingBrokerInfo)?,
            token: self.token,
        })
    }
}

impl Server {
    pub fn builder(token: CancellationToken) -> Builder {
        Builder::new(token)
    }

    pub async fn serve(&self) -> anyhow::Result<()> {
        let mut actors = Tracker::new(self.token.clone());
        let mut broker = broker_client::new(&self.connection, &self.broker, &mut actors).await?;

        // Resolve the port we will listen on if it's not specified.
        // This is prone to race conditions in theory, not sure about practice.
        let mut addr = self.address;
        if addr.port() == 0 {
            let listener = TcpListener::bind(addr).await?;
            addr = listener.local_addr()?;
            drop(listener);
        }
        // Expand the list of addresses we listen on if we are not bound to a
        // single interface (which is the default).
        let mut accepting_on: Vec<SocketAddr> = vec![];
        if addr.ip().is_unspecified() {
            let intro = netutil::Introspect::new()?;
            for ip in intro.resolve()? {
                accepting_on.push(SocketAddr::new(ip, addr.port()));
            }
        } else {
            accepting_on.push(addr);
        }

        tracing::info!("Accepting traffic on {:?}", &accepting_on);

        broker.set_addresses(&accepting_on).await?;

        let sink = SinkImpl::default();
        let svc = SinkServer::with_interceptor(sink, AuthInterceptor::default());
        let server = transport::Server::builder()
            .tls_config(
                ServerTlsConfig::new()
                    .identity(self.connection.identity().clone())
                    .client_ca_root(self.connection.peer_root().clone()),
            )?
            .add_service(svc);

        actors.start(Actor::new(addr, server), "Server");
        actor::combine(actors.run().await)
    }
}

#[derive(Default)]
struct SinkImpl {}

#[tonic::async_trait]
impl Sink for SinkImpl {
    async fn store(&self, request: Request<StoreRequest>) -> Result<Response<StoreReply>, Status> {
        let peer = auth::peer(&request)?;
        tracing::info!("[{:?}] source()", &peer);

        let request = request.get_ref();
        tracing::info!("received {} bytes of data", request.data.len());

        Ok(Response::new(StoreReply {}))
    }
}
