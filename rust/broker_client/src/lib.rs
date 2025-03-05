use anyhow::Context;
use broker_proto::{broker_client::BrokerClient, CheckinReply, CheckinRequest};
use http::Uri;
use mockall::automock;
use rpcutil::{Backoff, ExpBackoff};
use settings::connection;
use std::str::FromStr;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tonic::async_trait;
use tonic::transport::{Channel, ClientTlsConfig};

#[automock]
#[async_trait]
pub trait Broker {
    /// Get the list of peers known to the Broker. This can be empty if the Broker has not yet received
    /// the information or if it was not yet contacted.
    async fn get_peers(&self) -> anyhow::Result<Arc<Vec<SinkLocation>>>;

    /// Inform the broker of the addresses the caller (typically, a Source) is listening on, to make it
    /// available to other peers (typically, Sinks).
    async fn set_addresses(&mut self, listening_on: &[SocketAddr]) -> anyhow::Result<()>;
}

/// Description of a sink, as known by the Broker.
#[derive(Debug, Clone)]
pub struct SinkLocation {
    id: String,
    addresses: Vec<String>,
}

/// Create a new Broker client. The client immediately starts performing checkins, and expects that
/// |checkin()| is regularly awaited.
pub async fn new(client: &connection::Info, server: &Settings) -> anyhow::Result<BrokerImpl> {
    let tls = ClientTlsConfig::new()
        .domain_name(server.name())
        .ca_certificate(client.peer_root().clone())
        .identity(client.identity().clone());
    // Connect lazily so that regular retries handle transient issues rather than having to do it
    // at creation as well.
    let channel = Channel::builder(server.address().clone())
        .tls_config(tls.clone())?
        .connect_lazy();

    BrokerImpl::new_impl(channel, Params::default()).await
}

/// Broker client. Used by all the peers to communicate their current state.
pub struct BrokerImpl {
    tx: mpsc::Sender<BrokerOps>,
}

impl SinkLocation {
    pub fn new(id: String, addresses: Vec<String>) -> Self {
        SinkLocation { id, addresses }
    }

    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn addresses(&self) -> &Vec<String> {
        &self.addresses
    }
}

pub mod wire {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Deserialize, Serialize)]
    pub struct Settings {
        pub name: String,
        pub address: String,
    }
}

#[derive(Debug, Clone)]
pub struct Settings {
    name: String,
    address: Uri,
}

impl Settings {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn address(&self) -> &Uri {
        &self.address
    }
}

impl settings::Anchored for Settings {
    type Wire = wire::Settings;

    fn anchor(wire: &Self::Wire, _anchor: &settings::Anchor) -> anyhow::Result<Self> {
        Ok(Settings {
            name: wire.name.clone(),
            address: Uri::from_str(&wire.address)?,
        })
    }
}
#[async_trait]
impl Broker for BrokerImpl {
    async fn get_peers(&self) -> anyhow::Result<Arc<Vec<SinkLocation>>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx.send(BrokerOps::GetPeers(reply_tx)).await?;
        reply_rx.await.context("broker agent closed unexpectedly")
    }

    async fn set_addresses(&mut self, listening_on: &[SocketAddr]) -> anyhow::Result<()> {
        let req = CheckinRequest {
            listening_on: listening_on
                .iter()
                .map(|addr| format!("https://{:?}", addr))
                .collect(),
        };
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx.send(BrokerOps::SetAddresses(req, reply_tx)).await?;
        reply_rx.await.context("broker agent closed unexpectedly")?;
        Ok(())
    }
}

enum BrokerOps {
    SetAddresses(CheckinRequest, oneshot::Sender<()>),
    GetPeers(oneshot::Sender<Arc<Vec<SinkLocation>>>),
}

struct BrokerActor {
    stub: BrokerClient<Channel>,
    rx: mpsc::Receiver<BrokerOps>,
    params: Params,
}

impl BrokerActor {
    async fn run(&mut self) -> anyhow::Result<()> {
        let mut checkin_delay = Duration::ZERO;
        let mut backoff = self.params.backoff.clone();
        let mut checkin_data = CheckinRequest::default();
        let mut location: Arc<Vec<SinkLocation>> = Arc::new(Vec::new());

        // At each round, we first wait for our required wait period then fetch the checkin data.
        // This ensures we wait _at least_ the right amount, and don't send unnecessary updates if
        // the local server is not ready.
        'actor: loop {
            // TODO: make the wait an absolute time, so that if we get interrupted with new checkin data
            // we don't postpone forever.
            tokio::select! {
                req = self.rx.recv() => {
                    match req {
                        Some(BrokerOps::SetAddresses(req, reply)) => {
                            checkin_data = req;
                            let _ = reply.send(());  // No need to check for errors, the client is gone.
                        }
                        Some(BrokerOps::GetPeers(reply)) => {
                            let _ = reply.send(location.clone());  // No need to check for errors, the client is gone.
                        }
                        None => {
                            // The client is shutting down, nothing more to do.
                            break 'actor;
                        }
                    }
                }
                // Time to check in with the Broker.
                _ = self.params.sleep.sleep(checkin_delay) => {
                    // TODO: handle deadlines.
                    match self.stub.checkin(checkin_data.clone()).await {
                        Ok(reply) => {
                            tracing::info!("checkin done.");
                            backoff.reset();
                            let reply :CheckinReply = reply.into_inner();
                            checkin_delay = Duration::from_secs(reply.next_checkin_s as u64);

                            location  = Arc::new(reply.sink.into_iter().map(|sink| {
                                SinkLocation { id: sink.id, addresses: sink.listening_on }
                            }).collect());
                        }
                        Err(err) => {
                            tracing::warn!("checkin failed: {:?}", err);
                            checkin_delay = backoff.again();
                        }
                    }
                }
            }
        }
        tracing::info!("shutting down client");
        Ok(())
    }
}

impl BrokerImpl {
    async fn new_impl(channel: Channel, params: Params) -> anyhow::Result<Self> {
        let (tx, rx) = mpsc::channel(1);

        tokio::spawn(async move {
            let mut actor = BrokerActor {
                stub: BrokerClient::new(channel),
                rx,
                params,
            };
            if let Err(err) = actor.run().await {
                tracing::error!("Broker client failed: {:?}", err);
            }
        });

        Ok(BrokerImpl { tx })
    }
}

/// Params are used to inject dependencies for testing.
struct Params {
    backoff: ExpBackoff,
    sleep: Box<dyn rpcutil::Sleeper>,
}

impl Params {
    pub fn default() -> Params {
        Params {
            backoff: ExpBackoff::new(&Backoff {
                min_delay: Duration::from_secs(1),
                max_delay: Duration::from_secs(60),
            }),
            sleep: rpcutil::jittery_sleeper(10),
        }
    }
}

#[cfg(test)]
mod test {
    use std::result::Result;

    use broker_proto::broker_server;
    use rpcutil::testing::{self, tracking_sleeper, CannedResponses};
    use tonic::{Request, Response, Status};

    use broker_proto::CheckinReply;

    use super::*;

    #[tokio::test]
    async fn client_handles_delays() -> anyhow::Result<()> {
        // Configure the server to send first an error (which causes an exponential backoff)
        // followed by a response so we can confirm we use the next_checkin recommendation.
        let mock_broker = Canned {
            0: CannedResponses::from([
                Err(Status::already_exists("me again")),
                Ok(Response::new(CheckinReply {
                    next_checkin_s: 321,
                    sink: vec![],
                })),
            ]),
        };
        let gudule = broker_server::BrokerServer::new(mock_broker);
        let channel = testing::fake_server(gudule).await?;

        let (tx, mut rx) = mpsc::channel::<Duration>(1);

        let client = super::BrokerImpl::new_impl(
            channel,
            Params {
                backoff: ExpBackoff::new(&Backoff {
                    min_delay: Duration::from_secs(12),
                    max_delay: Duration::from_secs(32),
                }),
                sleep: tracking_sleeper(tx),
            },
        )
        .await?;

        // We begin with two internal delays, as the first response from the Broker is an
        // error and did not return anything to the client.
        assert_eq!(rx.recv().await, Some(Duration::ZERO));
        assert_eq!(rx.recv().await, Some(Duration::from_secs(12)));

        // Then we get an actual update, and we see that the checkin duration is properly
        // fetched from the response.
        assert_eq!(rx.recv().await, Some(Duration::from_secs(321)));

        drop(client); // Ensure the client lives that long.

        Ok(())
    }

    struct Canned(CannedResponses<CheckinReply>);

    #[tonic::async_trait]
    impl broker_server::Broker for Canned {
        async fn checkin(
            &self,
            _request: Request<CheckinRequest>,
        ) -> Result<Response<CheckinReply>, Status> {
            self.0.next().await
        }
    }
}
