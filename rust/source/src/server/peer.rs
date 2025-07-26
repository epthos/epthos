//! This module abstracts the work needed to connect to a sink
//! through the Broker, possibly using a proxy, etc.

use anyhow::{Context, Result};
use broker_client::{Broker, BrokerImpl, SinkLocation};
use bytes::Bytes;
use crypto::model;
use rpcutil::{Backoff, ExpBackoff};
use settings::connection;
use sink_client::{Sink, SinkBuilder, SinkBuilderImpl};
use std::marker::PhantomData;
use std::{sync::Arc, time::Duration};
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    oneshot,
};
use tonic::async_trait;

#[cfg(test)]
use mockall::automock;

#[cfg_attr(test, automock)]
#[async_trait]
pub trait Peer {
    /// Send a chunk to the sink. Blocks until the sink is available.
    #[allow(dead_code)] // TODO: use it!
    async fn send(&self, data: &model::Chunk) -> anyhow::Result<()>;
}

/// Returns a new PeerImpl that will immediately start connecting to a Sink using id as its local identity.
pub fn new(broker: BrokerImpl, id: connection::Info) -> PeerImpl {
    PeerImpl::new(broker, SinkBuilderImpl, id, Params::default())
}

/// Default implementation of a Peer.
pub struct PeerImpl {
    tx: Sender<PeerOp>,
}

#[async_trait]
impl Peer for PeerImpl {
    async fn send(&self, data: &model::Chunk) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(PeerOp::Send(data.bytes(), tx)).await?;
        rx.await?
    }
}

impl PeerImpl {
    /// Returns a new Peer, with the given parameters.
    fn new<B, S, K>(broker: B, sink_builder: S, id: connection::Info, params: Params) -> PeerImpl
    where
        B: Broker + std::marker::Send + std::marker::Sync + 'static,
        S: SinkBuilder<K> + std::marker::Send + std::marker::Sync + 'static,
        K: Sink + Sized + std::marker::Send + std::marker::Sync + 'static,
    {
        let (tx, rx) = mpsc::channel(1);
        let mut actor = PeerActor {
            rx,
            broker,
            sink_builder,
            id,
            params,
            sink: PhantomData,
        };
        tokio::spawn(async move {
            if let Err(err) = actor.run().await {
                tracing::error!("Peer failed: {:?}", err);
            }
        });

        PeerImpl { tx }
    }
}

enum PeerOp {
    Send(Bytes, oneshot::Sender<Result<()>>),
}

struct PeerActor<B, S, K>
where
    B: Broker + std::marker::Send + std::marker::Sync,
    S: SinkBuilder<K> + std::marker::Send + std::marker::Sync,
    K: Sink + Sized + std::marker::Send + std::marker::Sync,
{
    rx: Receiver<PeerOp>,
    broker: B,
    sink_builder: S,
    params: Params,
    id: connection::Info,
    sink: PhantomData<K>,
}

/// Internal states of the PeerActor.
enum ActorState<K>
where
    K: Sink + Sized + std::marker::Send + std::marker::Sync,
{
    /// Pending an initial sink connection.
    Connecting,
    /// Connected to the sink.
    Connected(Arc<K>),
    /// The Peer handle closed the connection.
    Done,
}

impl<B, S, K> PeerActor<B, S, K>
where
    B: Broker + std::marker::Send + std::marker::Sync,
    S: SinkBuilder<K> + std::marker::Send + std::marker::Sync,
    K: Sink + Sized + std::marker::Send + std::marker::Sync,
{
    async fn run(&mut self) -> anyhow::Result<()> {
        let mut state = ActorState::<K>::Connecting;
        loop {
            match state {
                ActorState::Connecting => state = ActorState::Connected(self.get_sink().await),
                ActorState::Connected(sink) => state = self.serve_with_sink(sink).await,
                ActorState::Done => break,
            }
        }
        tracing::info!("Peer shutting done.");
        Ok(())
    }

    /// Serves requests on the given Sink. Returns true if there is no more work to do.
    async fn serve_with_sink(&mut self, sink: Arc<K>) -> ActorState<K> {
        loop {
            match self.rx.recv().await {
                Some(PeerOp::Send(data, tx)) => {
                    let result = sink.store(&data).await.context("Sink error");

                    // We might hide connection errors here to let the source find the new location of the sink.
                    // TODO: only return on connection errors, surface other errors to the caller.
                    if result.is_err() {
                        tracing::info!("Failed to send chunk: {:?}", result);
                        return ActorState::Connecting;
                    }
                    if let Err(err) = tx.send(result) {
                        tracing::error!("Failed to send result to caller: {:?}", err);
                    }
                }
                None => {
                    tracing::info!("Sink closed.");
                    return ActorState::Done;
                }
            }
        }
    }

    /// Returns a Sink that can be used to send chunks to.
    async fn get_sink(&self) -> Arc<K> {
        let mut backoff: ExpBackoff = ExpBackoff::new(&self.params.backoff);
        loop {
            match self.broker.get_peers().await {
                Ok(peers) => match self.find_sink(&peers, &self.id).await {
                    Some(sink) => return sink,
                    None => tracing::debug!("No sink found, retrying."),
                },
                Err(err) => tracing::error!("Failed to get peers: {:?}", err),
            };
            rpcutil::jittery_sleep(backoff.again()).await;
        }
    }

    /// Try to connect to a Sink with our current connection identity, using candidate addresses in
    /// turn. Returns the first Sink for which the connection is established, None if none can be
    /// found.
    async fn find_sink(
        &self,
        candidates: &Vec<SinkLocation>,
        connection: &connection::Info,
    ) -> Option<Arc<K>> {
        for candidate in candidates {
            for address in candidate.addresses() {
                match self
                    .sink_builder
                    .connect(connection, candidate.id(), address.clone())
                    .await
                {
                    Ok(client) => {
                        tracing::info!("Connected to {} at {}", candidate.id(), address);
                        return Some(Arc::new(client));
                    }
                    Err(err) => {
                        tracing::debug!(
                            "Failed to connect to {} at {}: {:?}",
                            candidate.id(),
                            address,
                            err
                        );
                    }
                }
            }
        }
        tracing::info!("No valid sink found.");
        None
    }
}

/// Params are used to inject dependencies for testing.
struct Params {
    backoff: Backoff,
}

impl Params {
    pub fn default() -> Params {
        Params {
            backoff: Backoff {
                min_delay: Duration::from_secs(10),
                max_delay: Duration::from_secs(60),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker_client::MockBroker;
    use sink_client::{MockSink, MockSinkBuilder};
    use tonic::Status;

    #[tokio::test]
    async fn smoke_test() -> anyhow::Result<()> {
        let mut mock_broker = MockBroker::new();
        let mut mock_sink_builder = MockSinkBuilder::<MockSink>::new();

        // Setup: we immediately get a sink address which we can connect to.
        mock_broker
            .expect_get_peers()
            .returning(|| locations(&["1.2.3.4"]))
            .times(1);
        mock_sink_builder
            .expect_connect()
            .withf(move |_, id, addr| id == "sink" && addr == "1.2.3.4")
            .returning(good_sink);

        send_chunk(mock_broker, mock_sink_builder).await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn propagate_failure() -> anyhow::Result<()> {
        let mut mock_broker = MockBroker::new();
        let mut mock_sink_builder = MockSinkBuilder::<MockSink>::new();

        // Setup: we immediately get a sink address which we can connect to, but the sink fails.
        // We confirm that a subsequent call will use a different peer again.
        mock_broker
            .expect_get_peers()
            .returning(|| locations(&["1.2.3.4"]))
            .times(1);
        mock_broker
            .expect_get_peers()
            .returning(|| locations(&["5.6.7.8"]))
            .times(1);
        mock_sink_builder
            .expect_connect()
            .withf(move |_, id, addr| id == "sink" && addr == "1.2.3.4")
            .returning(bad_sink);
        mock_sink_builder
            .expect_connect()
            .withf(move |_, id, addr| id == "sink" && addr == "5.6.7.8")
            .returning(good_sink);

        let peer = PeerImpl::new(
            mock_broker,
            mock_sink_builder,
            testcerts::broker_info(),
            Params::default(),
        );
        let chunk = model::Chunk::new(Bytes::copy_from_slice([1, 2, 3].as_ref()), &[]);

        let start = tokio::time::Instant::now();
        peer.send(&chunk).await.unwrap_err();
        peer.send(&chunk).await?;
        let duration = tokio::time::Instant::now().duration_since(start);

        // The second call is not subject to waiting as we directly connect to the second peer.
        assert_eq!(duration, Duration::from_secs(0));

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn attempt_all_ips() -> anyhow::Result<()> {
        let mut mock_broker = MockBroker::new();
        let mut mock_sink_builder = MockSinkBuilder::<MockSink>::new();

        // Setup: the first sink address fails, but the second works.
        mock_broker
            .expect_get_peers()
            .returning(|| locations(&["1.2.3.4", "5.6.7.8"]))
            .times(1);
        mock_sink_builder
            .expect_connect()
            .withf(move |_, id, addr| id == "sink" && addr == "1.2.3.4")
            .returning(move |_, _, _| Err(anyhow::anyhow!("Failed to connect")))
            .times(1);
        mock_sink_builder
            .expect_connect()
            .withf(move |_, id, addr| id == "sink" && addr == "5.6.7.8")
            .returning(good_sink)
            .times(1);

        let start = tokio::time::Instant::now();
        send_chunk(mock_broker, mock_sink_builder).await?;
        let duration = tokio::time::Instant::now().duration_since(start);

        // The call doesn't need to wait as we test all IPs in sequence, and only pause across attempts.
        assert_eq!(duration, Duration::from_secs(0));

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn retry_connection() -> anyhow::Result<()> {
        let mut mock_broker = MockBroker::new();
        let mut mock_sink_builder = MockSinkBuilder::<MockSink>::new();

        // Setup: the first sink info does not work, but is updated later by the broker.
        mock_broker
            .expect_get_peers()
            .returning(|| locations(&["1.2.3.4"]))
            .times(1);
        mock_broker
            .expect_get_peers()
            .returning(|| locations(&["5.6.7.8"]))
            .times(1);
        mock_sink_builder
            .expect_connect()
            .withf(move |_, id, addr| id == "sink" && addr == "1.2.3.4")
            .returning(move |_, _, _| Err(anyhow::anyhow!("Failed to connect")))
            .times(1);
        mock_sink_builder
            .expect_connect()
            .withf(move |_, id, addr| id == "sink" && addr == "5.6.7.8")
            .returning(good_sink)
            .times(1);

        let start = tokio::time::Instant::now();
        send_chunk(mock_broker, mock_sink_builder).await?;
        let duration = tokio::time::Instant::now().duration_since(start);

        // The call needs to wait for one exponential backoff before returning.
        assert!(duration >= Duration::from_secs(10));

        Ok(())
    }

    /// Helper that generates a SinkLocation vector with the given addresses.
    fn locations(ips: &[&str]) -> Result<Arc<Vec<SinkLocation>>> {
        Ok(Arc::new(vec![SinkLocation::new(
            "sink".to_string(),
            ips.iter().map(|ip| ip.to_string()).collect(),
        )]))
    }

    /// Helper that generates a Sink that will accept a single chunk.
    fn good_sink(_: &settings::connection::Info, _: &str, _: String) -> Result<MockSink> {
        let mut mock_sink = MockSink::new();
        mock_sink.expect_store().returning(|_| Ok(())).times(1);
        Ok(mock_sink)
    }

    /// Helper that generates a Sink that will connect but fail accepting a chunk.
    fn bad_sink(_: &settings::connection::Info, _: &str, _: String) -> Result<MockSink> {
        let mut mock_sink = MockSink::new();
        mock_sink
            .expect_store()
            .returning(|_| Err(Status::internal("boom")))
            .times(1);
        Ok(mock_sink)
    }

    /// Helper that attempts to send a chunk to a Sink using the given mocks.
    async fn send_chunk(
        mock_broker: MockBroker,
        mock_sink_builder: MockSinkBuilder<MockSink>,
    ) -> anyhow::Result<()> {
        let peer = PeerImpl::new(
            mock_broker,
            mock_sink_builder,
            testcerts::broker_info(),
            Params::default(),
        );

        let chunk = model::Chunk::new(Bytes::copy_from_slice([1, 2, 3].as_ref()), &[]);
        peer.send(&chunk).await
    }
}
