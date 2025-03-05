//! Collection of helpers for writing client/server tests.

use std::{collections::VecDeque, convert::Infallible, sync::Arc, time::Duration};

use http::{Request, Response};
use hyper_util::rt::tokio::TokioIo;
use tokio::sync::{mpsc::Sender, Mutex};
use tonic::{
    body::BoxBody,
    codegen::Service,
    server::NamedService,
    transport::{Channel, Endpoint, Server, Uri},
    Result, Status,
};

use crate::Sleeper;

/// Holds canned responses to implement mock servers.
pub struct CannedResponses<T> {
    responses: Arc<Mutex<VecDeque<Result<tonic::Response<T>>>>>,
}

impl<T> CannedResponses<T> {
    pub async fn next(&self) -> Result<tonic::Response<T>, Status> {
        match self.responses.lock().await.pop_front() {
            Some(value) => value,
            None => Err(Status::aborted("no more values")),
        }
    }
}

impl<T, const N: usize> From<[Result<tonic::Response<T>, Status>; N]> for CannedResponses<T> {
    fn from(values: [Result<tonic::Response<T>, Status>; N]) -> CannedResponses<T> {
        CannedResponses {
            responses: Arc::new(Mutex::new(VecDeque::from(values))),
        }
    }
}

/// Builds a fake gRPC client/server connection. The provided server will respond
/// when a client uses the returned channel.
///
/// A simple server can be created using the CannedResponses type above.
pub async fn fake_server<S>(service: S) -> anyhow::Result<Channel>
where
    S: Service<Request<BoxBody>, Response = Response<BoxBody>, Error = Infallible>
        + NamedService
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    let (client, server) = tokio::io::duplex(1024);

    tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming(futures::stream::iter(vec![Ok::<_, std::io::Error>(server)]))
            .await
    });

    let mut client = Some(client);

    // The address here needs to parse as a valid address, but is not actually being
    // used.
    let channel = Endpoint::try_from("http://[::1]:50001")?
        .connect_with_connector(tower::service_fn(move |_: Uri| {
            let client = client.take();
            async move {
                if let Some(client) = client {
                    // Expose the required Hyper Read/Write traits thanks to TokioIo.
                    Ok(TokioIo::new(client))
                } else {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Client already taken",
                    ))
                }
            }
        }))
        .await?;

    Ok(channel)
}

/// Build a request bound to a valid peer, for server testing.
pub fn request<T>(request: T, peer: crate::auth::Peer) -> tonic::Request<T> {
    let mut request = tonic::Request::new(request);
    request.extensions_mut().insert(peer);
    request
}

/// Returns a Sleeper implenentation which will send the duration
/// of the sleep on `tx`, without any delay (except if sending is
/// blocked).
pub fn tracking_sleeper(tx: Sender<Duration>) -> Box<dyn Sleeper> {
    Box::new(TrackingSleeper { delays: tx })
}

struct TrackingSleeper {
    delays: Sender<Duration>,
}

#[tonic::async_trait]
impl Sleeper for TrackingSleeper {
    async fn sleep(&self, duration: Duration) {
        self.delays.send(duration).await.expect("can't send");
    }
}
