//! Helper to register a tonic server's router in an
//! Actors instance.

use anyhow::Context;
use std::net::SocketAddr;
use tonic::transport::server::Router;

pub struct Actor {
    addr: SocketAddr,
    router: Router,
}

impl Actor {
    pub fn new(addr: SocketAddr, router: Router) -> Self {
        Actor { addr, router }
    }
}
impl crate::Async for Actor {
    type Operation = ();

    async fn run(self, _rx: tokio::sync::mpsc::Receiver<Self::Operation>) -> crate::Result<()> {
        self.router.serve(self.addr).await.context("router")?;
        Ok(())
    }
}
