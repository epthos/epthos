use bytes::Bytes;
use crypto::model;
use futures::channel::oneshot;
use rayon::{ThreadPool, ThreadPoolBuildError};
use std::time::Instant;
use tracing::Level;

#[derive(thiserror::Error, Debug)]
pub enum FingerprinterError {
    #[error("Threadpool could not be created")]
    ThreadpoolError(#[from] ThreadPoolBuildError),
}

/// CPU-heavy hashing and digest computation.
#[derive(Debug)]
pub struct Fingerprinter {
    pool: ThreadPool,
}

impl Fingerprinter {
    /// Creates a fingerprinter which runs on the specified number of threads.
    pub fn new(threads: usize) -> Result<Fingerprinter, FingerprinterError> {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()?;

        Ok(Fingerprinter { pool })
    }

    /// Hash a block of data.
    #[tracing::instrument(skip(self, data))]
    pub async fn hash(&self, data: Bytes) -> model::Chunk {
        let span = tracing::span!(Level::INFO, "compute");
        let (tx, rx) = oneshot::channel();

        self.pool.install(|| {
            let _e = span.enter();
            let start = Instant::now();
            let digest = ring::digest::digest(&ring::digest::SHA256, data.as_ref());
            tracing::debug!("hashed {} bytes in {:?}", data.len(), start.elapsed());
            tx.send(model::Chunk::new(data, digest.as_ref())).unwrap();
        });

        rx.await.unwrap()
    }
}
