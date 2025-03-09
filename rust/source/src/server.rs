use self::{builder::Builder, peer::Peer};
use crate::filepicker;
use anyhow::Result;
use std::{
    io::{ErrorKind, Read},
    path::{Path, PathBuf},
};
use tracing::instrument;

mod builder;
mod peer;

/// A Source server, which watches the filesystem and backs data up to a Sink.
pub struct Server<P: Peer> {
    roots: Vec<PathBuf>,
    _peer: P,
    picker: filepicker::Picker,
}

const CHUNK_SIZE: usize = 2usize.pow(22); // 23 breaks the current gRPC limit.

pub fn builder() -> Builder {
    Builder::default()
}

impl<P: Peer> Server<P> {
    /// Run the server.
    pub async fn serve(self) -> Result<()> {
        // Set up the file store.
        let roots: Vec<&Path> = self.roots.iter().map(|p| p.as_ref()).collect();
        self.picker.set_roots(&roots).await?;

        let mut batch = 0;
        loop {
            match self.picker.next().await? {
                filepicker::Next::ScanDir(directory, updates) => {
                    let _span_ = tracing::trace_span!("fs_scanning").entered();
                    match std::fs::read_dir(&directory) {
                        Ok(dir_list) => {
                            let mut complete = true;
                            for dir in dir_list {
                                let Ok(dir) = dir else {
                                    tracing::info!("failed to stat file: {:?}", &dir);
                                    complete = false;
                                    continue;
                                };
                                let Ok(md) = dir.metadata() else {
                                    tracing::info!("failed to get metadata for {:?}", &dir);
                                    complete = false;
                                    continue;
                                };
                                let tp = md.file_type();
                                if tp.is_dir() {
                                    updates
                                        .send(filepicker::ScanUpdate::Directory(dir.file_name()))
                                        .await?;
                                } else if tp.is_file() {
                                    let Ok(modified) = md.modified() else {
                                        tracing::info!("failed to get mtime for {:?}", &dir);
                                        complete = false;
                                        continue;
                                    };
                                    updates
                                        .send(filepicker::ScanUpdate::File(
                                            dir.file_name(),
                                            md.len(),
                                            modified,
                                        ))
                                        .await?;
                                }
                            }
                            updates.send(filepicker::ScanUpdate::Done(complete)).await?;
                        }
                        Err(err) => {
                            updates.send(filepicker::ScanUpdate::Error(err)).await?;
                        }
                    };
                    batch += 1;
                    if batch % 100 == 0 {
                        tracing::info!("scanned {:?} ({})", directory, batch);
                    }
                }
                filepicker::Next::CheckFile(file) => match hash_file(&file) {
                    Ok(digest) => {
                        self.picker
                            .update(file, filepicker::Update::Hash(digest))
                            .await?;
                    }
                    Err(err) => {
                        self.picker
                            .update(file, filepicker::Update::Unreadable(err))
                            .await?;
                    }
                },
            }
        }
    }
}

#[instrument]
fn hash_file(path: &Path) -> Result<ring::digest::Digest, std::io::Error> {
    let mut file = std::fs::File::open(path)?;
    let mut hasher = ring::digest::Context::new(&ring::digest::SHA256);
    let mut buffer = vec![0; CHUNK_SIZE];
    loop {
        match file.read(buffer.as_mut_slice()) {
            Ok(0) => break,
            Ok(bytes) => {
                hasher.update(&buffer[..bytes]);
            }
            Err(err) => {
                if err.kind() == ErrorKind::Interrupted {
                    continue;
                }
                return Err(err);
            }
        }
    }
    Ok(hasher.finish())
}
